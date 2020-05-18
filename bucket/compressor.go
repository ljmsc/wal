package bucket

import (
	"fmt"
	"os"

	"github.com/ljmsc/wal/pouch"
)

type compressor struct {
	// bucket is the bucket that is being compressed
	bucket *Bucket
	// pouchList stores the new created pouches during the compression
	pouchList []*pouch.Pouch
	// recordSequenceNumbers stores the pouch and offset for a sequence number
	recordSequenceNumbers map[uint64]RecordPosition
	// endSeqNum is the last sequence number for the compression
	endSeqNum uint64
	// bucketStartSeqNum is the smallest sequence number in the bucket after the compression
	bucketStartSeqNum uint64
	// pouchNamesSnapshot is a snapshot of all pouches which are included in the compression
	pouchNamesSnapshot map[string]struct{}
}

// createCompressor creates a new compressor to compress the bucket
func createCompressor(b *Bucket) (*compressor, error) {
	c := compressor{
		bucket:                b,
		pouchList:             make([]*pouch.Pouch, 0, len(b.pouchList)),
		recordSequenceNumbers: make(map[uint64]RecordPosition),
		endSeqNum:             0,
		bucketStartSeqNum:     0,
		pouchNamesSnapshot:    make(map[string]struct{}),
	}
	if err := c.addPouch(1); err != nil {
		return nil, fmt.Errorf("can't add new pouch to compressor: %w", err)
	}

	b.dataMutex.RLock()
	var latestPouch *pouch.Pouch
	for i := 0; i < len(b.pouchList)-1; i++ {
		latestPouch = b.pouchList[i]
		c.pouchNamesSnapshot[latestPouch.Name()] = struct{}{}
	}
	b.dataMutex.RUnlock()

	if latestPouch == nil {
		return nil, fmt.Errorf("can't read second latest pouch from bucket")
	}

	lastRecordOffset := latestPouch.LastOffset()
	pr := pouch.Record{}
	if err := latestPouch.ReadByOffset(lastRecordOffset, true, &pr); err != nil {
		return nil, fmt.Errorf("can't read last record from second latest pouch in bucket: %w", err)
	}
	r := Record{}
	if err := toRecord(pr, &r); err != nil {
		return nil, fmt.Errorf("can't convert latest record: %w", err)
	}
	c.endSeqNum = r.SequenceNumber()
	c.bucketStartSeqNum = r.SequenceNumber()
	return &c, nil
}

// endSequenceNumber .
func (c *compressor) endSequenceNumber() uint64 {
	return c.endSeqNum
}

// addPouch adds a new pouch to the compressor
func (c *compressor) addPouch(seqNum uint64) error {

	pouchName := c.bucket.pouchName(seqNum)
	for {
		if _, err := os.Stat(pouchName); !os.IsNotExist(err) {
			pouchName = pouchName + "c"
			continue
		}
		break
	}

	pouchItem, err := pouch.Open(pouchName)
	if err != nil {
		return fmt.Errorf("can't create new pouch: %w", err)
	}

	c.pouchList = append(c.pouchList, pouchItem)
	return nil
}

// currentWritePouch returns the current pouch to write to
func (c *compressor) currentWritePouch(currentSeqNum uint64) (*pouch.Pouch, error) {
	if len(c.pouchList) == 0 {
		return nil, fmt.Errorf("empty pouch list")
	}

	for {
		writePouch := c.pouchList[len(c.pouchList)-1]
		size, err := writePouch.Size()
		if err != nil {
			return nil, fmt.Errorf("can't read pouch size: %w", err)
		}
		if uint64(size) >= c.bucket.maxPouchSize {
			if err := c.addPouch(currentSeqNum); err != nil {
				return nil, fmt.Errorf("can't add new pouch file to compressor: %w", err)
			}
			continue
		}
		return writePouch, nil
	}
}

// write writes the record to the compressor pouch files
func (c *compressor) write(r *Record) error {
	writePouch, err := c.currentWritePouch(r.SequenceNumber())
	if err != nil {
		return fmt.Errorf("can't request write pouch: %w", err)
	}

	pr := pouch.Record{}
	r.toPouchRecord(&pr)

	offset, err := writePouch.WriteRecord(&pr)
	if err != nil {
		return fmt.Errorf("can't wirte to compressor: %w", err)
	}

	c.recordSequenceNumbers[r.SequenceNumber()] = RecordPosition{
		Offset: offset,
		Pouch:  writePouch,
	}

	if r.SequenceNumber() < c.bucketStartSeqNum {
		c.bucketStartSeqNum = r.SequenceNumber()
	}
	return nil
}

// apply changes the pouchList of the actual bucket. writing to the bucket is blocked during this step
func (c *compressor) apply() error {
	c.bucket.writeMutex.Lock()
	defer c.bucket.writeMutex.Unlock()
	c.bucket.dataMutex.Lock()
	defer c.bucket.dataMutex.Unlock()

	newPouchNameList := make([]string, len(c.bucket.pouchList))
	for _, p := range c.pouchList {
		newPouchNameList = append(newPouchNameList, p.Name())
	}

	for _, p := range c.bucket.pouchList {
		if _, ok := c.pouchNamesSnapshot[p.Name()]; !ok {
			c.pouchList = append(c.pouchList, p)
			newPouchNameList = append(newPouchNameList, p.Name())
		} else {
			// ignore error. the unclosed pouch should not stop the compression
			_ = p.Close()
		}
	}
	c.bucket.pouchList = c.pouchList

	// delete old record positions from bucket
	for seqNum := range c.bucket.recordSequenceNumbers {
		if seqNum <= c.endSeqNum {
			delete(c.bucket.recordSequenceNumbers, seqNum)
		}
	}

	// add new record position to bucket
	for seqNum, position := range c.recordSequenceNumbers {
		c.bucket.recordSequenceNumbers[seqNum] = position
	}

	c.bucket.firstSequenceNumber = c.bucketStartSeqNum

	// backup bucket store file
	storeBackupName := c.bucket.store.name + "_snapshot"
	if err := c.bucket.store.pou.Snapshot(storeBackupName); err != nil {
		return fmt.Errorf("can't create backup / snapshot of bucket store file: %w", err)
	}

	// update bucket store with new pouch names
	if err := c.bucket.store.update(newPouchNameList); err != nil {
		// the bucket will be closed to avoid data loss. After opening the bucket again, the old state should be restored
		_ = c.bucket.Close()
		return fmt.Errorf("can't update pouch names in bucket store. bucket is closed: %w", err)
	}

	// remove bucket store file backup
	_ = os.Remove(storeBackupName)

	for name := range c.pouchNamesSnapshot {
		// ignore removal error. this shouldn't stop the compression status
		_ = os.Remove(name)
	}

	return nil
}
