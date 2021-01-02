package bucket

import (
	"fmt"
	"os"

	"github.com/ljmsc/wal/segment"
)

type compressor struct {
	// bucket is the bucket that is being compressed
	bucket *Bucket
	// segmentList stores the new created segments during the compression
	segmentList []segment.Segment
	// recordSequenceNumbers stores the segment and offset for a sequence number
	recordSequenceNumbers map[uint64]RecordPosition
	// endSeqNum is the last sequence number for the compression
	endSeqNum uint64
	// bucketStartSeqNum is the smallest sequence number in the bucket after the compression
	bucketStartSeqNum uint64
	// segmentNamesSnapshot is a snapshot of all segments which are included in the compression
	segmentNamesSnapshot map[string]struct{}
}

// createCompressor creates a new compressor to compress the bucket
func createCompressor(b *Bucket) (*compressor, error) {
	c := compressor{
		bucket:                b,
		segmentList:           make([]segment.Segment, 0, len(b.segmentList)),
		recordSequenceNumbers: make(map[uint64]RecordPosition),
		endSeqNum:             0,
		bucketStartSeqNum:     0,
		segmentNamesSnapshot:  make(map[string]struct{}),
	}
	if err := c.addSegment(1); err != nil {
		return nil, fmt.Errorf("can't add new segment to compressor: %w", err)
	}

	b.dataMutex.RLock()
	var latestSegment segment.Segment
	for i := 0; i < len(b.segmentList)-1; i++ {
		latestSegment = b.segmentList[i]
		c.segmentNamesSnapshot[latestSegment.Name()] = struct{}{}
	}
	b.dataMutex.RUnlock()

	if latestSegment == nil {
		return nil, fmt.Errorf("can't read second latest segment from bucket")
	}

	lastRecordOffset := latestSegment.LastOffset()
	pr := segment.Record{}
	if err := latestSegment.ReadByOffset(lastRecordOffset, true, &pr); err != nil {
		return nil, fmt.Errorf("can't read last record from second latest segment in bucket: %w", err)
	}
	r := Record{}
	if err := toRecord(pr, &r); err != nil {
		return nil, ConvertErr{Err: err}
	}
	c.endSeqNum = r.SequenceNumber()
	c.bucketStartSeqNum = r.SequenceNumber()
	return &c, nil
}

// endSequenceNumber .
func (c *compressor) endSequenceNumber() uint64 {
	return c.endSeqNum
}

// addSegment adds a new segment to the compressor
func (c *compressor) addSegment(seqNum uint64) error {

	segmentName := c.bucket.segmentName(seqNum)
	for {
		if _, err := os.Stat(segmentName); !os.IsNotExist(err) {
			segmentName = segmentName + "c"
			continue
		}
		break
	}

	seg, err := segment.Open(segmentName)
	if err != nil {
		return fmt.Errorf("can't create new segment: %w", err)
	}

	c.segmentList = append(c.segmentList, seg)
	return nil
}

// currentWriteSegment returns the current segment to write to
func (c *compressor) currentWriteSegment(currentSeqNum uint64) (segment.Segment, error) {
	if len(c.segmentList) == 0 {
		return nil, fmt.Errorf("empty segment list")
	}

	for {
		writeSeg := c.segmentList[len(c.segmentList)-1]
		size, err := writeSeg.Size()
		if err != nil {
			return nil, fmt.Errorf("can't read segment size: %w", err)
		}
		if uint64(size) >= c.bucket.maxSegmentSize {
			if err := c.addSegment(currentSeqNum); err != nil {
				return nil, fmt.Errorf("can't add new segment file to compressor: %w", err)
			}
			continue
		}
		return writeSeg, nil
	}
}

// write writes the record to the compressor segment files
func (c *compressor) write(r *Record) error {
	writeSeg, err := c.currentWriteSegment(r.SequenceNumber())
	if err != nil {
		return fmt.Errorf("can't request write segment: %w", err)
	}

	pr := segment.Record{}
	r.toSegmentRecord(&pr)

	offset, err := writeSeg.WriteRecord(&pr)
	if err != nil {
		return fmt.Errorf("can't wirte to compressor: %w", err)
	}

	c.recordSequenceNumbers[r.SequenceNumber()] = RecordPosition{
		Offset:  offset,
		Segment: writeSeg,
	}

	if r.SequenceNumber() < c.bucketStartSeqNum {
		c.bucketStartSeqNum = r.SequenceNumber()
	}
	return nil
}

// apply changes the segmentList of the actual bucket. writing to the bucket is blocked during this step
func (c *compressor) apply() error {
	c.bucket.writeMutex.Lock()
	defer c.bucket.writeMutex.Unlock()
	c.bucket.dataMutex.Lock()
	defer c.bucket.dataMutex.Unlock()

	newSegmentNameList := make([]string, len(c.bucket.segmentList))
	for _, p := range c.segmentList {
		newSegmentNameList = append(newSegmentNameList, p.Name())
	}

	for _, p := range c.bucket.segmentList {
		if _, ok := c.segmentNamesSnapshot[p.Name()]; !ok {
			c.segmentList = append(c.segmentList, p)
			newSegmentNameList = append(newSegmentNameList, p.Name())
		} else {
			// ignore error. the unclosed segment should not stop the compression
			_ = p.Close()
		}
	}
	c.bucket.segmentList = c.segmentList

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
	if err := c.bucket.store.segment.Snapshot(storeBackupName); err != nil {
		return fmt.Errorf("can't create backup / snapshot of bucket store file: %w", err)
	}

	// update bucket store with new segment names
	if err := c.bucket.store.update(newSegmentNameList); err != nil {
		// the bucket will be closed to avoid data loss. After opening the bucket again, the old state should be restored
		_ = c.bucket.Close()
		return fmt.Errorf("can't update segment names in bucket store. bucket is closed now to avoid data loss: %w", err)
	}

	// remove bucket store file backup
	_ = os.Remove(storeBackupName)

	for name := range c.segmentNamesSnapshot {
		// ignore removal error. this shouldn't stop the compression status
		_ = os.Remove(name)
	}

	return nil
}
