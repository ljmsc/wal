package bucket

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/ljmsc/wal/pouch"
)

const (
	DefaultMaxPouchSize = 1e9 // 1GB
)

// Bucket .
type Bucket struct {
	// name is the name if the pouch bucket. This is a filepath
	name string
	// store is a list of all pouches in the pouch bucket
	store *store
	// pouchList is the list of pouches in the bucket
	pouchList []*pouch.Pouch
	// dataMutex is a mutex to protect the data in the bucket
	dataMutex sync.RWMutex
	// latestSequenceNumber is the last sequence number written to a pouch file in the bucket
	latestSequenceNumber uint64
	// firstSequenceNumber is the first sequence number in the bucket
	firstSequenceNumber uint64
	// recordSequenceNumbers stores the pouch and offset for a sequence number
	recordSequenceNumbers map[uint64]RecordPosition
	// recordKeys - the map key is the hash value of the record Key and the value is the bucket sequence number
	recordKeys map[uint64][]uint64
	// writeMutex is a mutex to protect the write operations to the pouches
	writeMutex sync.Mutex
	// closed
	closed bool
	// maxPouchSize defines the max size of the pouch files in the bucket in bytes
	maxPouchSize uint64
}

// OpenWithHandler .
func OpenWithHandler(name string, headOnly bool, handler func(r Record) error) (*Bucket, error) {
	return Open(name, DefaultMaxPouchSize, headOnly, handler)
}

// OpenWithSize .
func OpenWithSize(name string, maxPouchSize uint64) (*Bucket, error) {
	return Open(name, maxPouchSize, true, nil)
}

// OpenWithSize .
func Open(name string, maxPouchSize uint64, headOnly bool, handler func(r Record) error) (*Bucket, error) {
	if maxPouchSize == 0 {
		maxPouchSize = DefaultMaxPouchSize
	}
	c := Bucket{
		name:                  name,
		latestSequenceNumber:  0,
		firstSequenceNumber:   0,
		dataMutex:             sync.RWMutex{},
		recordSequenceNumbers: make(map[uint64]RecordPosition),
		recordKeys:            make(map[uint64][]uint64),
		writeMutex:            sync.Mutex{},
		closed:                false,
		maxPouchSize:          maxPouchSize,
	}

	var err error
	c.store, err = openStore(name)
	if err != nil {
		return nil, fmt.Errorf("can't open bucket store: %w", err)
	}

	pouchNames := c.store.get()
	c.pouchList = make([]*pouch.Pouch, 0, len(pouchNames)+5)

	for _, pouchName := range pouchNames {
		sequenceNumbers := make([]uint64, 0, len(pouchNames)*10)
		pou, err := pouch.OpenWithHandler(pouchName, headOnly, func(envelope pouch.Envelope) error {
			r := Record{}
			if err := toRecord(*envelope.Record, &r); err != nil {
				return ConvertErr{Err: err}
			}
			keyHash := r.Key.Hash()

			if err := r.Validate(); err != nil {
				return RecordNotValidErr{Err: err}
			}

			if handler != nil {
				if err := handler(r); err != nil {
					return fmt.Errorf("can't execute handler for record: %w", err)
				}
			}

			if _, ok := c.recordKeys[keyHash]; !ok {
				c.recordKeys[keyHash] = make([]uint64, 0, 10)
			}

			c.recordKeys[keyHash] = append(c.recordKeys[keyHash], r.SequenceNumber())
			c.recordSequenceNumbers[r.SequenceNumber()] = RecordPosition{
				KeyHash: envelope.Record.Key.Hash(),
				Offset:  envelope.Offset,
			}

			if r.SequenceNumber() > c.latestSequenceNumber {
				c.latestSequenceNumber = r.SequenceNumber()
			}

			if r.SequenceNumber() < c.firstSequenceNumber || c.firstSequenceNumber == 0 {
				c.firstSequenceNumber = r.SequenceNumber()
			}

			sequenceNumbers = append(sequenceNumbers, r.SequenceNumber())
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("can't open pouch file %s : %w", pouchName, err)
		}
		for _, sequenceNumber := range sequenceNumbers {
			if recordPosition, ok := c.recordSequenceNumbers[sequenceNumber]; ok {
				c.recordSequenceNumbers[sequenceNumber] = RecordPosition{
					KeyHash: recordPosition.KeyHash,
					Offset:  recordPosition.Offset,
					Pouch:   pou,
				}
			}
		}

		c.pouchList = append(c.pouchList, pou)
	}
	return &c, nil
}

// pouchName returns a pouch name for the bucket
func (b *Bucket) pouchName(startSeqNum uint64) string {
	return b.name + "_" + strconv.FormatUint(startSeqNum, 10)
}

// pouch returns a pouch file. the offset determines which pouch file to return
// 0 = latest
// 1 = second latest
// and so on
func (b *Bucket) pouch(listPosition uint64) (*pouch.Pouch, error) {
	listSize := uint64(len(b.pouchList))
	if listSize == 0 {
		if err := b.addPouch(); err != nil {
			return nil, fmt.Errorf("can't add new pouch to bucket: %w", err)
		}
		listSize = uint64(len(b.pouchList))
	}
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()

	pos := int64(listSize - (1 + listPosition))
	if pos < 0 {
		return nil, PouchNotFoundErr
	}

	return b.pouchList[pos], nil
}

func (b *Bucket) addPouch() error {
	b.dataMutex.Lock()
	defer b.dataMutex.Unlock()

	pouchName := b.pouchName(b.latestSequenceNumber + 1)
	pou, err := pouch.Open(pouchName)
	if err != nil {
		return fmt.Errorf("can't open new pouch file: %w", err)
	}
	b.pouchList = append(b.pouchList, pou)
	pouchNames := b.store.get()
	pouchNames = append(pouchNames, pouchName)
	if err := b.store.update(pouchNames); err != nil {
		_ = pou.Close()
		_ = os.Remove(pouchName)
		return fmt.Errorf("can't update bucket store: %w", err)
	}
	return nil
}

func (b *Bucket) readFromPouch(p *pouch.Pouch, offset int64, headOnly bool, r *Record) error {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	if b.closed {
		return ClosedErr
	}

	sr := pouch.Record{}
	if err := p.ReadByOffset(offset, headOnly, &sr); err != nil {
		return err
	}

	if err := toRecord(sr, r); err != nil {
		return ConvertErr{Err: err}
	}
	return nil
}

func (b *Bucket) read(hash uint64, headOnly bool, r *Record) error {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	if b.closed {
		return ClosedErr
	}

	for i := 0; i < len(b.pouchList); i++ {
		pou, err := b.pouch(uint64(i))
		if err != nil {
			if errors.Is(err, PouchNotFoundErr) {
				break
			}
			return err
		}
		sr := pouch.Record{}
		if err := pou.ReadByHash(hash, headOnly, &sr); err != nil {
			if errors.Is(err, pouch.RecordNotFoundErr) {
				continue
			}

			return ReadErr{
				PouchName: pou.Name(),
				Err:       err,
			}
		}

		if err = toRecord(sr, r); err != nil {
			return ConvertErr{Err: err}
		}

		return nil
	}
	return RecordNotFoundErr
}

func (b *Bucket) write(r *Record) error {
	b.writeMutex.Lock()
	defer b.writeMutex.Unlock()
	if b.closed {
		return ClosedErr
	}

	var latestPouch *pouch.Pouch
	for {
		var err error
		latestPouch, err = b.pouch(0)
		if err != nil {
			//
			return err
		}
		segSize, err := latestPouch.Size()
		if err != nil {
			return err
		}
		if uint64(segSize) > b.maxPouchSize {
			if err := b.addPouch(); err != nil {
				return err
			}
			continue
		}
		break
	}

	b.dataMutex.Lock()
	defer b.dataMutex.Unlock()
	setSequenceNumber(b.latestSequenceNumber+1, r)

	if err := r.Validate(); err != nil {
		return RecordNotValidErr{Err: err}
	}

	pr := pouch.Record{}
	r.toPouchRecord(&pr)

	recordOffset, err := latestPouch.WriteRecord(&pr)
	if err != nil {
		return WriteErr{
			PouchName: latestPouch.Name(),
			Err:       err,
		}
	}

	if _, ok := b.recordKeys[r.Key.Hash()]; !ok {
		b.recordKeys[r.Key.Hash()] = make([]uint64, 0, 10)
	}
	b.recordKeys[r.Key.Hash()] = append(b.recordKeys[r.Key.Hash()], r.SequenceNumber())
	b.recordSequenceNumbers[r.SequenceNumber()] = RecordPosition{
		KeyHash: r.Key.Hash(),
		Offset:  recordOffset,
		Pouch:   latestPouch,
	}
	b.latestSequenceNumber++

	return nil
}

// compress .
// for filter = true remove item
// for filter = false keep item
func (b *Bucket) compress(filter func(item *Record) bool) error {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	if b.IsClosed() {
		return ClosedErr
	}

	if len(b.pouchList) < 3 {
		return NotEnoughPouchesForCompressionErr
	}

	compressor, err := createCompressor(b)
	if err != nil {
		return fmt.Errorf("can't create compressor: %w", err)
	}
	stream := b.StreamRecords(1, compressor.endSequenceNumber(), false)
	for {
		item, ok := <-stream
		if !ok {
			break
		}
		if item.Err != nil {
			continue
		}
		if filter(item.Record) {
			continue
		}
		if err := compressor.write(item.Record); err != nil {
			return fmt.Errorf("can't write to compressor: %w", err)
		}
	}
	b.dataMutex.RUnlock()
	defer b.dataMutex.RLock()
	if err := compressor.apply(); err != nil {
		return fmt.Errorf("can't compress bucket: %w", err)
	}

	return nil
}

// Name returns the name of the pouch bucket
func (b *Bucket) Name() string {
	return b.name
}

// Close closes the bucket for read and writes
func (b *Bucket) Close() error {
	b.dataMutex.Lock()
	defer b.dataMutex.Unlock()
	b.writeMutex.Lock()
	defer b.writeMutex.Unlock()
	b.closed = true
	closeErrors := make([]error, 0, len(b.pouchList))
	for _, seg := range b.pouchList {
		if err := seg.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	err := b.store.close()
	if len(closeErrors) == 0 {
		return err
	}
	closeErrors = append(closeErrors, err)
	return CloseErr{Errs: closeErrors}
}

// IsClosed returns true if the bucket is already closed
func (b *Bucket) IsClosed() bool {
	return b.closed
}

// ReadByKey reads the latest record for a given key
func (b *Bucket) ReadByHash(keyHash uint64, headOnly bool, r *Record) error {
	return b.read(keyHash, headOnly, r)
}

// ReadByKey reads the latest record for a given key
func (b *Bucket) ReadByKey(key pouch.Key, headOnly bool, r *Record) error {
	return b.read(key.Hash(), headOnly, r)
}

// ReadBySequenceNumber reads a record by a given sequence number
func (b *Bucket) ReadBySequenceNumber(seqNum uint64, headOnly bool, r *Record) error {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	if _, ok := b.recordSequenceNumbers[seqNum]; !ok {
		return RecordNotFoundErr
	}
	pos := b.recordSequenceNumbers[seqNum]
	if err := b.readFromPouch(pos.Pouch, pos.Offset, headOnly, r); err != nil {
		if errors.Is(err, pouch.ReadErr{}) {
			return ReadErr{
				SequenceNumber: seqNum,
				PouchName:      pos.Pouch.Name(),
				Err:            err,
			}
		}
		return err
	}
	return nil

}

// Write writes key and data as a record to the pouch bucket
func (b *Bucket) Write(key pouch.Key, data pouch.Data) error {
	r := CreateRecord(key, data)
	return b.WriteRecord(r)
}

// WriteRecord writes the given record to the pouch bucket
func (b *Bucket) WriteRecord(r *Record) error {
	return b.write(r)
}

// Dump - removes all entries whose sequence number is greater or equal to seqNum. the removal is permanently.
// If want to delete an entry use Write() with empty data and compress the bucket.
func (b *Bucket) Dump(_seqNum uint64) error {
	b.writeMutex.Lock()
	defer b.writeMutex.Unlock()
	if b.closed {
		return ClosedErr
	}
	b.dataMutex.Lock()
	defer b.dataMutex.Unlock()
	truncPouchOffsets := make(map[*pouch.Pouch]int64)
	stripKeys := make([]uint64, 0, 10)
	for i := _seqNum; i <= b.latestSequenceNumber; i++ {
		if _, ok := b.recordSequenceNumbers[i]; !ok {
			continue
		}
		recordPos := b.recordSequenceNumbers[i]
		stripKeys = append(stripKeys, recordPos.KeyHash)
		delete(b.recordSequenceNumbers, i)
		if offset, ok := truncPouchOffsets[recordPos.Pouch]; ok {
			if offset < recordPos.Offset {
				continue
			}
		}
		truncPouchOffsets[recordPos.Pouch] = recordPos.Offset
	}

	for pou, offset := range truncPouchOffsets {
		if err := pou.Truncate(offset); err != nil {
			return fmt.Errorf("can't truncate pouch: %w", err)
		}
	}

	for _, keyHash := range stripKeys {
		if _, ok := b.recordKeys[keyHash]; !ok {
			continue
		}
		keySeqNumbers := b.recordKeys[keyHash]
		newKeySeqNumbers := make([]uint64, 0, len(keySeqNumbers))
		for _, keySeqNum := range keySeqNumbers {
			if keySeqNum >= _seqNum {
				break
			}
			newKeySeqNumbers = append(newKeySeqNumbers, keySeqNum)
		}

		b.recordKeys[keyHash] = newKeySeqNumbers
	}

	b.latestSequenceNumber = _seqNum

	return nil
}

// LatestSequenceNumbers returns the latest sequence numbers for all written keys
func (b *Bucket) LatestSequenceNumber() uint64 {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	return b.latestSequenceNumber
}

// LatestSequenceNumbers returns the latest sequence numbers for all written keys
func (b *Bucket) LatestSequenceNumbers() []uint64 {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	sequenceNumbers := make([]uint64, 0, len(b.recordKeys))
	for _, keySequenceNumbers := range b.recordKeys {
		if len(keySequenceNumbers) == 0 {
			continue
		}
		sequenceNumbers = append(sequenceNumbers, keySequenceNumbers[len(keySequenceNumbers)-1])
	}
	sort.Slice(sequenceNumbers, func(i, j int) bool {
		return sequenceNumbers[i] < sequenceNumbers[j]
	})
	return sequenceNumbers
}

// StreamRecords streams records from the bucket into the returning channel. startSeqNum defines the beginning.
// startSeqNum = 1 for all records
// endSeqNum = 0 for all records
func (b *Bucket) StreamRecords(startSeqNum uint64, endSeqNum uint64, headOnly bool) <-chan Envelope {
	stream := make(chan Envelope)
	go func() {
		b.dataMutex.RLock()
		defer b.dataMutex.RUnlock()
		if startSeqNum < b.firstSequenceNumber {
			startSeqNum = b.firstSequenceNumber
		}
		if endSeqNum == 0 {
			endSeqNum = b.latestSequenceNumber
		}

		for i := startSeqNum; i <= endSeqNum; i++ {
			if _, ok := b.recordSequenceNumbers[i]; !ok {
				continue
			}
			pos := b.recordSequenceNumbers[i]
			sr := pouch.Record{}

			if err := pos.Pouch.ReadByOffset(pos.Offset, headOnly, &sr); err != nil {
				stream <- Envelope{Err: err}
				continue
			}

			r := Record{}
			if err := toRecord(sr, &r); err != nil {
				stream <- Envelope{Err: ConvertErr{Err: err}}
				continue
			}
			stream <- Envelope{
				Record: &r,
			}
		}
		close(stream)
	}()
	return stream
}

// StreamLatestRecords returns a channel which will receive the latest records in the bucket. each key at least ones.
func (b *Bucket) StreamLatestRecords(headOnly bool) <-chan Envelope {
	stream := make(chan Envelope)
	go func() {
		sequenceNumbers := b.LatestSequenceNumbers()

		for _, sequenceNumber := range sequenceNumbers {
			r := Record{}
			if err := b.ReadBySequenceNumber(sequenceNumber, headOnly, &r); err != nil {
				stream <- Envelope{Err: err}
				continue
			}
			stream <- Envelope{Record: &r}
		}
		close(stream)
	}()
	return stream
}

// Compress compresses the bucket. at least one record for each key remains
func (b *Bucket) Compress() error {
	latestSeqNumbers := b.LatestSequenceNumbers()
	latestSeqNumbersMap := make(map[uint64]struct{})
	for _, number := range latestSeqNumbers {
		latestSeqNumbersMap[number] = struct{}{}
	}
	return b.compress(func(item *Record) bool {
		if _, ok := latestSeqNumbersMap[item.SequenceNumber()]; !ok {
			return true
		}
		return false
	})
}

// CompressWithFilter compresses the bucket based on a given filter
// for filter = true remove item
// for filter = false keep item
func (b *Bucket) CompressWithFilter(filter func(item *Record) bool) error {
	return b.compress(filter)
}

// Size returns the size of the bucket in bytes
func (b *Bucket) Size() (int64, error) {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	bSize := int64(0)
	for _, p := range b.pouchList {
		size, err := p.Size()
		if err != nil {
			return 0, fmt.Errorf("can't get size from pouch in bucket pouchList: %w", err)
		}
		bSize += size
	}
	size, err := b.store.pou.Size()
	if err != nil {
		return 0, fmt.Errorf("can't get size from bucket store file: %w", err)
	}
	bSize += size
	return bSize, nil
}

// Remove removes the bucket from disk
func (b *Bucket) Remove() error {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	if !b.closed {
		return NotClosedErr
	}

	for _, p := range b.pouchList {
		if err := p.Remove(); err != nil {
			return fmt.Errorf("can't remove pouch from bucket: %w", err)
		}
	}
	if err := b.store.pou.Remove(); err != nil {
		return fmt.Errorf("can't remove bucket store: %w", err)
	}
	return nil
}
