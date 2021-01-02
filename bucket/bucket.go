package bucket

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/ljmsc/wal/segment"
)

const (
	DefaultMaxSegmentSize = 1e9 // 1GB
)

// Bucket .
type Bucket struct {
	// name is the name if the segment bucket. This is a filepath
	name string
	// store is a list of all segments in the segment bucket
	store *store
	// segmentList is the list of segments in the bucket
	segmentList []segment.Segment
	// dataMutex is a mutex to protect the data in the bucket
	dataMutex sync.RWMutex
	// latestSequenceNumber is the last sequence number written to a segment file in the bucket
	latestSequenceNumber uint64
	// firstSequenceNumber is the first sequence number in the bucket. this changes during compression
	firstSequenceNumber uint64
	// recordSequenceNumbers stores the segment and offset for a sequence number
	recordSequenceNumbers map[uint64]RecordPosition
	// recordKeys - the map key is the hash value of the record Key and the value is the bucket sequence number
	recordKeys map[uint64][]uint64
	// writeMutex is a mutex to protect the write operations to the segments
	writeMutex sync.Mutex
	// closed
	closed bool
	// maxSegmentSize defines the max size of the segment files in the bucket in bytes
	maxSegmentSize uint64
}

// OpenWithHandler .
func OpenWithHandler(name string, headOnly bool, handler func(r Record) error) (*Bucket, error) {
	return Open(name, DefaultMaxSegmentSize, headOnly, handler)
}

// OpenWithSize .
func OpenWithSize(name string, maxSegmentSize uint64) (*Bucket, error) {
	return Open(name, maxSegmentSize, true, nil)
}

// OpenWithSize .
func Open(name string, maxSegmentSize uint64, headOnly bool, handler func(r Record) error) (*Bucket, error) {
	if maxSegmentSize == 0 {
		maxSegmentSize = DefaultMaxSegmentSize
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
		maxSegmentSize:        maxSegmentSize,
	}

	var err error
	c.store, err = openStore(name)
	if err != nil {
		return nil, fmt.Errorf("can't open bucket store: %w", err)
	}

	segmentNames := c.store.get()
	c.segmentList = make([]segment.Segment, 0, len(segmentNames)+5)

	for _, segName := range segmentNames {
		sequenceNumbers := make([]uint64, 0, len(segmentNames)*10)
		seg, err := segment.OpenWithHandler(segName, headOnly, func(envelope segment.Envelope) error {
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
			return nil, fmt.Errorf("can't open segment file %s : %w", segName, err)
		}
		for _, sequenceNumber := range sequenceNumbers {
			if recordPosition, ok := c.recordSequenceNumbers[sequenceNumber]; ok {
				c.recordSequenceNumbers[sequenceNumber] = RecordPosition{
					KeyHash: recordPosition.KeyHash,
					Offset:  recordPosition.Offset,
					Segment: seg,
				}
			}
		}

		c.segmentList = append(c.segmentList, seg)
	}
	return &c, nil
}

// segmentName returns a segment name for the bucket
func (b *Bucket) segmentName(startSeqNum uint64) string {
	return b.name + "_" + strconv.FormatUint(startSeqNum, 10)
}

// segment returns a segment file. the offset determines which segment file to return
// 0 = latest
// 1 = second latest
// and so on
func (b *Bucket) segment(listPosition uint64) (segment.Segment, error) {
	listSize := uint64(len(b.segmentList))
	if listSize == 0 {
		if err := b.addSegment(); err != nil {
			return nil, fmt.Errorf("can't add new segment to bucket: %w", err)
		}
		listSize = uint64(len(b.segmentList))
	}
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()

	pos := int64(listSize - (1 + listPosition))
	if pos < 0 {
		return nil, SegmentNotFoundErr
	}

	return b.segmentList[pos], nil
}

func (b *Bucket) addSegment() error {
	b.dataMutex.Lock()
	defer b.dataMutex.Unlock()

	segName := b.segmentName(b.latestSequenceNumber + 1)
	seg, err := segment.Open(segName)
	if err != nil {
		return fmt.Errorf("can't open new segment file: %w", err)
	}
	b.segmentList = append(b.segmentList, seg)
	segmentNames := b.store.get()
	segmentNames = append(segmentNames, segName)
	if err := b.store.update(segmentNames); err != nil {
		_ = seg.Close()
		_ = os.Remove(segName)
		return fmt.Errorf("can't update bucket store: %w", err)
	}
	return nil
}

func (b *Bucket) readFromSegment(p segment.Segment, offset int64, headOnly bool, r *Record) error {
	b.dataMutex.RLock()
	defer b.dataMutex.RUnlock()
	if b.closed {
		return ClosedErr
	}

	sr := segment.Record{}
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

	for i := 0; i < len(b.segmentList); i++ {
		seg, err := b.segment(uint64(i))
		if err != nil {
			if errors.Is(err, SegmentNotFoundErr) {
				break
			}
			return err
		}
		sr := segment.Record{}
		if err := seg.ReadByHash(hash, headOnly, &sr); err != nil {
			if errors.Is(err, segment.RecordNotFoundErr) {
				continue
			}

			return ReadErr{
				SegmentName: seg.Name(),
				Err:         err,
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

	var latestSegment segment.Segment
	for {
		var err error
		latestSegment, err = b.segment(0)
		if err != nil {
			//
			return err
		}
		segSize, err := latestSegment.Size()
		if err != nil {
			return err
		}
		if uint64(segSize) > b.maxSegmentSize {
			if err := b.addSegment(); err != nil {
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

	pr := segment.Record{}
	r.toSegmentRecord(&pr)

	recordOffset, err := latestSegment.WriteRecord(&pr)
	if err != nil {
		return WriteErr{
			SegmentName: latestSegment.Name(),
			Err:         err,
		}
	}

	if _, ok := b.recordKeys[r.Key.Hash()]; !ok {
		b.recordKeys[r.Key.Hash()] = make([]uint64, 0, 10)
	}
	b.recordKeys[r.Key.Hash()] = append(b.recordKeys[r.Key.Hash()], r.SequenceNumber())
	b.recordSequenceNumbers[r.SequenceNumber()] = RecordPosition{
		KeyHash: r.Key.Hash(),
		Offset:  recordOffset,
		Segment: latestSegment,
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

	if len(b.segmentList) < 3 {
		return NotEnoughSegmentsForCompressionErr
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

// Name returns the name of the segment bucket
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
	closeErrors := make([]error, 0, len(b.segmentList))
	for _, seg := range b.segmentList {
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
func (b *Bucket) ReadByKey(key segment.Key, headOnly bool, r *Record) error {
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
	if err := b.readFromSegment(pos.Segment, pos.Offset, headOnly, r); err != nil {
		if errors.Is(err, segment.ReadErr{}) {
			return ReadErr{
				SequenceNumber: seqNum,
				SegmentName:    pos.Segment.Name(),
				Err:            err,
			}
		}
		return err
	}
	return nil

}

// Write writes key and data as a record to the segment bucket
func (b *Bucket) Write(key segment.Key, data segment.Data) error {
	r := CreateRecord(key, data)
	return b.WriteRecord(r)
}

// WriteRecord writes the given record to the segment bucket
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
	truncSegmentOffsets := make(map[segment.Segment]int64)
	stripKeys := make([]uint64, 0, 10)
	for i := _seqNum; i <= b.latestSequenceNumber; i++ {
		if _, ok := b.recordSequenceNumbers[i]; !ok {
			continue
		}
		recordPos := b.recordSequenceNumbers[i]
		stripKeys = append(stripKeys, recordPos.KeyHash)
		delete(b.recordSequenceNumbers, i)
		if offset, ok := truncSegmentOffsets[recordPos.Segment]; ok {
			if offset < recordPos.Offset {
				continue
			}
		}
		truncSegmentOffsets[recordPos.Segment] = recordPos.Offset
	}

	for seg, offset := range truncSegmentOffsets {
		if err := seg.Truncate(offset); err != nil {
			return fmt.Errorf("can't truncate segment: %w", err)
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
			sr := segment.Record{}

			if err := pos.Segment.ReadByOffset(pos.Offset, headOnly, &sr); err != nil {
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
	for _, p := range b.segmentList {
		size, err := p.Size()
		if err != nil {
			return 0, fmt.Errorf("can't get size from segment in bucket segmentList: %w", err)
		}
		bSize += size
	}
	size, err := b.store.segment.Size()
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

	for _, p := range b.segmentList {
		if err := p.Remove(); err != nil {
			return fmt.Errorf("can't remove segment from bucket: %w", err)
		}
	}
	if err := b.store.segment.Remove(); err != nil {
		return fmt.Errorf("can't remove bucket store: %w", err)
	}
	return nil
}
