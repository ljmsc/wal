package chain

import (
	"fmt"
	"strconv"

	"github.com/ljmsc/wal/segment"
)

const (
	defaultMaxSegmentSize = 1e9 // 1GB
)

type Chain interface {
	Name() string
	ReadAt(_record Record, _seqNum uint64) error
	ReadKey(_record Record, _key uint64) error
	Write(_record Record) (uint64, error)
	Truncate(_seqNum uint64) error
	Size() (int64, error)
	FirstSeqNum() uint64
	SeqNum() uint64
	KeySeqNums() map[uint64][]uint64
	Positions() map[uint64]Position
	IsClosed() bool
	Close() error
}

// Keys returns a slice of all key written to the chain
func Keys(_chain Chain) []uint64 {
	keySeqNums := _chain.KeySeqNums()
	keys := make([]uint64, 0, len(keySeqNums))
	for key := range keySeqNums {
		keys = append(keys, key)
	}
	return keys
}

// Stream returns a channel which receives all records for the given sequence number range.
// if the chain is closed the function returns nil
func Stream(_chain Chain, _startSeqNum uint64, _endSeqNum uint64) <-chan RecordEnvelope {
	//todo: impl
	return nil
}

// chain .
type chain struct {
	// name is the name if the segment chain. This is a filepath
	name string
	// maxSegmentSize defines the max size of the segment files in the chain in bytes
	maxSegmentSize int64
	// store is a list of all segments in the segment chain
	store *store
	// firstSeqNum is the first sequence number in the chain. this changes during compression
	firstSeqNum uint64
	// seqNum is the last sequence number written to a segment file in the chain
	seqNum uint64
	// segmentList is the list of segments in the chain
	segmentList []segment.Segment
	// keySeqNums - the key of the record and the value are the sequence numbers which are already written to segments
	keySeqNums map[uint64][]uint64
	// positions stores the segment and offset for a sequence number
	positions map[uint64]Position
	// closed
	closed bool
}

// Open .
func Open(name string, _padding uint64, maxSegmentSize int64, handler func(_chain Chain, _envelope HeaderEnvelope) error) (Chain, error) {
	if maxSegmentSize == 0 {
		maxSegmentSize = defaultMaxSegmentSize
	}

	// add sequence number field length to padding to ensure seqNum is always read from segments
	_padding += headerSequenceNumberFieldLength

	c := chain{
		name:           name,
		maxSegmentSize: maxSegmentSize,
		firstSeqNum:    0,
		seqNum:         0,
		keySeqNums:     make(map[uint64][]uint64),
		positions:      make(map[uint64]Position),
		closed:         false,
	}

	var err error
	c.store, err = openStore(name)
	if err != nil {
		return nil, fmt.Errorf("can't open chain store: %w", err)
	}

	segmentNames := c.store.get()
	c.segmentList = make([]segment.Segment, 0, len(segmentNames))

	for _, segmentName := range segmentNames {
		seg, err := segment.OpenWithPadding(segmentName, _padding, func(_segment segment.Segment, _envelope segment.HeaderEnvelope) error {
			seqNum, err := decodeSeqNum(_envelope.Header.PaddingBytes[:headerSequenceNumberFieldLength])
			if err != nil {
				return err
			}
			c.consider(_envelope.Header.Key, seqNum, _envelope.Offset, _segment)

			if handler != nil {
				if err := handler(&c, HeaderEnvelope{
					SeqNum: seqNum,
					Header: _envelope.Header,
				}); err != nil {
					return err
				}
			}

			return nil
		})
		c.segmentList = append(c.segmentList, seg)
		if err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// Name returns the name of the segment chain
func (b *chain) Name() string {
	return b.name
}

// ReadAt reads a record by a given sequence number
func (b *chain) ReadAt(_record Record, _seqNum uint64) error {
	if _, ok := b.positions[_seqNum]; !ok {
		return segment.RecordNotFoundErr
	}
	pos := b.positions[_seqNum]
	if pos.Segment == nil {
		return fmt.Errorf("segment not found")
	}

	if err := pos.Segment.ReadAt(_record.ForSegment(), pos.Offset); err != nil {
		return err
	}

	return nil
}

// ReadKey reads the latest record for a given key
func (b *chain) ReadKey(_record Record, _key uint64) error {
	if _, ok := b.keySeqNums[_key]; !ok {
		return segment.RecordNotFoundErr
	}

	seqNums := b.keySeqNums[_key]
	if len(seqNums) == 0 {
		return segment.RecordNotFoundErr
	}

	return b.ReadAt(_record, seqNums[len(seqNums)-1])
}

// Write writes the given record to a segment file and returns the sequence number
func (b *chain) Write(_record Record) (uint64, error) {
	if b.closed {
		return 0, ClosedErr
	}

	_segment, err := b.segment()
	if err != nil {
		return 0, err
	}
	newSeqNum := b.seqNum + 1
	_record.SetSeqNum(newSeqNum)

	offset, err := _segment.Write(_record.ForSegment())
	if err != nil {
		return 0, err
	}
	b.seqNum = newSeqNum
	b.consider(_record.Key(), newSeqNum, offset, _segment)

	return newSeqNum, nil
}

// Truncate - removes all entries whose sequence number is greater or equal to _seqNum. the removal is permanently.
// If want to delete an entry use Write() with empty data and compress the chain.
func (b *chain) Truncate(_seqNum uint64) error {
	if b.closed {
		return ClosedErr
	}

	truncSegmentOffsets := make(map[segment.Segment]int64)
	stripKeys := make([]uint64, 0, 10)
	for i := _seqNum; i <= b.seqNum; i++ {
		if _, ok := b.positions[i]; !ok {
			continue
		}
		recordPos := b.positions[i]
		stripKeys = append(stripKeys, recordPos.Key)
		delete(b.keySeqNums, i)
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

	for _, key := range stripKeys {
		if _, ok := b.keySeqNums[key]; !ok {
			continue
		}
		keySeqNumbers := b.keySeqNums[key]
		newKeySeqNumbers := make([]uint64, 0, len(keySeqNumbers))
		for _, keySeqNum := range keySeqNumbers {
			if keySeqNum >= _seqNum {
				break
			}
			newKeySeqNumbers = append(newKeySeqNumbers, keySeqNum)
		}

		b.keySeqNums[key] = newKeySeqNumbers
	}

	b.seqNum = _seqNum

	return nil
}

// Size returns the size of the chain in bytes
func (b *chain) Size() (int64, error) {
	bSize := int64(0)
	for _, p := range b.segmentList {
		size, err := p.Size()
		if err != nil {
			return 0, fmt.Errorf("can't get size from segment in chain segmentList: %w", err)
		}
		bSize += size
	}
	size, err := b.store.segment.Size()
	if err != nil {
		return 0, fmt.Errorf("can't get size from chain store file: %w", err)
	}
	bSize += size
	return bSize, nil
}

// FirstSeqNum returns the latest sequence numbers for all written keys
func (b *chain) FirstSeqNum() uint64 {
	return b.firstSeqNum
}

// SeqNum returns the latest sequence numbers for all written keys
func (b *chain) SeqNum() uint64 {
	return b.seqNum
}

// KeySeqNums returns sequence numbers for each key
func (b *chain) KeySeqNums() map[uint64][]uint64 {
	return b.keySeqNums
}

// Positions returns the positions for each sequence number
func (b *chain) Positions() map[uint64]Position {
	return b.positions
}

// IsClosed returns true if the chain is already closed
func (b *chain) IsClosed() bool {
	return b.closed
}

// Close closes the chain for read and writes
func (b *chain) Close() error {
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

// segmentName returns a segment name for the chain
func (b *chain) segmentName(startSeqNum uint64) string {
	return b.name + "_" + strconv.FormatUint(startSeqNum, 10)
}

func (b *chain) consider(_key uint64, _seqNum uint64, _offset int64, _segment segment.Segment) {
	if _, ok := b.keySeqNums[_key]; !ok {
		b.keySeqNums[_key] = []uint64{}
	}
	b.keySeqNums[_key] = append(b.keySeqNums[_key], _seqNum)

	b.positions[_seqNum] = Position{
		Key:     _key,
		Offset:  _offset,
		Segment: _segment,
	}
}

// segment returns a segment file
func (b *chain) segment() (segment.Segment, error) {
	listSize := len(b.segmentList)
	if listSize == 0 {
		_, err := b.addSegment()
		if err != nil {
			return nil, err
		}
	}
	_segment := b.segmentList[listSize-1]
	size, err := _segment.Size()
	if err != nil {
		return nil, err
	}

	if size > b.maxSegmentSize {
		var err error
		_segment, err = b.addSegment()
		if err != nil {
			return nil, err
		}
	}

	return _segment, nil
}

func (b *chain) addSegment() (segment.Segment, error) {
	//todo:
	return nil, nil
}
