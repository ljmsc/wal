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
	Length() uint64
	Size() (int64, error)
	FirstSeqNum() uint64
	SeqNum() uint64
	KeySeqNums() map[uint64][]uint64
	Positions() map[uint64]Position
	IsClosed() bool
	Close() error
}

// Keys returns a slice of all key written to the chain. the slice is not sorted
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
// if _startSeqNum is zero it starts with the first possible record
// if _endSeqNum is zero it sends all possible records in the chain
func Stream(_chain Chain, _startSeqNum uint64, _endSeqNum uint64) <-chan RecordEnvelope {
	if _chain.IsClosed() {
		return nil
	}
	stream := make(chan RecordEnvelope)
	if _startSeqNum == 0 {
		_startSeqNum = _chain.FirstSeqNum()
		_endSeqNum = _chain.SeqNum()
	}
	go func() {
		defer close(stream)
		for seqNum := _startSeqNum; seqNum <= _endSeqNum; seqNum++ {
			r := record{}
			if err := _chain.ReadAt(&r, seqNum); err != nil {
				stream <- RecordEnvelope{
					SeqNum: seqNum,
					Err:    err,
				}
				return
			}
			stream <- RecordEnvelope{
				SeqNum: seqNum,
				Record: &r,
			}
		}
	}()
	return stream
}

// chain .
type chain struct {
	// name is the name if the segment chain. This is a filepath
	name string
	// maxSegmentSize defines the max size of the segment files in the chain in bytes
	maxSegmentSize uint64
	// store is a list of all segments in the segment chain
	store *store
	// firstSeqNum is the first sequence number in the chain. this changes during compression
	firstSeqNum uint64
	// seqNum is the last sequence number written to a segment file in the chain
	seqNum uint64
	// segmentList is the list of segments in the chain
	segmentList []segment.Segment
	// keySeqNums - for each record key all sequence numbers which are already written to segments
	keySeqNums map[uint64][]uint64
	// positions stores the segment and offset for a sequence number
	positions map[uint64]Position
	// closed
	closed bool
}

// Open .
func Open(name string, _padding uint64, maxSegmentSize uint64, handler func(_chain Chain, _envelope HeaderEnvelope) error) (Chain, error) {
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
			seqNum, err := decodeSeqNum(_envelope.PaddingData[:headerSequenceNumberFieldLength])
			if err != nil {
				return err
			}
			c.consider(_envelope.Offset, _envelope.Header.Key, seqNum, _segment)
			if c.firstSeqNum == 0 || c.firstSeqNum > seqNum {
				c.firstSeqNum = seqNum
			}

			if c.seqNum < seqNum {
				c.seqNum = seqNum
			}

			if handler != nil {
				if err := handler(&c, HeaderEnvelope{
					SeqNum:      seqNum,
					Header:      _envelope.Header,
					PaddingData: _envelope.PaddingData[headerSequenceNumberFieldLength:],
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
func (c *chain) Name() string {
	return c.name
}

// ReadAt reads a record by a given sequence number
func (c *chain) ReadAt(_record Record, _seqNum uint64) error {
	if _, ok := c.positions[_seqNum]; !ok {
		return segment.RecordNotFoundErr
	}
	pos := c.positions[_seqNum]
	if pos.Segment == nil {
		return fmt.Errorf("segment not found")
	}

	if err := pos.Segment.ReadAt(_record, pos.Offset); err != nil {
		return err
	}

	return nil
}

// ReadKey reads the latest record for a given key
func (c *chain) ReadKey(_record Record, _key uint64) error {
	if _, ok := c.keySeqNums[_key]; !ok {
		return segment.RecordNotFoundErr
	}

	seqNums := c.keySeqNums[_key]
	if len(seqNums) == 0 {
		return segment.RecordNotFoundErr
	}

	return c.ReadAt(_record, seqNums[len(seqNums)-1])
}

// Write writes the given record to a segment file and returns the sequence number
func (c *chain) Write(_record Record) (uint64, error) {
	if c.closed {
		return 0, ClosedErr
	}

	_segment, err := c.segment()
	if err != nil {
		return 0, err
	}
	newSeqNum := c.seqNum + 1
	_record.SetSeqNum(newSeqNum)

	offset, err := _segment.Write(_record)
	if err != nil {
		return 0, err
	}
	if c.firstSeqNum == 0 {
		c.firstSeqNum = newSeqNum
	}
	c.seqNum = newSeqNum
	c.consider(offset, _record.Key(), newSeqNum, _segment)

	return newSeqNum, nil
}

// Truncate - removes all entries whose sequence number is greater or equal to _seqNum. the removal is permanently.
// If want to delete an entry use Write() with empty data and compress the chain.
func (c *chain) Truncate(_seqNum uint64) error {
	if c.closed {
		return ClosedErr
	}

	// segments which needs to be truncated
	truncSegmentOffsets := make(map[segment.Segment]int64)
	// keys which needs to be striped
	stripKeys := make([]uint64, 0, 10)
	for i := _seqNum; i <= c.seqNum; i++ {
		if _, ok := c.positions[i]; !ok {
			continue
		}
		recordPos := c.positions[i]
		// collect keys which need to be edited
		stripKeys = append(stripKeys, recordPos.Key)
		// delete position for each seq number
		delete(c.positions, i)
		// check if segment is already in the slice
		if offset, ok := truncSegmentOffsets[recordPos.Segment]; ok {
			// if segment is already in the slice, check if offset is greater than stored one
			if offset < recordPos.Offset {
				continue
			}
		}
		// add segment with offset to list
		truncSegmentOffsets[recordPos.Segment] = recordPos.Offset
	}

	// truncate all relevant segment files
	for seg, offset := range truncSegmentOffsets {
		if err := seg.Truncate(offset); err != nil {
			return fmt.Errorf("can't truncate segment: %w", err)
		}
	}

	// update key sequence numbers
	for _, key := range stripKeys {
		if _, ok := c.keySeqNums[key]; !ok {
			continue
		}
		keySeqNumbers := c.keySeqNums[key]
		newKeySeqNumbers := make([]uint64, 0, len(keySeqNumbers))
		for _, keySeqNum := range keySeqNumbers {
			if keySeqNum >= _seqNum {
				break
			}
			newKeySeqNumbers = append(newKeySeqNumbers, keySeqNum)
		}
		c.keySeqNums[key] = newKeySeqNumbers
	}

	c.seqNum = _seqNum - 1
	if c.seqNum <= c.firstSeqNum {
		c.firstSeqNum = c.seqNum
	}

	return nil
}

func (c *chain) Length() uint64 {
	if c.seqNum == 0 && c.firstSeqNum == 0 {
		return 0
	}
	return (c.seqNum - c.firstSeqNum) + 1
}

// Size returns the size of the chain in bytes
func (c *chain) Size() (int64, error) {
	bSize := int64(0)
	for _, p := range c.segmentList {
		size, err := p.Size()
		if err != nil {
			return 0, fmt.Errorf("can't get size from segment in chain segmentList: %w", err)
		}
		bSize += size
	}
	size, err := c.store.segment.Size()
	if err != nil {
		return 0, fmt.Errorf("can't get size from chain store file: %w", err)
	}
	bSize += size
	return bSize, nil
}

// FirstSeqNum returns the latest sequence numbers for all written keys
func (c *chain) FirstSeqNum() uint64 {
	return c.firstSeqNum
}

// SeqNum returns the latest sequence numbers for all written keys
func (c *chain) SeqNum() uint64 {
	return c.seqNum
}

// KeySeqNums returns sequence numbers for each key
func (c *chain) KeySeqNums() map[uint64][]uint64 {
	return c.keySeqNums
}

// Positions returns the positions for each sequence number
func (c *chain) Positions() map[uint64]Position {
	return c.positions
}

// IsClosed returns true if the chain is already closed
func (c *chain) IsClosed() bool {
	return c.closed
}

// Close closes the chain for read and writes
func (c *chain) Close() error {
	c.closed = true
	closeErrors := make([]error, 0, len(c.segmentList))
	for _, seg := range c.segmentList {
		if err := seg.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	err := c.store.close()
	if len(closeErrors) == 0 {
		return err
	}
	closeErrors = append(closeErrors, err)
	return CloseErr{Errs: closeErrors}
}

// segmentName returns a segment name for the chain
func (c *chain) segmentName(startSeqNum uint64) string {
	return c.name + "_" + strconv.FormatUint(startSeqNum, 10)
}

func (c *chain) consider(_offset int64, _key uint64, _seqNum uint64, _segment segment.Segment) {
	if _, ok := c.keySeqNums[_key]; !ok {
		c.keySeqNums[_key] = []uint64{}
	}
	c.keySeqNums[_key] = append(c.keySeqNums[_key], _seqNum)

	c.positions[_seqNum] = Position{
		Key:     _key,
		Offset:  _offset,
		Segment: _segment,
	}
}

// segment returns a segment file
func (c *chain) segment() (segment.Segment, error) {
	listSize := len(c.segmentList)
	if listSize == 0 {
		_, err := c.addSegment()
		if err != nil {
			return nil, err
		}
		listSize += 1
	}
	_segment := c.segmentList[listSize-1]
	size, err := _segment.Size()
	if err != nil {
		return nil, err
	}

	if uint64(size) > c.maxSegmentSize {
		var err error
		_segment, err = c.addSegment()
		if err != nil {
			return nil, err
		}
	}

	return _segment, nil
}

func (c *chain) addSegment() (segment.Segment, error) {
	segmentName := c.segmentName(c.seqNum + 1)
	newSegment, err := segment.Open(segmentName, nil)
	if err != nil {
		return nil, fmt.Errorf("can't open new segment file: %w", err)
	}

	storeList := c.store.get()
	storeList = append(storeList, segmentName)
	if err := c.store.update(storeList); err != nil {
		return nil, fmt.Errorf("can't update store file: %w", err)
	}

	c.segmentList = append(c.segmentList, newSegment)
	return newSegment, nil
}
