package segment

import (
	"fmt"
	"io"
	"os"
)

// Segment File format
// [Record [Record Header [Key 8][Data Size 8]][Record Data ~]]...

// Segment .
type Segment interface {
	Name() string
	ReadAt(_record Record, _offset int64) error
	ReadKey(_record Record, _key uint64) error
	Write(_record Record) (int64, error)
	Truncate(_dumpOffset int64) error
	Size() (int64, error)
	Offset() int64
	Offsets() []int64
	KeyOffsets() map[uint64][]int64
	IsClosed() bool
	Close() error
}

// Keys returns a slice of all key written to the segment
func Keys(_segment Segment) []uint64 {
	keyOffsets := _segment.KeyOffsets()
	keys := make([]uint64, 0, len(keyOffsets))
	for key := range keyOffsets {
		keys = append(keys, key)
	}
	return keys
}

// LatestKeyOffsets returns for each key the last written offset
func LatestKeyOffsets(_segment Segment) map[uint64]int64 {
	keyOffset := make(map[uint64]int64)
	for key, offsets := range _segment.KeyOffsets() {
		keyOffset[key] = offsets[len(offsets)-1]
	}
	return keyOffset
}

// LatestOffsets returns the latest offsets written to the segment
func LatestOffsets(_segment Segment) []int64 {
	keyOffset := LatestKeyOffsets(_segment)
	latestOffsets := make([]int64, 0, len(keyOffset))
	for _, offset := range keyOffset {
		latestOffsets = append(latestOffsets, offset)
	}
	return latestOffsets
}

// Stream returns a channel which receives all records for the given offsets.
// if the segments is closed the function returns nil
func Stream(_segment Segment, _offsets []int64) <-chan RecordEnvelope {
	if _segment.IsClosed() {
		return nil
	}
	stream := make(chan RecordEnvelope)
	go func() {
		defer close(stream)
		for _, offset := range _offsets {
			r := record{}
			if err := _segment.ReadAt(&r, offset); err != nil {
				stream <- RecordEnvelope{
					Offset: offset,
					Err:    err,
				}
				continue
			}
			stream <- RecordEnvelope{
				Offset: offset,
				Record: &r,
			}
		}
	}()
	return stream
}

// Remove removes the segment from disc
func Remove(_segment Segment) error {
	if !_segment.IsClosed() {
		return NotClosedErr
	}

	if err := os.Remove(_segment.Name()); err != nil {
		return fmt.Errorf("can't delete segment file from disk: %w", err)
	}
	return nil
}

// Snapshot creates a snapshot of the given segment. the segment file content will be copied to a new file called name
func Snapshot(_segment Segment, _snapshotName string) error {
	if _, err := os.Stat(_snapshotName); !os.IsNotExist(err) {
		return fmt.Errorf("file with name %s already exists: %w", _snapshotName, err)
	}

	snapFile, err := os.Create(_snapshotName)
	if err != nil {
		return fmt.Errorf("can't create snapshot file: %w", err)
	}
	defer snapFile.Close()

	pfile, err := os.Open(_segment.Name())
	if err != nil {
		return fmt.Errorf("can't open segment file: %w", err)
	}
	defer pfile.Close()

	if _, err := io.Copy(snapFile, pfile); err != nil {
		return fmt.Errorf("can't copy content to snapshot file: %w", err)
	}

	return nil
}

// segment is the representation of the segment file
type segment struct {
	// name is the name if the segment file
	name string
	// keyOffsets - the map key is the value of the record Key and the value is the segment file offset
	keyOffsets map[uint64][]int64
	// offsets - the offsets of the written records
	offsets []int64
	// file is the segment file
	file *os.File
	// closed
	closed bool
}

// Open opens a segment file with the given name. If the segment already exists the segment is read.
// if handler function is defined it is called for each record in the segment and provides Header information
func Open(_name string, _handler func(_segment Segment, _envelope HeaderEnvelope) error) (Segment, error) {
	return OpenWithPadding(_name, 0, _handler)
}

// OpenWithPadding - same as Open but with padding for payload
// the padding defines the length of payload bytes are read in addition to the Header.
// use padding 0 for Header only or just use Open
func OpenWithPadding(_name string, _padding uint64, _handler func(_segment Segment, _envelope HeaderEnvelope) error) (Segment, error) {
	s := segment{
		name:       _name,
		keyOffsets: make(map[uint64][]int64),
		offsets:    []int64{},
		closed:     false,
	}
	var err error
	s.file, err = os.OpenFile(_name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("can't open segment file: %w", err)
	}

	recordStream := s.streamHeaderWithPadding(_padding)
	for envelope := range recordStream {
		if envelope.Err != nil {
			return nil, envelope.Err
		}

		s.consider(envelope.Offset, envelope.Header.Key)

		if _handler != nil {
			if err := _handler(&s, envelope); err != nil {
				return nil, fmt.Errorf("can't execute _handler for record: %w", err)
			}
		}
	}

	return &s, nil
}

// Name returns the name of the segment as presented to Open.
func (s *segment) Name() string {
	return s.name
}

// ReadAt reads the record at a given offset in the segment file
func (s *segment) ReadAt(_record Record, _offset int64) error {
	if s.closed {
		return ClosedErr
	}

	recordSize, err := s.calculateRecordSize(_offset)
	if err != nil {
		return err
	}

	rawRecord := make([]byte, recordSize)
	if _, err := s.file.ReadAt(rawRecord, _offset); err != nil {
		if err == io.EOF {
			return RecordNotFoundErr
		}
		return fmt.Errorf("can't read record from segment: %w", err)
	}

	if err := decode(_record, rawRecord); err != nil {
		return err
	}

	return nil
}

// ReadKey reads the latest record for a given record key
func (s *segment) ReadKey(_record Record, _key uint64) error {
	if _, ok := s.keyOffsets[_key]; !ok {
		return RecordNotFoundErr
	}
	if len(s.keyOffsets[_key]) == 0 {
		return RecordNotFoundErr
	}
	offset := s.keyOffsets[_key][len(s.keyOffsets[_key])-1]
	return s.ReadAt(_record, offset)
}

// WriteRecord writes the given record at the end of the segment
func (s *segment) Write(_record Record) (int64, error) {
	if s.closed {
		return 0, ClosedErr
	}

	offset, err := s.Size()
	if err != nil {
		return 0, err
	}

	rawRecord, err := encode(_record)
	if err != nil {
		return 0, err
	}

	if _, err := s.file.Write(rawRecord); err != nil {
		return 0, fmt.Errorf("can't write to segment file: %w", err)
	}

	if err := s.file.Sync(); err != nil {
		return 0, fmt.Errorf("can't sync file changes to disk: %w", err)
	}

	s.consider(offset, _record.Key())

	return offset, nil
}

// Truncate - removes all records whose offset is greater or equal to offset. the removal is permanently.
func (s *segment) Truncate(_dumpOffset int64) error {
	if s.closed {
		return ClosedErr
	}

	info, err := os.Stat(s.name)
	if err != nil {
		return fmt.Errorf("can't get file info from file: %w", err)
	}
	currentOffset := info.Size()
	if currentOffset <= _dumpOffset {
		return fmt.Errorf("can't truncate segment. _dumpOffset to big")
	}

	if err := s.file.Truncate(_dumpOffset); err != nil {
		return fmt.Errorf("can't truncate file: %w", err)
	}

	newRecordOffsets := make([]int64, 0, len(s.offsets))
	for _, recordOffset := range s.offsets {
		if recordOffset >= _dumpOffset {
			break
		}
		newRecordOffsets = append(newRecordOffsets, recordOffset)
	}
	newRecordKeyOffsets := make(map[uint64][]int64)
	for key, recordOffsets := range s.keyOffsets {
		if len(recordOffsets) == 0 {
			continue
		}
		for _, recordOffset := range recordOffsets {
			if recordOffset >= _dumpOffset {
				continue
			}
			newRecordKeyOffsets[key] = append(newRecordKeyOffsets[key], recordOffset)
		}
	}

	s.offsets = newRecordOffsets
	s.keyOffsets = newRecordKeyOffsets

	return nil
}

// Size returns the byte size of the segment file
func (s *segment) Size() (int64, error) {
	// can't use seek function since file is append mode - use os.Stat instead
	info, err := os.Stat(s.name)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Offset returns the offset of the last record which was written to the segment file
func (s *segment) Offset() int64 {
	if offsetCount := len(s.offsets); offsetCount > 0 {
		return s.offsets[offsetCount-1]
	}
	return 0
}

// Offsets returns the offsets of all written record in the segment file
func (s *segment) Offsets() []int64 {
	return s.offsets
}

// KeyOffsets returns for each key all written offsets
func (s *segment) KeyOffsets() map[uint64][]int64 {
	return s.keyOffsets
}

// IsClosed returns true if the segment is already closed
func (s *segment) IsClosed() bool {
	return s.closed
}

// Close closes the segment, make it unusable for I/O.
// Close will return an error if it has already been called.
func (s *segment) Close() error {
	s.closed = true
	return s.file.Close()
}

// streamHeaderWithPadding scans the segment file on disc from beginning and decodes the raw data to record objects.
// the padding defines the length of payload bytes are read in in addition to the Header bytes.
// use padding 0 for Header only
func (s *segment) streamHeaderWithPadding(_padding uint64) <-chan HeaderEnvelope {
	readLength := headerLength + _padding

	stream := make(chan HeaderEnvelope)
	go func() {
		defer close(stream)

		currOffset := int64(0)
		for {
			raw := make([]byte, readLength)
			n, err := s.file.ReadAt(raw, currOffset)
			if n > 0 && uint64(n) != readLength {
				stream <- HeaderEnvelope{Err: fmt.Errorf("can't read record data from file")}
				return
			}
			if err != nil {
				if err == io.EOF {
					return
				}
				stream <- HeaderEnvelope{Err: err}
				return
			}
			h := Header{}
			if err := decodeWithPadding(&h, raw); err != nil {
				stream <- HeaderEnvelope{Err: err}
				return
			}

			stream <- HeaderEnvelope{
				Offset: currOffset,
				Header: h,
			}

			currOffset += headerLength + int64(h.PayloadSize)
		}
	}()

	return stream
}

func (s *segment) consider(_offset int64, _key uint64) {
	if s.offsets == nil {
		s.offsets = []int64{}
	}

	if s.keyOffsets == nil {
		s.keyOffsets = make(map[uint64][]int64)
	}

	s.offsets = append(s.offsets, _offset)
	if _, ok := s.keyOffsets[_key]; !ok {
		s.keyOffsets[_key] = []int64{}
	}

	s.keyOffsets[_key] = append(s.keyOffsets[_key], _offset)

	listLen := len(s.offsets)
	if listLen > 1 {
		if s.offsets[listLen-1] < s.offsets[listLen-2] {
			// switch slice items if not sorted
			temp := s.offsets[listLen-2]
			s.offsets[listLen-2] = s.offsets[listLen-1]
			s.offsets[listLen-1] = temp
		}
	}
}

func (s *segment) calculateRecordSize(_offset int64) (int64, error) {
	nextOffsetIndex := 0
	for i, offset := range s.offsets {
		if offset == _offset {
			nextOffsetIndex = i + 1
			break
		}
	}
	if nextOffsetIndex == 0 {
		return 0, RecordNotFoundErr
	}

	if nextOffsetIndex > len(s.offsets)-1 {
		s, err := s.Size()
		if s < _offset && err != nil {
			return 0, err
		}
		return s - _offset, nil
	}

	return (s.offsets[nextOffsetIndex] - _offset), nil
}
