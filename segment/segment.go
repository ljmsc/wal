package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

const (
	MetaRecordSizeField       = 8
	MetaRecordHeaderSizeField = 8
)

type Segment interface {
	StreamRecords(headOnly bool) <-chan Envelope
	StreamLatestRecords(headOnly bool) <-chan Envelope
	Name() string
	Close() error
	ReadByOffset(offset int64, headOnly bool, r *Record) error
	ReadByHash(keyHash uint64, headOnly bool, r *Record) error
	ReadByKey(key Key, headOnly bool, r *Record) error
	Write(key Key, data Data) (int64, error)
	WriteRecord(record *Record) (int64, error)
	Truncate(_dumpOffset int64) error
	Size() (int64, error)
	LastOffset() int64
	LastOffsets() []int64
	LastOffsetForKey(key Key) (int64, error)
	IsClosed() bool
	Count() uint64
	Snapshot(name string) error
	Remove() error
}

// segment is the representation of the segment file
type segment struct {
	// name is the name if the segment file
	name string
	// recordKeyOffsets - the map key is the hash value of the record Key and the value is the segment file offset
	recordKeyOffsets map[uint64][]int64
	// recordOffsets - the offsets of the written records
	recordOffsets []int64
	// dataMutex is a mutex to protect the cached data in recordKeyOffsets and lastWrittenOffset
	dataMutex sync.RWMutex
	// file is the segment file
	file *os.File
	// fileMutex is a mutex to protect the segment file
	fileMutex sync.RWMutex
	// closed
	closed bool
}

// Open opens a segment file with the given name. If the segment already exists the segment is read.
func Open(name string) (Segment, error) {
	return OpenWithHandler(name, true, nil)
}

// OpenWithHandler opens a segment file with the given name. If the segment already exists the segment is read.
func OpenWithHandler(name string, headOnly bool, handler func(r Envelope) error) (Segment, error) {
	s := segment{
		name:             name,
		recordKeyOffsets: make(map[uint64][]int64),
		recordOffsets:    make([]int64, 0, 10),
		dataMutex:        sync.RWMutex{},
		fileMutex:        sync.RWMutex{},
		closed:           false,
	}
	var err error
	s.file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("can't open segment file: %w", err)
	}

	recordStream := s.StreamRecords(headOnly)
	for {
		item, ok := <-recordStream
		if !ok {
			break
		}
		if item.Err != nil {
			return nil, fmt.Errorf("can't stream record item: %w", item.Err)
		}

		if handler != nil {
			if err := handler(item); err != nil {
				return nil, fmt.Errorf("can't execute handler for item: %w", err)
			}
		}
		if _, ok := s.recordKeyOffsets[item.Record.Key.Hash()]; !ok {
			s.recordKeyOffsets[item.Record.Key.Hash()] = make([]int64, 0, 10)
		}
		s.recordKeyOffsets[item.Record.Key.Hash()] = append(s.recordKeyOffsets[item.Record.Key.Hash()], item.Offset)
		s.recordOffsets = append(s.recordOffsets, item.Offset)
	}
	return &s, nil
}

// readRecordHeader .
// [8 Record Size][8 RecordHeaderSize][RecordHeader]
func (p *segment) readRecordHeader(offset int64, r *Record) (uint64, error) {
	p.fileMutex.RLock()
	defer p.fileMutex.RUnlock()
	if p.closed {
		return 0, ClosedErr
	}

	recordSizeBytes := make([]byte, MetaRecordSizeField)
	if _, err := p.file.ReadAt(recordSizeBytes, offset); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("can't read record size from segment file: %w", err)
	}
	recordSize := binary.LittleEndian.Uint64(recordSizeBytes)

	recordHeaderSizeBytes := make([]byte, MetaRecordHeaderSizeField)
	if _, err := p.file.ReadAt(recordHeaderSizeBytes, offset+MetaRecordSizeField); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("can't read record header size from segment file: %w", err)
	}
	recordHeaderSize := binary.LittleEndian.Uint64(recordHeaderSizeBytes)

	recordHeaderBytes := make([]byte, recordHeaderSize)
	if _, err := p.file.ReadAt(recordHeaderBytes, offset+MetaRecordSizeField+MetaRecordHeaderSizeField); err != nil {
		return 0, fmt.Errorf("can't read record byte data from segment file: %w", err)
	}

	if err := ParseRecord(recordHeaderBytes, r); err != nil {
		return 0, fmt.Errorf("can't parse record byte to record struct: %w", err)
	}
	r.HeadOnly = true
	return recordSize, nil
}

// readRecord .
// [8 Record Size][8 RecordHeaderSize][RecordHeader][RecordData]
func (p *segment) readRecord(offset int64, r *Record) (uint64, error) {
	p.fileMutex.RLock()
	defer p.fileMutex.RUnlock()
	if p.closed {
		return 0, ClosedErr
	}

	recordSizeBytes := make([]byte, MetaRecordSizeField)
	if _, err := p.file.ReadAt(recordSizeBytes, offset); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("can't read record size from segment file: %w", err)
	}
	recordSize := binary.LittleEndian.Uint64(recordSizeBytes)

	recordBytes := make([]byte, recordSize)
	if _, err := p.file.ReadAt(recordBytes, offset+MetaRecordSizeField+MetaRecordHeaderSizeField); err != nil {
		return 0, fmt.Errorf("can't read record byte data from segment file: %w", err)
	}

	if err := ParseRecord(recordBytes, r); err != nil {
		return 0, fmt.Errorf("can't parse record byte to record struct: %w", err)
	}

	return recordSize, nil
}

// writeRecord .
// [8 Record Size][8 RecordHeaderSize][RecordHeader][RecordData]
func (p *segment) writeRecord(r *Record) (int64, error) {
	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()
	if p.closed {
		return 0, ClosedErr
	}

	if r.HeadOnly {
		return 0, OnlyHeaderErr
	}

	if err := r.Validate(); err != nil {
		return 0, RecordNotValidErr{Err: err}
	}

	offset, err := p.Size()
	if err != nil {
		return 0, fmt.Errorf("can't get current offset from file: %w", err)
	}

	fullSize := r.Size()
	fullSizeBytes := make([]byte, MetaRecordSizeField)
	binary.LittleEndian.PutUint64(fullSizeBytes, fullSize)

	headerSize := r.HeaderSize()
	headerSizeBytes := make([]byte, MetaRecordHeaderSizeField)
	binary.LittleEndian.PutUint64(headerSizeBytes, headerSize)

	b := make([]byte, 0, MetaRecordSizeField+MetaRecordHeaderSizeField+fullSize)
	b = append(b, fullSizeBytes...)
	b = append(b, headerSizeBytes...)
	b = append(b, r.HeaderBytes()...)
	b = append(b, r.Data...)

	if _, err := p.file.Write(b); err != nil {
		return 0, fmt.Errorf("can't write to segment file: %w", err)
	}

	if err := p.file.Sync(); err != nil {
		return 0, fmt.Errorf("can't sync file changes to disk: %w", err)
	}

	p.dataMutex.Lock()
	p.recordKeyOffsets[r.Key.Hash()] = append(p.recordKeyOffsets[r.Key.Hash()], offset)
	p.recordOffsets = append(p.recordOffsets, offset)
	p.dataMutex.Unlock()

	return offset, nil
}

// StreamRecords returns a channel which will receive all records in the segment file.
// HeadOnly = true returns only header data
func (p *segment) StreamRecords(headOnly bool) <-chan Envelope {
	stream := make(chan Envelope)
	go func() {
		var offset int64 = 0
		for {
			r := Record{}
			var err error
			var size uint64
			if headOnly {
				size, err = p.readRecordHeader(offset, &r)
			} else {
				size, err = p.readRecord(offset, &r)
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				stream <- Envelope{Err: err}
				break
			}
			stream <- Envelope{Offset: offset, Record: &r}
			offset = offset + MetaRecordSizeField + MetaRecordHeaderSizeField + int64(size)
		}
		close(stream)
	}()
	return stream
}

// StreamLatestRecords returns a channel which will receive the latest records in the segment file. each key at least ones.
// HeadOnly = true returns only header data
func (p *segment) StreamLatestRecords(headOnly bool) <-chan Envelope {
	stream := make(chan Envelope)
	go func() {
		latestOffsets := p.LastOffsets()
		for _, offset := range latestOffsets {
			r := Record{}
			var err error
			if headOnly {
				_, err = p.readRecordHeader(offset, &r)
			} else {
				_, err = p.readRecord(offset, &r)
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				stream <- Envelope{Err: err}
				continue
			}
			stream <- Envelope{Offset: offset, Record: &r}
		}
		close(stream)
	}()
	return stream
}

// Name returns the name of the segment as presented to Open.
func (p *segment) Name() string {
	return p.name
}

// Close closes the segment, make it unusable for I/O.
// Close will return an error if it has already been called.
func (p *segment) Close() error {
	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()
	p.closed = true

	return p.file.Close()
}

// ReadByOffset reads the record at a given offset in the segment file
func (p *segment) ReadByOffset(offset int64, headOnly bool, r *Record) error {
	var err error
	if headOnly {
		_, err = p.readRecordHeader(offset, r)
	} else {
		_, err = p.readRecord(offset, r)
	}

	if err != nil {
		if errors.Is(err, io.EOF) {
			return InvalidRecordOffsetErr
		}
		return ReadErr{Offset: offset, Err: err}
	}

	return nil
}

// ReadByHash reads the latest record for a given key hash value
func (p *segment) ReadByHash(keyHash uint64, headOnly bool, r *Record) error {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	if _, ok := p.recordKeyOffsets[keyHash]; !ok {
		return RecordNotFoundErr
	}
	if len(p.recordKeyOffsets[keyHash]) == 0 {
		return RecordNotFoundErr
	}
	offset := p.recordKeyOffsets[keyHash][len(p.recordKeyOffsets[keyHash])-1]
	return p.ReadByOffset(offset, headOnly, r)
}

// ReadByKey reads the latest record for a given key
func (p *segment) ReadByKey(key Key, headOnly bool, r *Record) error {
	return p.ReadByHash(key.Hash(), headOnly, r)
}

// Write writes the given key and data as a record at the end of the segment
func (p *segment) Write(key Key, data Data) (int64, error) {
	return p.WriteRecord(&Record{
		Key:  key,
		Data: data,
	})
}

// WriteRecord writes the given record at the end of the segment
func (p *segment) WriteRecord(record *Record) (int64, error) {
	offset, err := p.writeRecord(record)
	if err != nil {
		return 0, WriteErr{Err: err}
	}
	return offset, nil
}

// Truncate - removes all records whose offset is greater or equal to offset. the removal is permanently.
func (p *segment) Truncate(_dumpOffset int64) error {
	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()
	if p.closed {
		return ClosedErr
	}

	info, err := os.Stat(p.name)
	if err != nil {
		return fmt.Errorf("can't get file info from file: %w", err)
	}
	currentOffset := info.Size()
	if currentOffset <= _dumpOffset {
		return fmt.Errorf("can't truncate segment. _dumpOffset to big")
	}
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	if err := p.file.Truncate(_dumpOffset); err != nil {
		return fmt.Errorf("can't truncate file: %w", err)
	}

	newRecordOffsets := make([]int64, 0, len(p.recordOffsets))
	for _, recordOffset := range p.recordOffsets {
		if recordOffset >= _dumpOffset {
			break
		}
		newRecordOffsets = append(newRecordOffsets, recordOffset)
	}
	newRecordKeyOffsets := make(map[uint64][]int64)
	for keyHash, recordOffsets := range p.recordKeyOffsets {
		if len(recordOffsets) == 0 {
			continue
		}
		for _, recordOffset := range recordOffsets {
			if recordOffset >= _dumpOffset {
				continue
			}
			newRecordKeyOffsets[keyHash] = append(newRecordKeyOffsets[keyHash], recordOffset)
		}
	}

	p.recordOffsets = newRecordOffsets
	p.recordKeyOffsets = newRecordKeyOffsets

	return nil
}

// Size returns the byte size of the segment file
func (p *segment) Size() (int64, error) {
	// can't use seek function since file is append mode - use os.Stat instead
	info, err := os.Stat(p.name)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// LastOffset returns the offset of the last record which was written to the segment file
func (p *segment) LastOffset() int64 {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	if n := p.Count(); n > 0 {
		return p.recordOffsets[n-1]
	}
	return 0
}

func (p *segment) LastOffsets() []int64 {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	offsets := make([]int64, 0, len(p.recordKeyOffsets))
	for _, recordOffsets := range p.recordKeyOffsets {
		if len(recordOffsets) > 0 {
			offsets = append(offsets, recordOffsets[len(recordOffsets)-1])
		}
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	return offsets
}

// LastOffsetForKey returns the offset of the last record for a given key
func (p *segment) LastOffsetForKey(key Key) (int64, error) {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	if _, ok := p.recordKeyOffsets[key.Hash()]; !ok {
		return 0, KeyNotFoundErr
	}
	if len(p.recordKeyOffsets[key.Hash()]) == 0 {
		return 0, KeyNotFoundErr
	}
	recordOffsets := p.recordKeyOffsets[key.Hash()]

	return recordOffsets[len(recordOffsets)-1], nil
}

// IsClosed returns true if the segment is already closed
func (p *segment) IsClosed() bool {
	return p.closed
}

// Count returns the amount of records in the segment
func (p *segment) Count() uint64 {
	return uint64(len(p.recordOffsets))
}

// Snapshot creates a snapshot of the current segment. the segment file content will be copied to a new file called name
func (p *segment) Snapshot(name string) error {
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		return fmt.Errorf("file with name %s already exists: %w", name, err)
	}

	snapFile, err := os.Create(name)
	if err != nil {
		return fmt.Errorf("can't create snapshot file: %w", err)
	}
	defer snapFile.Close()

	p.fileMutex.RLock()
	defer p.fileMutex.RUnlock()

	pfile, err := os.Open(p.name)
	if err != nil {
		return fmt.Errorf("can't open segment file: %w", err)
	}
	defer pfile.Close()

	if _, err := io.Copy(snapFile, pfile); err != nil {
		return fmt.Errorf("can't copy content to snapshot file: %w", err)
	}

	return nil
}

// Remove deletes the segment file from disk
func (p *segment) Remove() error {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	if !p.closed {
		return NotClosedErr
	}

	if err := os.Remove(p.name); err != nil {
		return fmt.Errorf("can't delete segment file from disk: %w", err)
	}
	return nil
}