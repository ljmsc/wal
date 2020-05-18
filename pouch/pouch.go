package pouch

import (
	"encoding/binary"
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

// PouchName is the representation of the pouch file
type Pouch struct {
	// name is the name if the pouch file
	name string
	// recordOffsets - the map key is the hash value of the record Key and the value is the pouch file offset
	recordOffsets map[uint64]int64
	//lastWrittenOffset is the last written offset
	lastWrittenOffset int64
	// recordCount counts the records in the pouch
	recordCount uint64
	// dataMutex is a mutex to protect the cached data in recordOffsets and lastWrittenOffset
	dataMutex sync.RWMutex
	// file is the pouch file
	file *os.File
	// fileMutex is a mutex to protect the pouch file
	fileMutex sync.RWMutex
	// closed
	closed bool
}

// Open opens a pouch file with the given name. If the pouch already exists the pouch is read.
func Open(name string) (*Pouch, error) {
	return OpenWithHandler(name, nil)
}

// Open opens a pouch file with the given name. If the pouch already exists the pouch is read.
func OpenWithHandler(name string, handler func(r Envelope) error) (*Pouch, error) {
	s := Pouch{
		name:              name,
		recordOffsets:     make(map[uint64]int64),
		lastWrittenOffset: 0,
		recordCount:       0,
		dataMutex:         sync.RWMutex{},
		fileMutex:         sync.RWMutex{},
		closed:            false,
	}
	var err error
	s.file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("can't open pouch file: %w", err)
	}

	recordStream := s.StreamRecords(true)
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
		s.lastWrittenOffset = item.Offset
		s.recordOffsets[item.Record.Key.Hash()] = item.Offset
		s.recordCount++
	}
	return &s, nil
}

// readRecordHeader .
// [8 Record Size][8 RecordHeaderSize][RecordHeader]
func (p *Pouch) readRecordHeader(offset int64, r *Record) (int64, error) {
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
		return 0, fmt.Errorf("can't read record size from pouch file: %w", err)
	}
	recordSize, _ := binary.Varint(recordSizeBytes)

	recordHeaderSizeBytes := make([]byte, MetaRecordHeaderSizeField)
	if _, err := p.file.ReadAt(recordHeaderSizeBytes, offset+MetaRecordSizeField); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("can't read record size from pouch file: %w", err)
	}
	recordHeaderSize, _ := binary.Varint(recordHeaderSizeBytes)

	recordHeaderBytes := make([]byte, recordHeaderSize)
	if _, err := p.file.ReadAt(recordHeaderBytes, offset+MetaRecordSizeField+MetaRecordHeaderSizeField); err != nil {
		return 0, fmt.Errorf("can't read record data bytes from pouch file: %w", err)
	}

	if err := ParseRecord(recordHeaderBytes, r); err != nil {
		return 0, fmt.Errorf("can't parse record byte to record struct: %w", err)
	}
	r.HeadOnly = true
	return recordSize, nil
}

// readRecord .
// [8 Record Size][8 RecordHeaderSize][RecordHeader][RecordData]
func (p *Pouch) readRecord(offset int64, r *Record) (int64, error) {
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
		return 0, fmt.Errorf("can't read record size from pouch file: %w", err)
	}
	recordSize, _ := binary.Varint(recordSizeBytes)

	recordBytes := make([]byte, recordSize)
	if _, err := p.file.ReadAt(recordBytes, offset+MetaRecordSizeField+MetaRecordHeaderSizeField); err != nil {
		return 0, fmt.Errorf("can't read record data bytes from pouch file: %w", err)
	}

	if err := ParseRecord(recordBytes, r); err != nil {
		return 0, fmt.Errorf("can't parse record byte to record struct: %w", err)
	}

	return recordSize, nil
}

// writeRecord .
// [8 Record Size][8 RecordHeaderSize][RecordHeader][RecordData]
func (p *Pouch) writeRecord(r *Record) (int64, error) {
	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()
	if p.closed {
		return 0, ClosedErr
	}

	if r.HeadOnly {
		return 0, OnlyHeaderErr
	}

	if err := r.Validate(); err != nil {
		return 0, fmt.Errorf("record is not valid: %w", err)
	}

	offset, err := p.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("can't seek offset from file: %w", err)
	}

	fullSize := r.Size()
	fullSizeBytes := make([]byte, MetaRecordSizeField)
	binary.PutVarint(fullSizeBytes, fullSize)

	headerSize := r.HeaderSize()
	headerSizeBytes := make([]byte, MetaRecordHeaderSizeField)
	binary.PutVarint(headerSizeBytes, headerSize)

	b := make([]byte, 0, MetaRecordSizeField+MetaRecordHeaderSizeField+fullSize)
	b = append(b, fullSizeBytes...)
	b = append(b, headerSizeBytes...)
	b = append(b, r.HeaderBytes()...)
	b = append(b, r.Data...)

	if _, err := p.file.Write(b); err != nil {
		return 0, fmt.Errorf("can't write to pouch file: %w", err)
	}

	if err := p.file.Sync(); err != nil {
		return 0, fmt.Errorf("can't sync file changes to disk: %w", err)
	}

	p.dataMutex.Lock()
	p.lastWrittenOffset = offset
	p.recordOffsets[r.Key.Hash()] = offset
	p.recordCount++
	p.dataMutex.Unlock()

	return offset, nil
}

// StreamRecords returns a channel which will receive all records in the pouch file.
// HeadOnly = true returns only header data
func (p *Pouch) StreamRecords(headOnly bool) <-chan Envelope {
	stream := make(chan Envelope)
	go func() {
		var offset int64 = 0
		for {
			r := Record{}
			var err error
			var size int64
			if headOnly {
				size, err = p.readRecordHeader(offset, &r)
			} else {
				size, err = p.readRecord(offset, &r)
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				stream <- Envelope{Err: &ReadErr{Offset: offset, Err: err}}
				break
			}
			stream <- Envelope{Offset: offset, Record: &r}
			offset = offset + MetaRecordSizeField + MetaRecordHeaderSizeField + size
		}
		close(stream)
	}()
	return stream
}

// StreamLatestRecords returns a channel which will receive the latest records in the pouch file. each key at least ones.
// HeadOnly = true returns only header data
func (p *Pouch) StreamLatestRecords(headOnly bool) <-chan Envelope {
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
				stream <- Envelope{Err: &ReadErr{Offset: offset, Err: err}}
				continue
			}
			stream <- Envelope{Offset: offset, Record: &r}
		}
		close(stream)
	}()
	return stream
}

// Name returns the name of the pouch as presented to Open.
func (p *Pouch) Name() string {
	return p.name
}

// Close closes the pouch, make it unusable for I/O.
// Close will return an error if it has already been called.
func (p *Pouch) Close() error {
	p.fileMutex.Lock()
	defer p.fileMutex.Unlock()
	p.closed = true

	return p.file.Close()
}

// ReadByOffset reads the record at a given offset in the pouch file
func (p *Pouch) ReadByOffset(offset int64, headOnly bool, r *Record) error {
	var err error
	if headOnly {
		_, err = p.readRecordHeader(offset, r)
	} else {
		_, err = p.readRecord(offset, r)
	}

	if err != nil {
		return &ReadErr{Offset: offset, Err: err}
	}

	return nil
}

// ReadByHash reads the latest record for a given key hash value
func (p *Pouch) ReadByHash(keyHash uint64, headOnly bool, r *Record) error {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	if _, ok := p.recordOffsets[keyHash]; !ok {
		return RecordNotFoundErr
	}
	offset := p.recordOffsets[keyHash]
	return p.ReadByOffset(offset, headOnly, r)
}

// ReadByKey reads the latest record for a given key
func (p *Pouch) ReadByKey(key Key, headOnly bool, r *Record) error {
	return p.ReadByHash(key.Hash(), headOnly, r)
}

// Write writes the given key and data as a record at the end of the pouch
func (p *Pouch) Write(key Key, data Data) (int64, error) {
	return p.WriteRecord(&Record{
		Key:  key,
		Data: data,
	})
}

// WriteRecord writes the given record at the end of the pouch
func (p *Pouch) WriteRecord(record *Record) (int64, error) {
	offset, err := p.writeRecord(record)
	if err != nil {
		return 0, &WriteErr{Err: err}
	}
	return offset, nil
}

// Size returns the byte size of the pouch file
func (p *Pouch) Size() (int64, error) {
	if p.closed {
		return 0, ClosedErr
	}
	fileInfo, err := p.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("can't get pouch file info: %w", err)
	}
	return fileInfo.Size(), nil
}

// LastOffset returns the offset of the last record which was written to the pouch file
func (p *Pouch) LastOffset() int64 {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	return p.lastWrittenOffset
}

func (p *Pouch) LastOffsets() []int64 {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	offsets := make([]int64, 0, len(p.recordOffsets))
	for _, offset := range p.recordOffsets {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	return offsets
}

// LastOffsetForKey returns the offset of the last record for a given key
func (p *Pouch) LastOffsetForKey(key Key) (int64, error) {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	if _, ok := p.recordOffsets[key.Hash()]; !ok {
		return 0, KeyNotFoundErr
	}

	return p.recordOffsets[key.Hash()], nil
}

// IsClosed returns true if the pouch is already closed
func (p *Pouch) IsClosed() bool {
	return p.closed
}

// Count returns the amount of records in the pouch
func (p *Pouch) Count() uint64 {
	return p.recordCount
}

// Snapshot creates a snapshot of the current pouch. the pouch file content will be copied to a new file called name
func (p *Pouch) Snapshot(name string) error {
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
		return fmt.Errorf("can't open pouch file: %w", err)
	}
	defer pfile.Close()

	if _, err := io.Copy(snapFile, pfile); err != nil {
		return fmt.Errorf("can't copy content to snapshot file: %w", err)
	}

	return nil
}

// Remove deletes the pouch file from disk
func (p *Pouch) Remove() error {
	p.dataMutex.RLock()
	defer p.dataMutex.RUnlock()
	if !p.closed {
		return NotClosedErr
	}

	if err := os.Remove(p.name); err != nil {
		return fmt.Errorf("can't delete pouch file from disk: %w", err)
	}
	return nil
}
