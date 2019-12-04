package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	lengthOfRecordSizeField = 8
)

// segment represents a file in the log.
type segment interface {
	Num() uint64
	SequenceBoundaries() (uint64, uint64)
	RecordVersions() map[uint64]uint64
	Offsets(key Key) (offsets []int64, err error)
	Write(record *Record) error
	ReadOffset(offset int64, record *Record) error
	ReadSequenceNum(sequenceNum uint64, record *Record) error
	ReadLatest(key Key, record *Record) error
	IsWritable() bool
	CloseForWriting() error
	Close() error
	Remove() error
}

// createSegment creates a new segmentFile file
func createSegment(segmentNumber uint64, startSeqNum uint64, config Config) (segment, error) {
	if startSeqNum < 1 {
		return nil, errors.New("sequence number must be greater than zero")
	}
	s := segmentFile{
		config:          config,
		mutex:           sync.RWMutex{},
		keyOffsets:      make(map[uint64][]int64),
		sequenceOffsets: make(map[uint64]int64),
		writeOffset:     0,
		closed:          false,
		startSeqNum:     startSeqNum,
		latestSeqNum:    startSeqNum - 1,
		number:          segmentNumber,
		versions:        make(map[uint64]uint64),
	}
	filename := config.SegmentFileDir + "/" + config.SegmentFilePrefix + strconv.FormatUint(segmentNumber, 10)

	var err error
	_, err = os.Stat(filename)
	if err == nil {
		return nil, errors.New("segmentFile file already exists")
	}

	if !os.IsNotExist(err) {
		return nil, err
	}

	if s.file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600); err != nil {
		return nil, err
	}

	return &s, nil
}

// parseSegment parses a existing segmentFile file on disk.
func parseSegment(filename string, config Config) (segment, error) {
	s := segmentFile{
		config:          config,
		mutex:           sync.RWMutex{},
		keyOffsets:      make(map[uint64][]int64),
		sequenceOffsets: make(map[uint64]int64),
		closed:          false,
		versions:        make(map[uint64]uint64),
	}
	if err := s.parseNumber(filename); err != nil {
		return nil, err
	}
	filename = config.SegmentFileDir + "/" + filename

	var err error
	info, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return nil, errors.New("filename is a directory")
	}

	if s.file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0600); err != nil {
		return nil, err
	}

	if err := s.scan(); err != nil {
		return nil, err
	}

	return &s, nil
}

// segmentFile .
type segmentFile struct {
	// the DiskWal configuration
	config Config
	// mutex to avoid concurrent writing
	mutex sync.RWMutex
	// mutex to lock the write function
	writeMutex sync.Mutex
	// the segment file on disk
	file *os.File
	// keyOffsets contains a list of all record positions (offsets) in the segment file for a given key
	keyOffsets map[uint64][]int64
	// sequenceOffsets contains a list of offsets for a given sequence number
	sequenceOffsets map[uint64]int64
	// write offset is the offset of the end of the segment file
	writeOffset int64
	// closed is false as long as the segment file is writable. if the file reaches the max segment size, closed is true
	closed bool
	// the segment file number
	number uint64
	// the sequence number of the first record in the segment file
	startSeqNum uint64
	// the  sequence number of the latest record in the segment file
	latestSeqNum uint64
	// versions stores the latest version for a given key (hash) in this segment file
	versions map[uint64]uint64
}

// Num returns the segmentFile file number
func (s *segmentFile) Num() uint64 {
	return s.number
}

// SequenceBoundaries returns the first and the latest sequence number in the segment file
func (s *segmentFile) SequenceBoundaries() (uint64, uint64) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.startSeqNum, s.latestSeqNum
}

// RecordVersions returns the highest version for all keys in this segment file
func (s *segmentFile) RecordVersions() map[uint64]uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.versions
}

// Offsets writes all offset positions to offsets for a given key.
func (s *segmentFile) Offsets(key Key) (offsets []int64, err error) {
	hash := key.HashSum64()
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if _, ok := s.keyOffsets[hash]; !ok {
		return offsets, ErrNoRecordFound
	}
	offsets = s.keyOffsets[hash]
	return offsets, nil
}

// Write writes the given record to disk and adds the offset to the record
func (s *segmentFile) Write(record *Record) error {
	s.writeMutex.Lock()
	defer s.writeMutex.Unlock()

	s.mutex.RLock()
	if s.closed {
		s.mutex.RUnlock()
		return ErrSegmentFileClosed
	}

	record.meta.sequenceNumber = s.latestSeqNum + 1
	if record.meta.createdAt.IsZero() {
		record.meta.createdAt = time.Now()
	}

	if err := record.isReadyToWrite(); err != nil {
		s.mutex.RUnlock()
		return err
	}
	hash := record.Key.HashSum64()

	if record.Version() <= s.versions[hash] {
		s.mutex.RUnlock()
		return errors.New("version already exists")
	}

	s.mutex.RUnlock()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	recordBytes := record.ToBytes()
	newWriteOffset := s.writeOffset + record.Size() + lengthOfRecordSizeField
	if newWriteOffset > s.config.SegmentMaxSizeBytes {
		s.closed = true
		return ErrSegmentFileClosed
	}
	recordSizeBytes := make([]byte, lengthOfRecordSizeField)
	binary.BigEndian.PutUint64(recordSizeBytes, uint64(record.Size()))
	if _, err := s.file.Write(recordSizeBytes); err != nil {
		return err
	}
	if _, err := s.file.Write(recordBytes); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}

	if _, ok := s.keyOffsets[hash]; !ok {
		s.keyOffsets[hash] = make([]int64, 0, s.config.SegmentIndexTableAlloc)
	}
	s.keyOffsets[hash] = append(s.keyOffsets[hash], s.writeOffset)
	s.versions[hash] = record.Version()
	s.sequenceOffsets[record.meta.sequenceNumber] = s.writeOffset
	record.meta.offset = s.writeOffset
	s.writeOffset = newWriteOffset
	s.latestSeqNum++
	return nil
}

// ReadOffset reads the record from the given offset from the segmentFile file
func (s *segmentFile) ReadOffset(offset int64, record *Record) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	recordSizeBytes := make([]byte, lengthOfRecordSizeField)
	if _, err := s.file.ReadAt(recordSizeBytes, offset); err != nil {
		return err
	}
	recordSize := binary.BigEndian.Uint64(recordSizeBytes)
	recordBytes := make([]byte, recordSize)
	if _, err := s.file.ReadAt(recordBytes, offset+lengthOfRecordSizeField); err != nil {
		return err
	}
	return record.FromBytes(recordBytes)
}

// ReadSequenceNum reads the record for a given sequence number
func (s *segmentFile) ReadSequenceNum(sequenceNum uint64, record *Record) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if _, ok := s.sequenceOffsets[sequenceNum]; !ok {
		return ErrNoRecordFound
	}
	offset := s.sequenceOffsets[sequenceNum]
	return s.ReadOffset(offset, record)
}

// ReadLatest .
func (s *segmentFile) ReadLatest(key Key, record *Record) error {
	s.mutex.RLock()
	hash := key.HashSum64()
	if _, ok := s.keyOffsets[hash]; !ok {
		s.mutex.RUnlock()
		return ErrNoRecordFound
	}
	offset := s.keyOffsets[hash][len(s.keyOffsets[hash])-1]
	s.mutex.RUnlock()
	return s.ReadOffset(offset, record)
}

// IsWritable returns true if the segmentFile is writable and false if not
func (s *segmentFile) IsWritable() bool {
	return !s.closed
}

// CloseForWriting closes the segmentFile for further writes
func (s *segmentFile) CloseForWriting() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.closed = true
	return nil
}

// Close closes the open file handler to the segmentFile file on disk
func (s *segmentFile) Close() error {
	return s.file.Close()
}

// Remove deletes the segmentFile file
func (s *segmentFile) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.file.Name()); err != nil {
		return err
	}
	return nil
}

// scan scans the segmentFile file for records. scan is not thread safe
func (s *segmentFile) scan() error {
	var offset int64 = 0
	for {
		record := Record{}
		err := s.ReadOffset(offset, &record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if offset == 0 {
			s.startSeqNum = record.meta.sequenceNumber
		}
		s.latestSeqNum = record.meta.sequenceNumber
		hash := record.Key.HashSum64()

		if _, ok := s.keyOffsets[hash]; !ok {
			s.keyOffsets[hash] = make([]int64, 0, 1)
		}

		if record.Version() > s.versions[hash] {
			s.versions[hash] = record.Version()
		}

		s.keyOffsets[hash] = append(s.keyOffsets[hash], offset)
		s.sequenceOffsets[record.meta.sequenceNumber] = offset
		offset = offset + record.Size() + lengthOfRecordSizeField
	}

	s.writeOffset = offset
	return nil
}

func (s *segmentFile) parseNumber(filename string) error {
	filename = filepath.Base(filename)
	strNum := strings.Trim(filename, s.config.SegmentFilePrefix)
	var err error
	s.number, err = strconv.ParseUint(strNum, 10, 64)
	if err != nil {
		return err
	}
	return nil
}
