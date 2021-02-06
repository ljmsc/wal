package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	maxSizeErr     = fmt.Errorf("segment already reached maximum size")
	offsetBlockErr = fmt.Errorf("offset must be start of block")
	recordNotFound = fmt.Errorf("record not found")
)

type segment struct {
	file   *os.File
	header header
}

func createSegment(_name string, _split int64, _size int64) (*segment, error) {
	s := segment{}

	var err error
	s.file, err = os.OpenFile(_name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	size, err := s.size()
	if err != nil {
		return nil, err
	}
	if size == 0 {
		if err := s.writeHeader(_split, _size); err != nil {
			return nil, err
		}
		return &s, nil
	}
	if err := s.readHeader(); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *segment) readHeader() error {
	raw := make([]byte, headerLength)
	if _, err := s.file.ReadAt(raw, 0); err != nil {
		return err
	}
	if err := s.header.unmarshal(raw); err != nil {
		return err
	}
	return nil
}

func (s *segment) writeHeader(_split int64, _size int64) error {
	ps, err := pageSize(s.file.Name())
	if err != nil {
		return err
	}
	if (ps % _split) != 0 {
		return fmt.Errorf("_split must be a divider of the os block sys")
	}

	blksize := ps / _split
	// segment file is empty
	s.header = header{
		Version: 1,
		Page:    ps,
		Block:   blksize,
		Size:    _size,
	}
	raw := make([]byte, blksize)
	rawh, err := s.header.marshal()
	if err != nil {
		return err
	}

	copy(raw, rawh)

	if _, err := s.file.Write(raw); err != nil {
		return err
	}
	return nil
}

// isBlock returns true if _offset is the start of a new block, otherwise false
func (s *segment) isBlock(_offset int64) bool {
	return _offset%s.header.Block == 0
}

func (s *segment) page(_offset int64) int64 {
	return _offset - (_offset % s.header.Page)
}

func (s *segment) size() (int64, error) {
	stat, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// free returns the free blocks in the segment
func (s *segment) free() int64 {
	size, err := s.size()
	if err != nil {
		return 0
	}
	return s.header.Size - (size / s.header.Block)
}

// readAt reads the record from _offset
func (s *segment) readAt(_record *record, _offset int64) error {
	if !s.isBlock(_offset) {
		return offsetBlockErr
	}

	// can't read header as record
	if _offset == 0 {
		return fmt.Errorf("can't read header as offset")
	}

	pa := s.page(_offset)
	rs := s.header.Page - (_offset - pa)
	raw := make([]byte, rs)
	n, err := s.file.ReadAt(raw, _offset)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			return recordNotFound
		}
	}

	if err := _record.unmarshal(raw); err != nil {
		if !errors.Is(err, invalidChecksumErr) || _record.size <= (uint64(s.header.Page)-recordMetadataLength) {
			return err
		}
	}

	// more than one block
	if _record.size > (uint64(s.header.Page) - recordMetadataLength) {
		raw := make([]byte, _record.size-uint64(len(_record.payload)))
		if _, err := s.file.ReadAt(raw, pa+s.header.Page); err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			if n == 0 {
				return recordNotFound
			}
		}
		if err := _record.appendPayload(raw); err != nil {
			return err
		}
	}

	return nil
}

// write writes the record to the segment
func (s *segment) write(_record record) (int64, error) {
	// check if enough space is available to write all blocks
	if s.free() < _record.blockC(s.header.Block) {
		return 0, maxSizeErr
	}

	// get current size as offset for written data
	wOff, err := s.size()
	if err != nil {
		return 0, err
	}

	raw, err := _record.marshal()
	if err != nil {
		return 0, err
	}

	// inflate raw chunk to block size
	blks := int64(len(raw)) / s.header.Block
	if (int64(len(raw)) % s.header.Block) > 0 {
		blks++
	}
	rawBlocks := make([]byte, blks*s.header.Block)
	copy(rawBlocks, raw)

	// write combined data to segment file
	if _, err := s.file.Write(rawBlocks); err != nil {
		return 0, err
	}

	return wOff, nil
}

// sync syncs the written data to disk
func (s *segment) sync() error {
	return s.file.Sync()
}

// truncate .
func (s *segment) truncate(_offset int64) error {
	if !s.isBlock(_offset) {
		return offsetBlockErr
	}
	return s.file.Truncate(_offset)
}

// close closes the segment file
func (s *segment) close() error {
	return s.file.Close()
}
