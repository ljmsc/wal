package wal

import (
	"fmt"
	"os"
)

var (
	maxSizeErr     = fmt.Errorf("segment already reached maximum size")
	offsetBlockErr = fmt.Errorf("offset must be start of block")
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
	ps, err := pageSize()
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

func (s *segment) size() (int64, error) {
	stat, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// free returns the free block space of the segment
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
	_block := block{}
	raw := make([]byte, s.header.Block)
	if _, err := s.file.ReadAt(raw, _offset); err != nil {
		return nil, err
	}
	if err := _block.unmarshal(raw); err != nil {
		return nil, err
	}

	_blocks := make([]block, 1, _block.Follow+1)

	// add the first block
	_blocks[0] = _block
	if _block.Follow > 0 {
		raw := make([]byte, s.header.Block*int64(_block.Follow))
		if _, err := s.file.ReadAt(raw, _offset+s.header.Block); err != nil {
			return nil, err
		}
		fb, err := fragment(raw, s.header.Block)
		if err != nil {
			return nil, err
		}
		_blocks = append(_blocks, fb...)
	}

	return nil
}

// write writes the record to the segment
func (s *segment) write(_record record) (int64, error) {
	// check if enough space is available to write all blocks
	if s.free() < int64(len(_blocks)) {
		return 0, maxSizeErr
	}

	// get current size as offset for written data
	wOff, err := s.size()
	if err != nil {
		return 0, err
	}

	// combine blocks to byte slice
	raw, err := deFragment(_blocks, s.header.Block)
	if err != nil {
		return 0, err
	}

	// write combined data to segment file
	if _, err := s.file.Write(raw); err != nil {
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
