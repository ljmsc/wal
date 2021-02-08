package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	errMaxSize     = fmt.Errorf("segment already reached maximum size")
	errOffsetBlock = fmt.Errorf("offsetBy must be start of block")
)

type segment struct {
	file    *os.File
	header  header
	offsets []int64
}

func openSegment(_name string, _split int64, _size int64) (*segment, error) {
	s := segment{
		offsets: []int64{},
	}

	var err error
	s.file, err = os.OpenFile(_name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("can't open segment file: %w", err)
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
	if err := s.scan(); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *segment) scan() error {
	offset := s.header.Block
	for {
		pData := make([]byte, recordMetadataLength)
		if _, err := s.file.ReadAt(pData, offset); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("can't read from segment file: %w", err)
		}
		r := record{}
		if err := r.unmarshalMetadata(pData); err != nil {
			return err
		}
		s.offsets = append(s.offsets, offset)
		offset += r.blockC(s.header.Block) * s.header.Block
	}

	return nil
}

func (s *segment) readHeader() error {
	raw := make([]byte, headerLength)
	if _, err := s.file.ReadAt(raw, 0); err != nil {
		return fmt.Errorf("can't read from segment file: %w", err)
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
		return fmt.Errorf("can't write to segment file: %w", err)
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
		return 0, fmt.Errorf("can't read segment file stats: %w", err)
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

func (s *segment) readPage(_offset int64) ([]byte, int64, error) {
	pDelta := _offset % s.header.Page
	pOffset := _offset - (_offset % s.header.Page)

	pData := make([]byte, s.header.Page)
	n, err := s.file.ReadAt(pData, pOffset)
	if err != nil {
		if !errors.Is(err, io.EOF) || n == 0 {
			return nil, 0, fmt.Errorf("can't read from segment file: %w", err)
		}
	}
	return pData[pDelta:], pOffset, nil
}

// readAt reads the record from _offset
func (s *segment) readAt(_record *record, _offset int64) error {
	if !s.isBlock(_offset) {
		return errOffsetBlock
	}

	if _record == nil {
		return fmt.Errorf("record is nil")
	}

	// can't read header as record
	if _offset == 0 {
		return fmt.Errorf("can't read header as offsetBy")
	}

	pData, pOffset, err := s.readPage(_offset)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return ErrEntryNotFound
		}
		return err
	}

	if err := _record.unmarshalMetadata(pData[:recordMetadataLength]); err != nil {
		return err
	}
	buff := bytes.NewBuffer(pData[recordMetadataLength:])
	for {
		if uint64(len(pData)) >= _record.size {
			break
		}
		var pData []byte
		pData, pOffset, err = s.readPage(pOffset + s.header.Page)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return ErrEntryNotFound
			}
			return err
		}

		if _, err := buff.Write(pData); err != nil {
			return fmt.Errorf("can't write payload to buffer: %w", err)
		}
	}

	payload := buff.Bytes()
	if err := _record.unmarshalPayload(payload[:_record.size]); err != nil {
		return err
	}

	return nil
}

// readFrom reads _n records starting from _offset
func (s *segment) readFrom(_offset int64, _n int64) (<-chan envelope, error) {
	out := make(chan envelope)

	if !s.isBlock(_offset) {
		return nil, errOffsetBlock
	}

	go func() {
		defer close(out)
		var pData []byte
		pOffset := _offset
		var _record record
		for {
			var err error
			pData, pOffset, err = s.readPage(pOffset)
			if err != nil {
				out <- envelope{err: err}
				return
			}

			if !_record.validMeta() {
				if err := _record.unmarshalMetadata(pData[:recordMetadataLength]); err != nil {
					out <- envelope{err: err}
					return
				}
			}
			pOffset += s.header.Page
		}
	}()

	return out, nil
}

// write writes the record to the segment
func (s *segment) write(_record record) (int64, error) {
	// check if enough space is available to write all blocks
	if s.free() < _record.blockC(s.header.Block) {
		return 0, errMaxSize
	}

	// get current size as offsetBy for written data
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
		return 0, fmt.Errorf("can't write to segment file: %w", err)
	}

	s.offsets = append(s.offsets, wOff)
	return wOff, nil
}

// sync syncs the written data to disk
func (s *segment) sync() error {
	return s.file.Sync()
}

// offsetBy returns the offsetBy of the record on position _pos. _pos must be > 0
func (s *segment) offsetBy(_pos uint64) (int64, error) {
	if _pos <= 0 {
		return 0, fmt.Errorf("invalid position: %d", _pos)
	}
	if uint64(len(s.offsets)) < _pos {
		return 0, ErrEntryNotFound
	}

	return s.offsets[_pos-1], nil
}

// truncate .
func (s *segment) truncate(_offset int64) error {
	if !s.isBlock(_offset) {
		return errOffsetBlock
	}
	return s.file.Truncate(_offset)
}

// close closes the segment file
func (s *segment) close() error {
	return s.file.Close()
}
