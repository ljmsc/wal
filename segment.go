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
	errOffsetBlock = fmt.Errorf("offsetByPos must be start of block")
)

type segment struct {
	file    *os.File
	header  header
	offsets []int64
	// safe is the last written offset which is flushed/synced to persistent storage
	safe int64
}

// openSegment opens a existing or new segment file
// _name is the full filepath of the segment
// _slit is the amount of blocks which can be written to a single filesystem page
// _size is the amount of blocks which can be written to the segment file. one block is always reserved for the header
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

	if len(s.offsets) > 0 {
		// last offset in the list is the "safe" offset
		s.safe = s.offsets[len(s.offsets)-1]
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
		return fmt.Errorf("_split must be a divider of the os block size")
	}

	blksize := ps / _split
	if blksize < headerLength {
		return fmt.Errorf("the block size must be greater or equal to the segment header (%d bytes)", headerLength)
	}
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

// size returns the current segment size in bytes
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

func (s *segment) readPage(_pageData []byte, _offset int64) (int64, error) {
	if _offset < s.header.Block {
		// segment header is not part of the record data. so we skip the first block
		_offset = s.header.Block
	}
	pDelta := _offset % s.header.Page
	pOffset := _offset - (_offset % s.header.Page)
	if int64(len(_pageData)) < (s.header.Page - pDelta) {
		return 0, fmt.Errorf("provided byte slice to small")
	}

	pData := make([]byte, s.header.Page)
	n, err := s.file.ReadAt(pData, pOffset)
	if err != nil {
		if !errors.Is(err, io.EOF) || n == 0 {
			return 0, fmt.Errorf("can't read from segment file: %w", err)
		}
	}

	retN := int64(copy(_pageData, pData[pDelta:n]))
	return retN, err
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
		return fmt.Errorf("can't read header as offsetByPos")
	}
	pData := make([]byte, s.header.Page)
	n, err := s.readPage(pData, _offset)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			return ErrEntryNotFound
		}
	}

	pData = pData[:n]

	if err := _record.unmarshalMetadata(pData[:recordMetadataLength]); err != nil {
		return err
	}
	buff := bytes.NewBuffer(pData[recordMetadataLength:])
	for {
		if uint64(buff.Len()) >= _record.size {
			break
		}
		pData := make([]byte, s.header.Page)
		n, err = s.readPage(pData, _offset+n+s.header.Page)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return ErrEntryNotFound
			}
			return err
		}
		pData = pData[:n]

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
	if _offset < s.header.Block {
		// segment header is not part of the record data. so we skip the first block
		_offset = s.header.Block
	}

	if !s.isBlock(_offset) {
		return nil, errOffsetBlock
	}

	go func() {
		defer close(out)

		var outc int64
		var eof bool
		rp := createRecordParser(s.header.Block)
		nextOffset := _offset
		var chunk []byte
		for {
			if outc >= _n {
				return
			}
			complete, err := rp.check()
			if err != nil {
				out <- envelope{err: err}
				return
			}
			if complete {
				r := record{}
				parsOffset, err := rp.read(&r)
				if err != nil {
					out <- envelope{err: err}
					return
				}
				out <- envelope{
					offset: parsOffset + _offset,
					record: r,
				}
				if eof {
					return
				}
				outc++
				continue
			}

			chunk = make([]byte, s.header.Page)
			n, err := s.readPage(chunk, nextOffset)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					out <- envelope{err: err}
					return
				}
				if n == 0 {
					return
				}
				// mark segment as EOF, loop should use the rest of the read data and return
				eof = true
			}

			chunk = chunk[:n]
			if err := rp.write(chunk); err != nil {
				out <- envelope{err: err}
				return
			}
			nextOffset += n
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

	// get current size as offsetByPos for written data
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
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("can't sync data to disk: %w", err)
	}

	if len(s.offsets) > 0 {
		s.safe = s.offsets[len(s.offsets)-1]
	}

	return nil
}

// offsetByPos returns the offset of the record on position _pos. _pos must be > 0
func (s *segment) offsetByPos(_pos uint64) (int64, error) {
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
	if err := s.file.Truncate(_offset); err != nil {
		return fmt.Errorf("can't truncate segment: %w", err)
	}
	return s.sync()
}

// close closes the segment file
func (s *segment) close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("can't close segment file: %w", err)
	}
	return nil
}
