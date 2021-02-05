package segment

import (
	"bytes"
	"encoding"
	"fmt"
	"os"
	"syscall"
)

// Segment .
type Segment interface {
	Name() string
	Block() uint64
	ReadAt(_record encoding.BinaryUnmarshaler, _offset uint64) error
	Write(_record encoding.BinaryMarshaler) (uint64, error)
	Truncate(_offset uint64) error
	Size() (uint64, error)
	IsClosed() bool
	Close() error
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

// segment is the representation of the segment file
type segment struct {
	// name is the name if the segment file
	name string
	// header contains the header information of the segment file
	header header
	// file is the segment file
	file *os.File
	// closed
	closed bool
}

// Open opens a segment file with the given name
func Open(_name string, _block uint64, _size uint64) (Segment, error) {
	s := segment{
		name:   _name,
		closed: false,
	}

	var err error
	s.file, err = os.OpenFile(_name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("can't open segment file: %w", err)
	}

	size, err := s.Size()
	if err != nil {
		return nil, err
	}
	// segment file is empty
	if size == 0 {
		stat := syscall.Stat_t{}
		if err := syscall.Stat(_name, &stat); err != nil {
			return nil, err
		}
		s.header = header{
			Version: V1,
			Page:    uint64(stat.Blksize),
			Block:   _block,
			Size:    _size,
		}
		if err := s.writeHead(); err != nil {
			return nil, err
		}
	}

	if err := s.readHead(); err != nil {
		return nil, fmt.Errorf("can't read segment file header: %w", err)
	}

	return &s, nil
}

// Name returns the name of the segment as presented to Open.
func (s *segment) Name() string {
	return s.name
}

func (s *segment) Block() uint64 {
	return s.header.Block
}

// ReadAt reads the record at a given offset in the segment file
func (s *segment) ReadAt(_record encoding.BinaryUnmarshaler, _offset uint64) error {
	if s.closed {
		return ClosedErr
	}

	_blocks, err := s.blocks(_offset)
	if err != nil {
		return fmt.Errorf("can't read from segment file: %w", err)
	}
	buff := bytes.Buffer{}
	for _, bl := range _blocks {
		if _, err := buff.Write(bl.Payload); err != nil {
			return err
		}
	}
	if err := _record.UnmarshalBinary(buff.Bytes()); err != nil {
		return err
	}

	return nil
}

// WriteRecord writes the given record at the end of the segment
func (s *segment) Write(_record encoding.BinaryMarshaler) (uint64, error) {
	if s.closed {
		return 0, ClosedErr
	}

	offset, err := s.Size()
	if err != nil {
		return 0, err
	}

	raw, err := _record.MarshalBinary()
	if err != nil {
		return 0, err
	}

	//todo: impl

	if err := s.file.Sync(); err != nil {
		return 0, fmt.Errorf("can't sync file changes to disk: %w", err)
	}

	return offset, nil
}

// Truncate - removes all records whose offset is greater or equal to offset. the removal is permanently.
func (s *segment) Truncate(_offset uint64) error {
	if s.closed {
		return ClosedErr
	}

	offset, err := s.Size()
	if err != nil {
		return err
	}
	if offset <= _offset {
		return fmt.Errorf("can't truncate segment. offset to big")
	}

	if err := s.file.Truncate(int64(_offset)); err != nil {
		return fmt.Errorf("can't truncate file: %w", err)
	}

	return nil
}

// Size returns the byte size of the segment file
func (s *segment) Size() (uint64, error) {
	// can't use seek function since file is append mode - use os.Stat instead
	info, err := os.Stat(s.name)
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
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

func (s *segment) readHead() error {
	if s.file == nil {
		return fmt.Errorf("segment file is nil")
	}

	headBlock := make([]byte, blockMetadataLength)
	_, err := s.file.ReadAt(headBlock, 0)
	if err != nil {
		return err
	}
	if err := s.header.UnmarshalBinary(headBlock); err != nil {
		return err
	}
	return nil
}

func (s *segment) writeHead() error {
	if s.file == nil {
		return fmt.Errorf("segment file is nil")
	}
	raw, err := s.header.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = s.file.Write(raw)
	if err != nil {
		return err
	}
	return nil
}

func (s *segment) pageOffset(_offset uint64) uint64 {
	return _offset - (_offset % s.header.Page)
}

func (s *segment) page(_offset uint64) (page []byte, err error) {
	page = make([]byte, s.header.Page)
	_, err = s.file.ReadAt(page, int64(_offset))
	if err != nil {
		return nil, err
	}
	return page, err
}

func (s *segment) blocks(_offset uint64) (_blocks []block, err error) {
	po := s.pageOffset(_offset)
	pageData, err := s.page(po)
	if err != nil {
		return nil, err
	}
	bloff := _offset - po
	raw := pageData[bloff:]
	_block := block{}
	if err := _block.UnmarshalBinary(raw[:s.header.Block]); err != nil {
		return nil, err
	}
	raw = raw[s.header.Block:]
	if !_block.First {
		return nil, fmt.Errorf("can't find start of record")
	}
	_blocks = make([]block, 0, _block.Length+1)
	_blocks = append(_blocks, _block)
	if _block.Length > 0 {
		for i := uint64(1); i <= uint64(_block.Length); i++ {
			if len(raw) == 0 {
				// load new page
				raw, err = s.page(po + (i * s.header.Page))
				if err != nil {
					return nil, err
				}
			}
			if uint64(len(raw)) < s.header.Block {
				return nil, fmt.Errorf("invalid raw data. not enough bytes")
			}
			_block := block{}
			if err := _block.UnmarshalBinary(raw[:s.header.Block]); err != nil {
				return nil, err
			}
			_blocks = append(_blocks, _block)
			raw = raw[s.header.Block:]
		}
	}
	return _blocks, nil
}

func (s *segment) block(_offset uint64) (_block block, err error) {
	p, err := s.page(s.pageOffset(_offset))
	if err != nil {
		return block{}, err
	}
	bo := _offset - (_offset % s.header.Block)
	bb := p[bo : bo+s.header.Block]
	_block = block{}
	if err = _block.UnmarshalBinary(bb); err != nil {
		return block{}, err
	}
	return _block, err
}
