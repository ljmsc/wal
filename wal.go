package wal

import (
	"io"
)

type Wal interface {
	// Name returns the name of the write ahead log
	Name() string
	// ReadAt reads the data of record with the given sequence number
	ReadAt(_w io.Writer, _seqNum uint64) error
	// Write writes the given record on disk and returns the new sequence number
	Write(_r io.Reader) (uint64, error)
	// Truncate dumps all records whose sequence number is greater or equal to offset
	Truncate(_seqNum uint64) error
	// Length returns the amount of records in the write ahead log
	Length() uint64
	// Start returns the first sequence number in the write ahead log
	Start() uint64
	// SeqNum returns the latest written sequence number in the write ahead log
	SeqNum() uint64
	// Close closes the write ahead log and all segment files
	Close() error
}

type wal struct {
	name     string
	split    int64
	size     int64
	seqNum   uint64
	segments []*segment
}

// Open opens or creates a write ahead log
func Open(_name string, _split int64, _size int64) (Wal, error) {
	w := wal{
		name:   _name,
		split:  _split,
		size:   _size,
		seqNum: 0,
	}

	if err := w.scan(); err != nil {
		return nil, err
	}

	return &w, nil
}

func (w *wal) scan() error {
	panic("implement me")
}

func (w *wal) seg() *segment {
	if len(w.segments) == 0 {
		// todo: create segment file
		// todo: add new segment file to
	}
	return w.segments[len(w.segments)-1]
}

// Name returns the name of the write ahead log
func (w *wal) Name() string {
	return w.name
}

// ReadAt reads the data of record with the given sequence number
func (w *wal) ReadAt(_w io.Writer, _seqNum uint64) error {
	panic("implement me")
}

// Write writes the given record on disk and returns the new sequence number
func (w *wal) Write(_r io.Reader) (uint64, error) {
	panic("implement me")
}

func (w *wal) Truncate(_seqNum uint64) error {
	panic("implement me")
}

func (w *wal) Length() uint64 {
	panic("implement me")
}

func (w *wal) Start() uint64 {
	panic("implement me")
}

func (w *wal) SeqNum() uint64 {
	return w.seqNum
}

func (w *wal) Close() error {
	panic("implement me")
}
