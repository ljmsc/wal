package wal

import (
	"encoding"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	// public errors

	ErrNoData           = fmt.Errorf("no data provided")
	ErrEntryNotFound    = fmt.Errorf("entry not found")
	ErrSeqNumOutOfRange = fmt.Errorf("sequence number is out of range")

	// internal errors

	errSegmentNotFound = fmt.Errorf("segment not found")
)

type Wal interface {
	// Name returns the name of the write ahead log
	Name() string
	// Read reads the data of the entry with the given sequence number and returns it
	Read(_seqNum uint64) ([]byte, error)
	// ReadAt reads the data of the entry with the given sequence number to _entry
	// Deprecated
	ReadAt(_entry Entry, _seqNum uint64) error
	// ReadTo reads the data of the entry with the given sequence number to _entry
	ReadTo(_entry encoding.BinaryUnmarshaler, _seqNum uint64) error
	// ReadFrom reads the payload of _n entries starting from _seqNum
	ReadFrom(_seqNum uint64, _n int64) (<-chan Envelope, error)
	// Write writes the given data on disk and returns the new sequence number
	Write(_data []byte) (uint64, error)
	// WriteFrom extracts data from the given object and writes it on disk. it returns the new sequence number
	WriteFrom(r encoding.BinaryMarshaler) (uint64, error)
	// Truncate dumps all records whose sequence number is greater or equal to offsetByPos
	Truncate(_seqNum uint64) error
	// Sync flushes changes to persistent storage and returns the latest safe sequence number
	Sync() (uint64, error)
	// First returns the first sequence number in the write ahead log
	First() uint64
	// SeqNum returns the latest written sequence number in the write ahead log
	SeqNum() uint64
	// Safe returns the latest safe sequence number
	Safe() uint64
	// Close closes the write ahead log and all segment files
	Close() error
}

type wal struct {
	// name is the name of the write ahead log. all segment files have this as name prefix
	name string
	// split defines in how many chunks a single filesystem block should be divided
	split int64
	// size is the max amount of blocks which should be written to a single segment file
	size int64
	// first is the first sequence number in the write ahead log
	first uint64
	// seqNum is the latest written sequence number in the write ahead log
	seqNum uint64
	// segments is a list of segments which belongs to the write ahead log
	segments []segpos
}

// Open opens or creates a write ahead log.
// _segBlockN is the amount of records which can be written to a single filesystem page
// _segSize is the amount of records which can be written to the segment file
func Open(_name string, _segBlockN int64, _segSize int64) (Wal, error) {
	w := wal{
		name:     _name,
		split:    _segBlockN,
		size:     _segSize,
		first:    1,
		seqNum:   0,
		segments: []segpos{},
	}

	if err := w.scan(); err != nil {
		return nil, err
	}

	return &w, nil
}

func (w *wal) scan() error {
	dir := filepath.Dir(w.name)
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("can't read directory: %w", err)
	}

	first := true
	for _, info := range fileInfos {
		segName := dir + "/" + info.Name()
		if !strings.HasPrefix(segName, w.name) {
			continue
		}
		if !strings.Contains(segName, "_") {
			return fmt.Errorf("invalid segment name: %s", segName)
		}
		split := strings.Split(segName, "_")
		// parse the last part of the segment name as sequence number
		seqNum, err := strconv.ParseUint(split[len(split)-1], 10, 64) // nolint
		if err != nil {
			return fmt.Errorf("can't parse seq num from filename: %w", err)
		}

		seg, err := openSegment(segName, w.split, w.size)
		if err != nil {
			return err
		}

		w.segments = append(w.segments, segpos{
			segment: seg,
			seqNum:  seqNum,
		})
		if first {
			w.first = seqNum
			first = false
		}
	}

	if len(w.segments) > 0 {
		segpos := w.segments[len(w.segments)-1]
		w.seqNum = segpos.seqNum + uint64(len(segpos.segment.offsets)-1)
	}

	return nil
}

func (w *wal) segAdd() error {
	firstSegNum := w.seqNum + 1
	segmentName := fmt.Sprintf("%s_%d", w.name, firstSegNum)
	ns, err := openSegment(segmentName, w.split, w.size)
	if err != nil {
		return err
	}
	w.segments = append(w.segments, segpos{
		segment: ns,
		seqNum:  firstSegNum,
	})
	return nil
}

func (w *wal) segBy(seqNum uint64) (segpos, error) {
	if seqNum == 0 {
		return segpos{}, errSegmentNotFound
	}

	for i := len(w.segments) - 1; i >= 0; i-- {
		sp := w.segments[i]
		if sp.seqNum <= seqNum {
			return sp, nil
		}
	}

	return segpos{}, errSegmentNotFound
}

func (w *wal) seg() (*segment, error) {
	if len(w.segments) == 0 {
		err := w.segAdd()
		if err != nil {
			return nil, err
		}
	}
	return w.segments[len(w.segments)-1].segment, nil
}

// Name returns the name of the write ahead log
func (w *wal) Name() string {
	return w.name
}

// Read reads the data of the entry with the given sequence number and returns it
func (w *wal) Read(_seqNum uint64) ([]byte, error) {
	segpos, err := w.segBy(_seqNum)
	if err != nil {
		if errors.Is(err, errSegmentNotFound) {
			return nil, ErrEntryNotFound
		}
		return nil, err
	}
	offset, err := segpos.offsetBySeqNum(_seqNum)
	if err != nil {
		return nil, err
	}

	_r := record{}
	if err := segpos.segment.readAt(&_r, offset); err != nil {
		return nil, err
	}

	return _r.payload, nil
}

// ReadAt reads the data of the entry with the given sequence number to _entry
// Deprecated
func (w *wal) ReadAt(_entry Entry, _seqNum uint64) error {
	payload, err := w.Read(_seqNum)
	if err != nil {
		return err
	}

	if err := _entry.Unmarshal(payload); err != nil {
		return fmt.Errorf("can't unmarshal data: %w", err)
	}
	return nil
}

// ReadTo reads the data of the entry with the given sequence number to _entry
func (w *wal) ReadTo(_entry encoding.BinaryUnmarshaler, _seqNum uint64) error {
	payload, err := w.Read(_seqNum)
	if err != nil {
		return err
	}

	if err := _entry.UnmarshalBinary(payload); err != nil {
		return fmt.Errorf("can't unmarshal data: %w", err)
	}
	return nil
}

// ReadFrom reads the payload of _n entries starting from _seqNum
func (w *wal) ReadFrom(_seqNum uint64, _n int64) (<-chan Envelope, error) {
	out := make(chan Envelope)

	if _seqNum < w.first || _seqNum > w.seqNum {
		return nil, ErrSeqNumOutOfRange
	}

	go func() {
		defer close(out)
		currSeqNum := _seqNum
		// sendN counts the elements which are already send via the out channel
		var sendN int64
		for {
			// return if the next seqNum is larger than the last one written
			if currSeqNum > w.seqNum {
				return
			}
			// return if _n elements are send via the channel
			if sendN >= _n {
				return
			}
			segpos, err := w.segBy(currSeqNum)
			if err != nil {
				if errors.Is(err, errSegmentNotFound) {
					err = ErrEntryNotFound
				}
				out <- Envelope{Err: err}
				return
			}

			offset, err := segpos.offsetBySeqNum(currSeqNum)
			if err != nil {
				out <- Envelope{Err: err}
				return
			}
			env, err := segpos.segment.readFrom(offset, _n)
			if err != nil {
				out <- Envelope{Err: err}
				return
			}
			for envelope := range env {
				c := int64(currSeqNum - _seqNum)
				if c >= _n || currSeqNum > w.seqNum {
					return
				}
				if envelope.err != nil {
					out <- Envelope{Err: envelope.err}
					return
				}
				out <- Envelope{
					SeqNum:  currSeqNum,
					Payload: envelope.record.payload,
				}
				sendN++
				currSeqNum++
			}
		}
	}()

	return out, nil
}

// Write writes the given record on disk and returns the new sequence number
func (w *wal) Write(_data []byte) (uint64, error) {
	// check is data is available
	if len(_data) == 0 {
		return 0, ErrNoData
	}

	_r := record{payload: _data}

	for {
		seg, err := w.seg()
		if err != nil {
			return 0, fmt.Errorf("no segment available: %w", err)
		}

		// try to write record to segment file
		_, err = seg.write(_r)
		if err == nil {
			break
		}
		if !errors.Is(err, errMaxSize) {
			return 0, fmt.Errorf("couldn't write entry: %w", err)
		}
		// sync open changes to disk before opening a new segment
		if err := seg.sync(); err != nil {
			return 0, err
		}
		err = w.segAdd()
		if err != nil {
			return 0, fmt.Errorf("no segment available: %w", err)
		}
	}

	w.seqNum++
	return w.seqNum, nil
}

// WriteFrom extracts data from the given object and writes it on disk. it returns the new sequence number
func (w *wal) WriteFrom(r encoding.BinaryMarshaler) (uint64, error) {
	data, err := r.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("can't marshal data: %w", err)
	}

	return w.Write(data)
}

func (w *wal) Truncate(_seqNum uint64) error {
	for i := len(w.segments) - 1; i >= 0; i-- {
		segpos := w.segments[i]
		if segpos.seqNum < _seqNum {
			offset, err := segpos.offsetBySeqNum(_seqNum)
			if err != nil {
				return err
			}
			return segpos.segment.truncate(offset)
		}

		// remove segment reference
		w.segments = w.segments[:len(w.segments)-1]
		_ = segpos.segment.close()
		// delete the segment since all elements will be truncated
		err := os.Remove(segpos.segment.file.Name())
		if err != nil {
			return fmt.Errorf("can't remove segment file: %w", err)
		}
	}
	w.seqNum = _seqNum - 1
	return nil
}

func (w *wal) Sync() (uint64, error) {
	seg, err := w.seg()
	if err != nil {
		return 0, err
	}
	if err := seg.sync(); err != nil {
		return 0, err
	}

	return w.seqNum, nil
}

func (w *wal) First() uint64 {
	return w.first
}

func (w *wal) SeqNum() uint64 {
	return w.seqNum
}

func (w *wal) Safe() uint64 {
	if len(w.segments) == 0 {
		return 0
	}

	segpos := w.segments[len(w.segments)-1]
	seg := segpos.segment
	for i, offset := range seg.offsets {
		if offset != seg.safe {
			continue
		}
		return segpos.seqNum + uint64(i)
	}
	return segpos.seqNum - 1
}

func (w *wal) Close() error {
	var gerr error
	for _, seg := range w.segments {
		err := seg.segment.close()
		if err != nil {
			gerr = err
		}
	}
	return gerr
}
