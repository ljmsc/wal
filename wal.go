package wal

import (
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

	ErrNoData        = fmt.Errorf("no data provided")
	ErrEntryNotFound = fmt.Errorf("entry not found")

	// internal errors

	errSegmentNotFound = fmt.Errorf("segment not found")
)

type Wal interface {
	// Name returns the name of the write ahead log
	Name() string
	// ReadAt reads the data of record with the given sequence number
	ReadAt(_entry Entry, _seqNum uint64) error
	// Write writes the given record on disk and returns the new sequence number
	Write(_entry Entry) (uint64, error)
	// Truncate dumps all records whose sequence number is greater or equal to offsetBy
	Truncate(_seqNum uint64) error
	// First returns the first sequence number in the write ahead log
	First() uint64
	// SeqNum returns the latest written sequence number in the write ahead log
	SeqNum() uint64
	// Close closes the write ahead log and all segment files
	Close() error
}

type wal struct {
	// name is the name of the write ahead log. all segment files have this as name prefix
	name string
	// split defines in how many chunks a single filesystem block should be divided
	split int64
	// size is the max size of blocks which should be written to a single segment file
	size int64
	// first is the first sequence number in the write ahead log
	first uint64
	// seqNum is the latest written sequence number in the write ahead log
	seqNum uint64
	// segments is a list of segments which belongs to the write ahead log
	segments []segpos
}

// Open opens or creates a write ahead log.
func Open(_name string, _segSplit int64, _segSize int64) (Wal, error) {
	w := wal{
		name:     _name,
		split:    _segSplit,
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
		seqNum, err := strconv.ParseUint(split[len(split)-1], 10, 64)
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

// ReadAt reads the data of record with the given sequence number
func (w *wal) ReadAt(_entry Entry, _seqNum uint64) error {
	segpos, err := w.segBy(_seqNum)
	if err != nil {
		if errors.Is(err, errSegmentNotFound) {
			return ErrEntryNotFound
		}
		return err
	}
	seg := segpos.segment
	offset, err := seg.offsetBy((_seqNum - segpos.seqNum) + 1)
	if err != nil {
		return err
	}

	_r := record{}
	if err := seg.readAt(&_r, offset); err != nil {
		return err
	}

	if err := _entry.Unmarshal(_r.payload); err != nil {
		return fmt.Errorf("can't unmarshal data: %w", err)
	}
	return nil
}

// Write writes the given record on disk and returns the new sequence number
func (w *wal) Write(_entry Entry) (uint64, error) {
	raw, err := _entry.Marshal()
	if err != nil {
		return 0, fmt.Errorf("can't marshal data: %w", err)
	}

	// check is data is available
	if len(raw) == 0 {
		return 0, ErrNoData
	}

	_r := record{
		payload: raw,
	}
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
		err = w.segAdd()
		if err != nil {
			return 0, fmt.Errorf("no segment available: %w", err)
		}
	}

	w.seqNum++
	return w.seqNum, nil
}

func (w *wal) Truncate(_seqNum uint64) error {
	for i := len(w.segments) - 1; i >= 0; i-- {
		segpos := w.segments[i]
		if segpos.seqNum >= _seqNum {
			w.segments = w.segments[:len(w.segments)-1]
			_ = segpos.segment.close()
			err := os.Remove(segpos.segment.file.Name())
			if err != nil {
				return fmt.Errorf("can't remove segment file: %w", err)
			}
			continue
		}
		offset, err := segpos.segment.offsetBy(_seqNum - segpos.seqNum)
		if err != nil {
			return err
		}
		return segpos.segment.truncate(offset)
	}
	return nil
}

func (w *wal) First() uint64 {
	return w.first
}

func (w *wal) SeqNum() uint64 {
	return w.seqNum
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
