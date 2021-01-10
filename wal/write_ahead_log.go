package wal

import (
	"fmt"

	"github.com/ljmsc/wal/segment"

	"github.com/ljmsc/wal/chain"
)

type Wal interface {
	// Name returns the name of the write ahead log
	Name() string
	// ReadAt reads the data of record with the given sequence number
	ReadAt(_record Record, _seqNum uint64) error
	// ReadKey reads the latest record with the given key
	ReadKey(_record Record, _key uint64) error
	// ReadKey reads the record with the given key and version
	ReadVersion(_record Record, _key uint64, _version uint64) error
	// Write writes the given record on disc and returns the sequence number
	Write(_record Record) (uint64, error)
	CompareAndWrite(_record Record, _version uint64) (uint64, error)
	// Truncate dumps all records whose sequence number is greater or equal to offset
	Truncate(_seqNum uint64) error
	// Length returns the amount of records in the write ahead log
	Length() uint64
	Size() (int64, error)
	FirstSeqNum() uint64
	SeqNum() uint64
	KeySeqNums() map[uint64][]uint64
	KeyVersionSeqNum() map[uint64]map[uint64]uint64
	IsClosed() bool
	Close() error
}

// Version returns the latest version number of the key.
// returns 0 when key does not exist
func Version(_wal Wal, _key uint64) uint64 {
	keyVersions := _wal.KeyVersionSeqNum()
	if _, ok := keyVersions[_key]; !ok {
		return 0
	}
	latestVersion := uint64(0)
	for version := range keyVersions[_key] {
		if version > latestVersion {
			latestVersion = version
		}
	}
	return latestVersion
}

// Stream returns a channel which receives all records for the given sequence number range.
// if the wal is closed the function returns nil
// if _startSeqNum is zero it starts with the first possible record
// if _endSeqNum is zero it sends all possible records in the chain
func Stream(_wal Wal, _startSeqNum uint64, _endSeqNum uint64) <-chan RecordEnvelope {
	if _wal.IsClosed() {
		return nil
	}
	stream := make(chan RecordEnvelope)
	if _startSeqNum == 0 {
		_startSeqNum = _wal.FirstSeqNum()
		_endSeqNum = _wal.SeqNum()
	}
	go func() {
		defer close(stream)
		for seqNum := _startSeqNum; seqNum <= _endSeqNum; seqNum++ {
			r := record{}
			if err := _wal.ReadAt(&r, seqNum); err != nil {
				stream <- RecordEnvelope{
					SeqNum: seqNum,
					Err:    err,
				}
				return
			}
			stream <- RecordEnvelope{
				SeqNum: seqNum,
				Record: &r,
			}
		}
	}()
	return stream
}

// wal is a write ahead log
type wal struct {
	// chain is the file storage for the write ahead log
	chain chain.Chain
	// keyVersionSeqNum - for each key the versions and there corresponding sequence number
	keyVersionSeqNum map[uint64]map[uint64]uint64
	// closed
	closed bool
}

// Open opens a new or existing wal
func Open(name string, _padding uint64, maxFileSize uint64, handler func(_wal Wal, _envelope HeaderEnvelope) error) (Wal, error) {

	// add version field length to padding to ensure version is always read
	_padding += headerVersionFieldLength

	w := wal{
		keyVersionSeqNum: map[uint64]map[uint64]uint64{},
		closed:           false,
	}
	c, err := chain.Open(name, _padding, maxFileSize, func(_chain chain.Chain, _envelope chain.HeaderEnvelope) error {
		version, err := decodeVersion(_envelope.PaddingData[:headerVersionFieldLength])
		if err != nil {
			return err
		}
		w.consider(_envelope.Header.Key, _envelope.SeqNum, version)
		if handler != nil {
			h := HeaderEnvelope{
				Header:      _envelope.Header,
				SeqNum:      _envelope.SeqNum,
				Version:     version,
				PaddingData: _envelope.PaddingData[headerVersionFieldLength:],
			}
			if err := handler(&w, h); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	w.chain = c

	return &w, nil
}

func (w *wal) Name() string {
	return w.chain.Name()
}

func (w *wal) ReadAt(_record Record, _seqNum uint64) error {
	if w.IsClosed() {
		return ClosedErr
	}
	return w.chain.ReadAt(_record, _seqNum)
}

func (w *wal) ReadKey(_record Record, _key uint64) error {
	if w.IsClosed() {
		return ClosedErr
	}
	return w.chain.ReadKey(_record, _key)
}

func (w *wal) ReadVersion(_record Record, _key uint64, _version uint64) error {
	if w.IsClosed() {
		return ClosedErr
	}
	if _, ok := w.keyVersionSeqNum[_key]; !ok {
		return segment.RecordNotFoundErr
	}
	if _, ok := w.keyVersionSeqNum[_version]; !ok {
		return RecordVersionNotFoundErr
	}
	seqNum := w.keyVersionSeqNum[_key][_version]
	return w.chain.ReadAt(_record, seqNum)
}

func (w *wal) Write(_record Record) (uint64, error) {
	if w.IsClosed() {
		return 0, ClosedErr
	}

	version := Version(w, _record.Key()) + 1
	if _, ok := w.keyVersionSeqNum[_record.Key()][version]; ok {
		return 0, fmt.Errorf("version already exists for this key")
	}
	_record.SetVersion(version)
	seqNum, err := w.chain.Write(_record)
	if err != nil {
		return 0, err
	}
	w.consider(_record.Key(), seqNum, version)
	return seqNum, nil
}

// CompareAndWrite checks the given version with the latest entry in the wal.
// if version > latest version => invalid version number
// if version < latest version => version is to old, there is a newer version in the wal
// if version == latest version => the given entry is written to the log
func (w *wal) CompareAndWrite(_record Record, _version uint64) (uint64, error) {
	if w.IsClosed() {
		return 0, ClosedErr
	}

	version := Version(w, _record.Key())
	if _version > version {
		return 0, fmt.Errorf("invalid version")
	} else if _version < version {
		return 0, RecordOutdatedErr
	}
	return w.Write(_record)
}

func (w *wal) Truncate(_seqNum uint64) error {
	if w.IsClosed() {
		return ClosedErr
	}

	for key, versionSeqNum := range w.keyVersionSeqNum {
		for version, seqNum := range versionSeqNum {
			if seqNum >= _seqNum {
				delete(w.keyVersionSeqNum[key], version)
			}
		}
	}

	return w.chain.Truncate(_seqNum)
}

func (w *wal) Length() uint64 {
	return w.chain.Length()
}

// Size returns the size of the log in bytes
func (w *wal) Size() (int64, error) {
	return w.chain.Size()
}

func (w *wal) FirstSeqNum() uint64 {
	return w.chain.FirstSeqNum()
}

func (w *wal) SeqNum() uint64 {
	return w.chain.SeqNum()
}

func (w *wal) KeySeqNums() map[uint64][]uint64 {
	return w.chain.KeySeqNums()
}

func (w *wal) KeyVersionSeqNum() map[uint64]map[uint64]uint64 {
	return w.keyVersionSeqNum
}

// IsClosed returns true if the log is already closed
func (w *wal) IsClosed() bool {
	return w.closed
}

// Close closes the log
func (w *wal) Close() error {
	w.closed = true
	return w.chain.Close()
}

func (w *wal) consider(_key uint64, _seqNum uint64, _version uint64) {
	if _, ok := w.keyVersionSeqNum[_key]; !ok {
		w.keyVersionSeqNum[_key] = make(map[uint64]uint64)
	}
	w.keyVersionSeqNum[_key][_version] = _seqNum
}
