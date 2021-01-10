package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ljmsc/wal/segment"

	"github.com/ljmsc/wal/chain"
)

const (
	headerVersionFieldLength = 8
)

type Record interface {
	chain.Record
	// Version returns the version of the record
	Version() uint64
	// SetVersion sets the version on the record. this is used only internal
	SetVersion(_version uint64)
	// Data returns the data of the record
	Data() []byte
}

func EncodeFields(_record Record) ([]byte, error) {
	raw, err := chain.EncodeFields(_record)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(raw)
	buf.Write(segment.EncodeUint64(_record.Version()))
	return buf.Bytes(), nil
}

func DecodeFields(_record Record, _raw []byte) ([]byte, error) {
	raw, err := chain.DecodeFields(_record, _raw)
	if err != nil {
		return nil, err
	}

	version, err := decodeVersion(raw[:headerVersionFieldLength])
	if err != nil {
		return nil, err
	}
	_record.SetVersion(version)
	return raw[headerVersionFieldLength:], nil
}

// record is a entry in the write ahead log
type record struct {
	key     uint64
	seqNum  uint64
	version uint64
	data    []byte
}

func CreateRecord(_key uint64, _data []byte) Record {
	return &record{
		key:  _key,
		data: _data,
	}
}

func (r *record) Key() uint64 {
	return r.key
}

func (r *record) SetKey(_key uint64) {
	r.key = _key
}

func (r *record) SeqNum() uint64 {
	return r.seqNum
}

func (r *record) SetSeqNum(_seqNum uint64) {
	r.seqNum = _seqNum
}

func (r *record) Version() uint64 {
	return r.version
}

func (r *record) SetVersion(_version uint64) {
	r.version = _version
}

func (r *record) Encode() ([]byte, error) {
	raw, err := EncodeFields(r)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(raw)
	buf.Write(r.data)
	return buf.Bytes(), nil
}

func (r *record) Decode(_raw []byte) error {
	data, err := DecodeFields(r, _raw)
	if err != nil {
		return err
	}
	r.data = data
	return nil
}

func (r *record) Data() []byte {
	return r.data
}

func decodeVersion(_rawVersion []byte) (uint64, error) {
	if len(_rawVersion) != headerVersionFieldLength {
		return 0, fmt.Errorf("version decoding needs exactly %d bytes", headerVersionFieldLength)
	}
	version := binary.LittleEndian.Uint64(_rawVersion)
	if version == 0 {
		return 0, fmt.Errorf("version is zero")
	}
	return version, nil
}
