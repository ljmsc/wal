package chain

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ljmsc/wal/segment"
)

const (
	headerSequenceNumberFieldLength = 8
)

type Record interface {
	segment.Record
	// SeqNum returns the sequence number of the record
	SeqNum() uint64
	// SetSeqNum sets the sequence number to the record. this is used only internal
	SetSeqNum(_seqNum uint64)
}

func EncodeFields(_record Record) ([]byte, error) {
	if _record.SeqNum() == 0 {
		return nil, fmt.Errorf("sequence number must be greater than zero")
	}
	raw, err := segment.EncodeFields(_record)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(raw)
	buf.Write(segment.EncodeUint64(_record.SeqNum()))
	return buf.Bytes(), nil
}

func DecodeFields(_record Record, _raw []byte) ([]byte, error) {
	raw, err := segment.DecodeFields(_record, _raw)
	if err != nil {
		return nil, err
	}

	seqNum, err := decodeSeqNum(raw[:headerSequenceNumberFieldLength])
	if err != nil {
		return nil, err
	}
	_record.SetSeqNum(seqNum)
	return raw[headerSequenceNumberFieldLength:], nil
}

// Record .
type record struct {
	key            uint64
	sequenceNumber uint64
	data           []byte
}

func (r *record) Key() uint64 {
	return r.key
}

func (r *record) SetKey(_key uint64) {
	r.key = _key
}

func (r *record) SeqNum() uint64 {
	return r.sequenceNumber
}

func (r *record) SetSeqNum(_seqNum uint64) {
	r.sequenceNumber = _seqNum
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

func decodeSeqNum(_rawSeqNum []byte) (uint64, error) {
	if len(_rawSeqNum) != headerSequenceNumberFieldLength {
		return 0, fmt.Errorf("sequence number decoding needs exactly %d bytes", headerSequenceNumberFieldLength)
	}
	seqNum := binary.LittleEndian.Uint64(_rawSeqNum)
	if seqNum == 0 {
		return 0, fmt.Errorf("sequence number is zero")
	}
	return seqNum, nil
}
