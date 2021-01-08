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
	// Key returns an unique identifier for the record
	Key() uint64
	// SeqNum returns the sequence number of the record
	SeqNum() uint64
	// SetSeqNum sets the sequence number to the record
	SetSeqNum(_seqNum uint64)
	// ForSegment returns the record as segment.Record
	ForSegment() segment.Record
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

func (r *record) SeqNum() uint64 {
	return r.sequenceNumber
}

func (r *record) SetSeqNum(_seqNum uint64) {
	r.sequenceNumber = _seqNum
}

func (r *record) Data() []byte {
	return r.data
}

func (r *record) Encode() ([]byte, error) {
	buf := bytes.Buffer{}
	rawSeqNumber := encodeSeqNum(r.sequenceNumber)
	buf.Write(rawSeqNumber)
	buf.Write(r.data)
	return buf.Bytes(), nil
}

func (r *record) Decode(_key uint64, _payload []byte) error {
	r.key = _key
	var err error
	r.sequenceNumber, err = decodeSeqNum(_payload[:headerSequenceNumberFieldLength])
	if err != nil {
		return err
	}
	r.data = _payload[headerSequenceNumberFieldLength:]
	return nil
}

func (r *record) ForSegment() segment.Record {
	return r
}

func encodeSeqNum(_seqNum uint64) []byte {
	rawSeqNumber := make([]byte, headerSequenceNumberFieldLength)
	binary.LittleEndian.PutUint64(rawSeqNumber, _seqNum)
	return rawSeqNumber
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
