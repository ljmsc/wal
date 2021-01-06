package bucket

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
	SequenceNumber() uint64
	SegmentRecord() segment.Record
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

func (r *record) SequenceNumber() uint64 {
	return r.sequenceNumber
}

func (r *record) Data() []byte {
	return r.data
}

func (r *record) Encode() ([]byte, error) {
	buf := bytes.Buffer{}
	rawSeqNumber := make([]byte, headerSequenceNumberFieldLength)
	binary.LittleEndian.PutUint64(rawSeqNumber, r.sequenceNumber)
	buf.Write(rawSeqNumber)

	buf.Write(r.data)
	return buf.Bytes(), nil
}

func (r *record) Decode(_key uint64, _payload []byte) error {
	r.key = _key
	r.sequenceNumber = binary.LittleEndian.Uint64(_payload[:headerSequenceNumberFieldLength])
	if r.sequenceNumber == 0 {
		return fmt.Errorf("sequence number is zero")
	}
	r.data = _payload[headerSequenceNumberFieldLength:]
	return nil
}

func (r *record) SegmentRecord() segment.Record {
	return r
}
