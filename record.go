package wal

import (
	"bytes"
	"fmt"
)

const (
	recordSeqNumLength   = 8
	recordSizeLength     = 8
	recordChecksumLength = 8
	recordMetadataLength = recordSeqNumLength + recordSizeLength + recordChecksumLength
)

type record struct {
	// SeqNum is the sequence number of the record in the write ahead log
	seqNum uint64
	// size is the total length of the payload
	size uint64
	// checksum is a checksum of the payload
	checksum uint64
	// Payload is the payload of the block
	payload []byte
}

func (r *record) marshal() (data []byte, err error) {
	buff := bytes.Buffer{}

	_, err = buff.Write(encodeUint64(r.seqNum))
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(encodeUint64(uint64(len(r.payload))))
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(encodeUint64(sum(r.payload)))
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(r.payload)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (r *record) unmarshalMetadata(_data []byte) error {
	if len(_data) < recordMetadataLength {
		return fmt.Errorf("not enough bytes")
	}

	r.seqNum = decodeUint64(_data[:recordSeqNumLength])
	_data = _data[recordSeqNumLength:]

	r.size = decodeUint64(_data[:recordSizeLength])
	_data = _data[recordSizeLength:]

	r.checksum = decodeUint64(_data[:recordChecksumLength])
	_data = _data[recordChecksumLength:]

	return nil
}

func (r *record) unmarshalPayload(data []byte) error {
	if r.seqNum == 0 || r.checksum == 0 || r.size == 0 {
		return fmt.Errorf("header not set")
	}
	if uint64(len(data)) < r.size {
		return fmt.Errorf("not enough bytes")
	}

	r.payload = data[:r.size]

	if !check(r.payload, r.checksum) {
		return invalidChecksumErr
	}

	return nil
}
