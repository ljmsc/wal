package wal

import (
	"bytes"
	"fmt"
)

const (
	recordSizeLength     = 8
	recordChecksumLength = 8
	recordMetadataLength = recordSizeLength + recordChecksumLength
)

var (
	errInvalidChecksum = fmt.Errorf("invalid checksum")
)

type record struct {
	// size is the total length of the payload
	size uint64
	// checksum is a checksum of the payload
	checksum uint64
	// payload is the payload of the block
	payload []byte
}

func (r *record) blockC(_blockSize int64) int64 {
	c := (int64(len(r.payload)) + recordMetadataLength) / _blockSize
	if int64(len(r.payload))+recordMetadataLength%_blockSize > 0 {
		c++
	}
	return c
}

func (r *record) validMeta() bool {
	if r.checksum == 0 || r.size == 0 {
		return false
	}
	return true
}

func (r *record) marshal() (data []byte, err error) {
	buff := bytes.Buffer{}
	// size
	_, err = buff.Write(encodeUint64(uint64(len(r.payload))))
	if err != nil {
		return nil, fmt.Errorf("can't write payload size to buffer: %w", err)
	}
	// checksum
	_, err = buff.Write(encodeUint64(sum(r.payload)))
	if err != nil {
		return nil, fmt.Errorf("can't write payload checksum to buffer: %w", err)
	}
	// payload
	_, err = buff.Write(r.payload)
	if err != nil {
		return nil, fmt.Errorf("can't write payload to buffer: %w", err)
	}

	return buff.Bytes(), nil
}

func (r *record) unmarshal(_data []byte) error {
	if len(_data) < recordMetadataLength {
		return fmt.Errorf("not enough bytes")
	}

	// size
	r.size = decodeUint64(_data[:recordSizeLength])
	_data = _data[recordSizeLength:]

	// checksum
	r.checksum = decodeUint64(_data[:recordChecksumLength])
	_data = _data[recordChecksumLength:]

	// payload
	size := r.size
	if uint64(len(_data)) < r.size {
		size = uint64(len(_data))
	}
	r.payload = _data[:size]

	if !check(r.payload, r.checksum) {
		return errInvalidChecksum
	}

	return nil
}

func (r *record) appendPayload(_data []byte) error {
	if !r.validMeta() {
		return fmt.Errorf("header not set")
	}

	r.payload = append(r.payload, _data...)

	if uint64(len(r.payload)) != r.size {
		return fmt.Errorf("payload size missmatch")
	}

	if !check(r.payload, r.checksum) {
		return errInvalidChecksum
	}

	return nil
}
