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
	errInvalidMetadata = fmt.Errorf("invalid metadata")
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

func (r *record) isMetaValid() bool {
	if r.checksum == 0 || r.size == 0 {
		return false
	}
	return true
}

func (r *record) isValid() bool {
	if !r.isMetaValid() {
		return false
	}

	if uint64(len(r.payload)) != r.size {
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

func (r *record) unmarshalMetadata(_data []byte) error {
	if len(_data) < recordMetadataLength {
		return fmt.Errorf("not enough bytes")
	}

	// size
	r.size = decodeUint64(_data[:recordSizeLength])
	_data = _data[recordSizeLength:]

	// checksum
	r.checksum = decodeUint64(_data[:recordChecksumLength])

	if !r.isMetaValid() {
		return errInvalidMetadata
	}

	return nil
}

func (r *record) unmarshalPayload(_data []byte) error {
	if !r.isMetaValid() {
		return errInvalidMetadata
	}

	if uint64(len(_data)) < r.size {
		return fmt.Errorf("not enough bytes")
	}

	r.payload = _data[:r.size]

	if !check(r.payload, r.checksum) {
		return errInvalidChecksum
	}

	return nil
}
