package wal

import (
	"bytes"
	"fmt"
)

const (
	headerVersionLength = 1
	headerPageLength    = 8
	headerBlockLength   = 8
	headerSizeLength    = 8
	headerLength        = headerVersionLength + headerPageLength + headerBlockLength + headerSizeLength
)

type formatVersion uint8

// header contains the metadata information of the segment (25 byte)
type header struct {
	// Version is the version of the segment file format. (1 byte)
	Version formatVersion
	// Page is the size of the pages on disk. this value is defined by the filesystem (8 byte)
	Page int64
	// Block is the size of each record block on disk in bytes. (8 byte)
	Block int64
	// Size is the amount of records which can be written to the segment file (8 byte)
	Size int64
}

func (h *header) marshal() (_raw []byte, err error) {

	if h.Page%h.Block != 0 {
		return nil, fmt.Errorf("Block must be a divider of the os block sys")
	}

	buff := bytes.Buffer{}
	_, err = buff.Write([]byte{uint8(h.Version)})
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(encodeInt64(h.Page))
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(encodeInt64(h.Block))
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(encodeInt64(h.Size))
	if err != nil {
		return nil, err
	}
	_raw = buff.Bytes()

	return _raw, nil
}

func (h *header) unmarshal(_raw []byte) error {
	if len(_raw) < headerLength {
		return fmt.Errorf("not enough bytes")
	}

	h.Version = formatVersion(_raw[0])
	_raw = _raw[headerVersionLength:]
	h.Page = decodeInt64(_raw[:headerPageLength])
	_raw = _raw[headerPageLength:]
	h.Block = decodeInt64(_raw[:headerBlockLength])
	_raw = _raw[headerBlockLength:]
	h.Size = decodeInt64(_raw[:headerSizeLength])
	return nil
}
