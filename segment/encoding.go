package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// [Record [Record Header [Key Hash 8][Data Size 8]][Record Data ~]]...

const (
	headerKeyHashFieldLength     = 8
	headerPayloadSizeFieldLength = 8
	headerLength                 = headerKeyHashFieldLength + headerPayloadSizeFieldLength
)

type Header struct {
	Key          uint64
	PayloadSize  uint64
	PaddingBytes []byte
}

func decodeHeader(_header *Header, _raw []byte) error {
	if len(_raw) < headerLength {
		return fmt.Errorf("not enough bytes for decoding")
	}

	_header.Key = binary.LittleEndian.Uint64(_raw[:headerKeyHashFieldLength])
	if _header.Key == 0 {
		return fmt.Errorf("key hash is zero")
	}
	_header.PayloadSize = binary.LittleEndian.Uint64(_raw[headerKeyHashFieldLength:headerLength])

	return nil
}

func encodeHeader(_header Header) ([]byte, error) {
	if _header.Key == 0 {
		return nil, fmt.Errorf("Header Key is zero")
	}

	if _header.PayloadSize == 0 {
		return nil, fmt.Errorf("Header PayloadSize is zero")
	}

	buff := bytes.Buffer{}
	rawKeyHash := make([]byte, headerKeyHashFieldLength)
	binary.LittleEndian.PutUint64(rawKeyHash, _header.Key)
	buff.Write(rawKeyHash)

	rawPayloadSize := make([]byte, headerPayloadSizeFieldLength)
	binary.LittleEndian.PutUint64(rawPayloadSize, _header.PayloadSize)
	buff.Write(rawPayloadSize)

	return buff.Bytes(), nil
}

// encode encodes the given record into a byte slice for including the Header information.
func encode(_record Record) ([]byte, error) {
	buff := bytes.Buffer{}

	payload, err := _record.Encode()
	if err != nil {
		return nil, fmt.Errorf("can't encode record payload")
	}

	header := Header{
		Key:         _record.Key(),
		PayloadSize: uint64(len(payload)),
	}

	rawHeader, err := encodeHeader(header)
	if err != nil {
		return nil, err
	}

	n, err := buff.Write(rawHeader)
	if err != nil {
		return nil, fmt.Errorf("can't encode record Header: %w", err)
	}

	if n != len(rawHeader) {
		return nil, fmt.Errorf("can't encode record Header")
	}

	buff.Write(payload)

	return buff.Bytes(), nil
}

// decode decodes the byte slices into the given record object including the Header information.
func decode(_record Record, _raw []byte) error {
	header := Header{}
	if err := decodeHeader(&header, _raw[:headerLength]); err != nil {
		return err
	}

	if err := _record.Decode(header.Key, _raw[headerLength:]); err != nil {
		return fmt.Errorf("can't decode record payload")
	}

	return nil
}

// decodeWithPadding decodes the byte slice into the given Header object
func decodeWithPadding(_header *Header, _raw []byte) error {
	if err := decodeHeader(_header, _raw[:headerLength]); err != nil {
		return err
	}

	_header.PaddingBytes = _raw[headerLength:]

	return nil
}
