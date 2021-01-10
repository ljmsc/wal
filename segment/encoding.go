package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	headerPayloadSizeFieldLength = 8
	headerLength                 = headerKeyHashFieldLength + headerPayloadSizeFieldLength
)

func DecodeUint64(_raw []byte) uint64 {
	return binary.LittleEndian.Uint64(_raw)
}

func EncodeUint64(value uint64) []byte {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, value)
	return raw
}

type Header struct {
	PayloadSize uint64
	Key         uint64
}

func decodeHeader(_header *Header, _raw []byte) error {
	if len(_raw) < headerPayloadSizeFieldLength {
		return fmt.Errorf("not enough bytes for decoding")
	}
	var err error
	_header.PayloadSize = DecodeUint64(_raw[:headerPayloadSizeFieldLength])
	_header.Key, err = decodeKey(_raw[headerPayloadSizeFieldLength:headerLength])
	if err != nil {
		return err
	}
	return nil
}

func encodeHeader(_header Header) ([]byte, error) {
	if _header.PayloadSize == 0 {
		return nil, fmt.Errorf("Header PayloadSize is zero")
	}

	buff := bytes.Buffer{}
	buff.Write(EncodeUint64(_header.PayloadSize))
	rawKey := EncodeUint64(_header.Key)
	buff.Write(rawKey)

	return buff.Bytes(), nil
}

// encode encodes the given record into a byte slice for including the Header information.
func encode(_record Record) ([]byte, error) {
	buff := bytes.Buffer{}

	rawRecord, err := _record.Encode()
	if err != nil {
		return nil, err
	}

	// add header information
	buff.Write(EncodeUint64(uint64(len(rawRecord))))
	buff.Write(rawRecord)

	return buff.Bytes(), nil
}

// decode decodes the byte slices into the given record object.
func decode(_record Record, _raw []byte) error {
	header := Header{}
	if err := decodeHeader(&header, _raw[:headerLength]); err != nil {
		return err
	}
	if header.PayloadSize != uint64(len(_raw[headerPayloadSizeFieldLength:])) {
		return fmt.Errorf("not enough bytes to decode record")
	}

	if err := _record.Decode(_raw[headerPayloadSizeFieldLength:]); err != nil {
		return err
	}

	return nil
}
