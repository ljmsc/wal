package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	headerVersionLength  = 1
	headerPageLength     = 8
	headerBlockLength    = 8
	headerSizeLength     = 8
	headerMetadataLength = headerVersionLength + headerPageLength + headerBlockLength + headerSizeLength

	blockFlagLength     = 1
	blockLengthLength   = 4
	blockMetadataLength = blockFlagLength + blockLengthLength
)

type formatVersion uint8

const (
	V1 = 1
)

// header contains the metadata information of the segment (25 byte)
type header struct {
	// Version is the version of the segment file format. (1 byte)
	Version formatVersion
	// Page is the size of the pages on disk. this value is defined by the filesystem (8 byte)
	Page uint64
	// Block is the size of each record block on disk in bytes. (8 byte)
	Block uint64
	// Size is the amount of records which can be written to the segment file (8 byte)
	Size uint64
}

func (h *header) MarshalBinary() (data []byte, err error) {
	buff := bytes.Buffer{}
	_, err = buff.Write([]byte{uint8(h.Version)})
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(encodeUint64(h.Page))
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(encodeUint64(h.Block))
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(encodeUint64(h.Size))
	if err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return data, nil
}

func (h *header) UnmarshalBinary(data []byte) error {
	if len(data) < headerMetadataLength {
		return fmt.Errorf("not enough bytes")
	}
	h.Version = formatVersion(data[0])
	data = data[headerVersionLength:]
	h.Page = decodeUint64(data[:headerPageLength])
	data = data[headerPageLength:]
	h.Block = decodeUint64(data[:headerBlockLength])
	data = data[headerBlockLength:]
	h.Size = decodeUint64(data[:headerSizeLength])
	return nil
}

// block contains the information of a block in the segment
type block struct {
	// First defines if this block is the first block of a record
	First bool
	// Length defines how many blocks are following to build the fill record
	Length uint32
	// Payload is the payload of the block
	Payload []byte
}

func (b *block) MarshalBinary() (data []byte, err error) {
	buff := bytes.Buffer{}
	var flags byte
	flags |= (1 << 0)
	_, err = buff.Write([]byte{flags})
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(encodeUint32(b.Length))
	if err != nil {
		return nil, err
	}
	_, err = buff.Write(b.Payload)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (b *block) UnmarshalBinary(data []byte) error {
	if len(data) < blockMetadataLength {
		return fmt.Errorf("not enough bytes")
	}
	flags := data[0]
	b.First = (flags & (1 << 0)) != 0
	data = data[blockFlagLength:]
	b.Length = decodeUint32(data[:blockLengthLength])
	data = data[blockLengthLength:]
	b.Payload = data
	return nil
}

func decodeUint32(_raw []byte) uint32 {
	return binary.LittleEndian.Uint32(_raw)
}

func decodeUint64(_raw []byte) uint64 {
	return binary.LittleEndian.Uint64(_raw)
}

func encodeUint64(value uint64) []byte {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, value)
	return raw
}

func encodeUint32(value uint32) []byte {
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, value)
	return raw
}
