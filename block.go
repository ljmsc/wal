package wal

import (
	"bytes"
	"fmt"
	"hash/fnv"
)

const (
	blockFlagLength     = 1
	blockFollowLength   = 4
	blockSizeLength     = 8
	blockChecksumLength = 8
	blockMetadataLength = blockFlagLength + blockFollowLength + blockSizeLength + blockChecksumLength
)

var (
	invalidChecksumErr = fmt.Errorf("invalid checksum")
)

// block contains the information of a block in the segment
type block struct {
	// First defines if this block is the first block of a record
	First bool
	// Follow defines how many blocks are following to build the fill record
	Follow uint32
	// size is the total length of the payload
	size uint64
	// checksum is a checksum of the payload
	checksum uint64
	// Payload is the payload of the block
	Payload []byte
}

func (b *block) marshal() (data []byte, err error) {
	buff := bytes.Buffer{}
	var flags byte
	flags |= (1 << 0)
	_, err = buff.Write([]byte{flags})
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(encodeUint32(b.Follow))
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(encodeUint64(uint64(len(b.Payload))))
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(encodeUint64(sum(b.Payload)))
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(b.Payload)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (b *block) unmarshal(data []byte) error {
	if len(data) < blockMetadataLength {
		return fmt.Errorf("not enough bytes")
	}
	flags := data[0]
	b.First = (flags & (1 << 0)) != 0
	data = data[blockFlagLength:]

	b.Follow = decodeUint32(data[:blockFollowLength])
	data = data[blockFollowLength:]

	size := decodeUint64(data[:blockSizeLength])
	data = data[blockSizeLength:]

	checksum := decodeUint64(data[:blockChecksumLength])
	data = data[blockChecksumLength:]

	b.Payload = data[:size]

	if !check(b.Payload, checksum) {
		return invalidChecksumErr
	}

	return nil
}

func fragment(_data []byte, _size int64) ([]block, error) {
	bn := int64(len(_data)) / _size
	if int64(len(_data))%_size != 0 {
		bn += 1
	}
	_blocks := make([]block, 0, bn)
	for i := int64(0); i < bn; i++ {
		s := _size
		if int64(len(_data)) < s {
			s = int64(len(_data))
		}
		b := block{}
		if err := b.unmarshal(_data[:s]); err != nil {
			return nil, err
		}
		_blocks = append(_blocks, b)
		_data = _data[s:]
	}
	if len(_data) != 0 {
		return nil, fmt.Errorf("still data left")
	}

	return _blocks, nil
}

func deFragment(_blocks []block, _size int64) ([]byte, error) {
	buff := bytes.Buffer{}
	for _, b := range _blocks {
		_raw := make([]byte, _size)
		_rawBlock, err := b.marshal()
		if err != nil {
			return []byte{}, err
		}
		copy(_raw, _rawBlock)
		buff.Write(_raw)
	}
	return buff.Bytes(), nil
}

func sum(_payload []byte) uint64 {
	h := fnv.New64a()
	// error can be ignored since fnv can't produce an error
	_, _ = h.Write(_payload)
	return h.Sum64()
}

func check(_payload []byte, _sum uint64) bool {
	return sum(_payload) == _sum
}
