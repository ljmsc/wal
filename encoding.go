package wal

import "encoding/binary"

func decodeUint32(_raw []byte) uint32 {
	return binary.LittleEndian.Uint32(_raw)
}

func decodeInt64(_raw []byte) int64 {
	return int64(binary.LittleEndian.Uint64(_raw))
}

func decodeUint64(_raw []byte) uint64 {
	return binary.LittleEndian.Uint64(_raw)
}

func encodeUint32(value uint32) []byte {
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, value)
	return raw
}

func encodeInt64(value int64) []byte {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, uint64(value))
	return raw
}

func encodeUint64(value uint64) []byte {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, value)
	return raw
}
