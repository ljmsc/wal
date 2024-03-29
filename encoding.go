package wal

import "encoding/binary"

func decodeInt64(_raw []byte) int64 {
	return int64(binary.LittleEndian.Uint64(_raw))
}

func decodeUint64(_raw []byte) uint64 {
	return binary.LittleEndian.Uint64(_raw)
}

func encodeInt64(value int64) []byte {
	raw := make([]byte, 8) // nolint
	binary.LittleEndian.PutUint64(raw, uint64(value))
	return raw
}

func encodeUint64(value uint64) []byte {
	raw := make([]byte, 8) // nolint
	binary.LittleEndian.PutUint64(raw, value)
	return raw
}
