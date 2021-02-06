package wal

import "hash/fnv"

func sum(_payload []byte) uint64 {
	h := fnv.New64a()
	// error can be ignored since fnv can't produce an error
	_, _ = h.Write(_payload)
	return h.Sum64()
}

func check(_payload []byte, _sum uint64) bool {
	return sum(_payload) == _sum
}
