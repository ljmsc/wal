package wal

type segpos struct {
	segment *segment
	seqNum  uint64
}
