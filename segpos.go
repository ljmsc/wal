package wal

type segpos struct {
	segment *segment
	seqNum  uint64
}

// offsetBySeqNum returns the offset of the record with sequence number _seqNum.
// _startSeqNum is the first seq number in the segment
func (s *segpos) offsetBySeqNum(_seqNum uint64) (int64, error) {
	return s.segment.offsetByPos((_seqNum - s.seqNum) + 1)
}
