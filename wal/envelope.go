package wal

import "github.com/ljmsc/wal/segment"

// RecordEnvelope .
type RecordEnvelope struct {
	SeqNum uint64
	Err    error
	Record Record
}

// HeaderEnvelope is a wrapper objects for a record header
// the envelope could also contains parts of the data if a padding was provided
type HeaderEnvelope struct {
	Header      segment.Header
	SeqNum      uint64
	Version     uint64
	PaddingData []byte
}
