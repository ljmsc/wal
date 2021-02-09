package wal

type envelope struct {
	offset int64
	record record
	err    error
}

type Envelope struct {
	SeqNum  uint64
	Payload []byte
	err     error
}
