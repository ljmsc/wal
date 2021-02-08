package wal

type envelope struct {
	record record
	err    error
}
