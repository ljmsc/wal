package wal

type Entry interface {
	Unmarshal(_data []byte) error
}
