package wal

type Entry interface {
	Marshal() (_data []byte, err error)
	Unmarshal(_data []byte) error
}
