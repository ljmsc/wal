package pouch

import (
	"fmt"
	"strconv"
)

var (
	KeyIsEmptyErr     = fmt.Errorf("record key is empty")
	RecordNotFoundErr = fmt.Errorf("junk not found")
	ClosedErr         = fmt.Errorf("pouch is closed")
	KeyNotFoundErr    = fmt.Errorf("key not found")
	NotEnoughBytesErr = fmt.Errorf("not enough bytes")
	TooManyBytesErr   = fmt.Errorf("too many bytes")
	OnlyHeaderErr     = fmt.Errorf("record contains only header")
	NotClosedErr      = fmt.Errorf("pouch is open for read/write")
)

// ReadErr error occurs when a record could not be read from the pouch file
type ReadErr struct {
	Offset int64
	Err    error
}

func (e *ReadErr) Error() string {
	strOffset := strconv.FormatInt(e.Offset, 10)
	return "can't read record at offset " + strOffset + ": " + e.Err.Error()
}

func (e *ReadErr) Unwrap() error { return e.Err }

// WriteErr error occurs when a record could not be written to the pouch file
type WriteErr struct {
	Err error
}

func (e *WriteErr) Error() string { return "can't write record: " + e.Err.Error() }

func (e *WriteErr) Unwrap() error { return e.Err }

// EnvelopeErr .
type EnvelopeErr struct {
	LastWrittenOffsetNew int64
	LastWrittenOffsetOld int64
	Err                  error
}

func (e EnvelopeErr) Error() string {
	return "stream envelope contains error: " + e.Err.Error()
}

func (e EnvelopeErr) Unwrap() error { return e.Err }
