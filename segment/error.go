package segment

import (
	"fmt"
	"strconv"
)

var (
	KeyIsEmptyErr          = fmt.Errorf("record key is empty")
	RecordNotFoundErr      = fmt.Errorf("junk not found")
	ClosedErr              = fmt.Errorf("segment is closed")
	KeyNotFoundErr         = fmt.Errorf("key not found")
	NotEnoughBytesErr      = fmt.Errorf("not enough bytes")
	TooManyBytesErr        = fmt.Errorf("too many bytes")
	OnlyHeaderErr          = fmt.Errorf("record contains only header")
	NotClosedErr           = fmt.Errorf("segment is open for read/write")
	InvalidRecordOffsetErr = fmt.Errorf("can't read record from offset")
)

// ReadErr error occurs when a record could not be read from the segment file
type ReadErr struct {
	Offset int64
	Err    error
}

func (e ReadErr) Error() string {
	strOffset := strconv.FormatInt(e.Offset, 10)
	return "can't read record at offset " + strOffset + ": " + e.Err.Error()
}

func (e ReadErr) Unwrap() error { return e.Err }

// WriteErr error occurs when a record could not be written to the segment file
type WriteErr struct {
	Err error
}

func (e WriteErr) Error() string { return "can't write record: " + e.Err.Error() }

func (e WriteErr) Unwrap() error { return e.Err }

// CompressionErr .
type CompressionErr struct {
	LastWrittenOffsetNew int64
	LastWrittenOffsetOld int64
	Err                  error
}

func (e CompressionErr) Error() string {
	return "stream envelope contains error: " + e.Err.Error()
}

func (e CompressionErr) Unwrap() error { return e.Err }

type RecordNotValidErr struct {
	Err error
}

func (e RecordNotValidErr) Error() string {
	return "record is not valid: " + e.Err.Error()
}

func (e RecordNotValidErr) Unwrap() error { return e.Err }
