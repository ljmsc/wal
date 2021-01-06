package segment

import (
	"fmt"
)

var (
	RecordNotFoundErr = fmt.Errorf("record not found")
	ClosedErr         = fmt.Errorf("segment is closed")
	NotClosedErr      = fmt.Errorf("segment is open for read/write")
)

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
