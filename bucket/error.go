package bucket

import (
	"bytes"
	"fmt"
	"strconv"
)

var (
	ZeroSequenceErr               = fmt.Errorf("sequence number is zero")
	PouchNotFoundErr              = fmt.Errorf("no pouch file for offset")
	RecordNotFoundErr             = fmt.Errorf("record not found")
	ClosedErr                     = fmt.Errorf("bucket is closed")
	MissingSequenceNumberFieldErr = fmt.Errorf("sequence number field is missing in record metadata")
	NotClosedErr                  = fmt.Errorf("bucket is open for read/write")

	NotEnoughPouchesForCompressionErr = fmt.Errorf("not enough pouches for compression")
)

// ReadErr error occurs when a record could not be read from the pouch file
type ReadErr struct {
	SequenceNumber uint64
	PouchName      string
	Err            error
}

func (e ReadErr) Error() string {
	seqNumStr := "N/A"
	if e.SequenceNumber > 0 {
		seqNumStr = strconv.FormatUint(e.SequenceNumber, 10)
	}
	if len(e.PouchName) == 0 {
		return "can't read from bucket: " + e.Err.Error()
	}
	return "can't read from bucket in pouch " + e.PouchName + " with Sequence number " + seqNumStr + ": " + e.Err.Error()
}

func (e ReadErr) Unwrap() error { return e.Err }

// ReadErr error occurs when a record could not be read from the pouch file
type WriteErr struct {
	PouchName string
	Err       error
}

func (e WriteErr) Error() string {
	if len(e.PouchName) == 0 {
		return "can't write to bucket: " + e.Err.Error()
	}
	return "can't write to bucket in pouch " + e.PouchName + ": " + e.Err.Error()
}

func (e WriteErr) Unwrap() error { return e.Err }

type CloseErr struct {
	Errs []error
}

func (e CloseErr) Error() string {
	closeTextBuf := bytes.NewBufferString("")
	for _, err := range e.Errs {
		closeTextBuf.WriteString(err.Error())
	}
	return "can't close bucket: " + closeTextBuf.String()
}

type AddPouchFileErr struct {
	Err error
}

func (e AddPouchFileErr) Error() string {
	return "can't add new pouch file to bucket: " + e.Err.Error()
}

func (e AddPouchFileErr) Unwrap() error { return e.Err }

type CompressionErr struct {
	Err error
}

func (e CompressionErr) Error() string {
	return "can't compress bucket: " + e.Err.Error()
}

func (e CompressionErr) Unwrap() error { return e.Err }
