package bucket

import (
	"bytes"
	"fmt"
	"strconv"
)

var (
	ZeroSequenceErr                    = fmt.Errorf("sequence number is zero")
	SegmentNotFoundErr                 = fmt.Errorf("no segment file for offset")
	RecordNotFoundErr                  = fmt.Errorf("record not found")
	ClosedErr                          = fmt.Errorf("bucket is closed")
	MissingSequenceNumberFieldErr      = fmt.Errorf("sequence number field is missing in record metadata")
	NotClosedErr                       = fmt.Errorf("bucket is open for read/write")
	NotEnoughSegmentsForCompressionErr = fmt.Errorf("not enough segments for compression")
)

// ReadErr error occurs when a record could not be read from the segment file
type ReadErr struct {
	SequenceNumber uint64
	SegmentName    string
	Err            error
}

func (e ReadErr) Error() string {
	seqNumStr := "N/A"
	if e.SequenceNumber > 0 {
		seqNumStr = strconv.FormatUint(e.SequenceNumber, 10)
	}
	if len(e.SegmentName) == 0 {
		return "can't read from bucket: " + e.Err.Error()
	}
	return "can't read from segment " + e.SegmentName + " in bucket with Sequence number " + seqNumStr + ": " + e.Err.Error()
}

func (e ReadErr) Unwrap() error { return e.Err }

// ReadErr error occurs when a record could not be read from the segment file
type WriteErr struct {
	SegmentName string
	Err         error
}

func (e WriteErr) Error() string {
	if len(e.SegmentName) == 0 {
		return "can't write to bucket: " + e.Err.Error()
	}
	return "can't write to segment " + e.SegmentName + " in bucket: " + e.Err.Error()
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

type RecordNotValidErr struct {
	Err error
}

func (e RecordNotValidErr) Error() string { return "record is not valid: " + e.Err.Error() }
func (e RecordNotValidErr) Unwrap() error { return e.Err }

type ConvertErr struct {
	Err error
}

func (e ConvertErr) Error() string { return "can't convert record: " + e.Err.Error() }
func (e ConvertErr) Unwrap() error { return e.Err }
