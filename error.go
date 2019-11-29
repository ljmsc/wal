package wal

import "errors"

var (
	NotEnoughBytesErr    = errors.New("not enough bytes to create a record struct")
	NoRecordFoundErr     = errors.New("no record found")
	SegmentFileClosedErr = errors.New("the segment file is closed")
)
