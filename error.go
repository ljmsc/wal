package wal

import "errors"

var (
	// ErrNotEnoughBytes .
	ErrNotEnoughBytes = errors.New("not enough bytes to create a record struct")
	// ErrNoRecordFound .
	ErrNoRecordFound = errors.New("no record found")
	// ErrSegmentFileClosed .
	ErrSegmentFileClosed = errors.New("the segment file is closed")
)
