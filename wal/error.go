package wal

import "fmt"

var (
	ClosedErr               = fmt.Errorf("write ahead log is closed")
	NotClosedErr            = fmt.Errorf("write ahead log is still open for read/write")
	EntryNotFoundErr        = fmt.Errorf("entry not found")
	EntryVersionNotFoundErr = fmt.Errorf("entry version not found")
	MissingVersionErr       = fmt.Errorf("missing entry version")
	VersionIsZeroErr        = fmt.Errorf("version is zero")
	InvalidVersionErr       = fmt.Errorf("invalid version")
	OldVersionErr           = fmt.Errorf("version is to old")
)

type RemoveErr struct {
	Err error
}

func (e RemoveErr) Error() string { return "can't remove log: " + e.Err.Error() }
func (e RemoveErr) Unwrap() error { return e.Err }

type ReadErr struct {
	Err error
}

func (e ReadErr) Error() string { return "can't read from log: " + e.Err.Error() }
func (e ReadErr) Unwrap() error { return e.Err }

type WriteErr struct {
	Err error
}

func (e WriteErr) Error() string { return "can't write to log: " + e.Err.Error() }
func (e WriteErr) Unwrap() error { return e.Err }

type EntryNotValidErr struct {
	Err error
}

func (e EntryNotValidErr) Error() string { return "record is not valid: " + e.Err.Error() }
func (e EntryNotValidErr) Unwrap() error { return e.Err }

type ConvertErr struct {
	Err error
}

func (e ConvertErr) Error() string { return "can't convert entry: " + e.Err.Error() }
func (e ConvertErr) Unwrap() error { return e.Err }
