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

func (e RemoveErr) Error() string { return "can't remove write ahead log: " + e.Err.Error() }
func (e RemoveErr) Unwrap() error { return e.Err }

type ReadErr struct {
	Err error
}

func (e ReadErr) Error() string { return "can't read from ahead log: " + e.Err.Error() }
func (e ReadErr) Unwrap() error { return e.Err }

type WriteErr struct {
	Err error
}

func (e WriteErr) Error() string { return "can't write from ahead log: " + e.Err.Error() }
func (e WriteErr) Unwrap() error { return e.Err }

type EntryNotValidErr struct {
	Err error
}

func (e EntryNotValidErr) Error() string { return "record is not valid: " + e.Err.Error() }
func (e EntryNotValidErr) Unwrap() error { return e.Err }
