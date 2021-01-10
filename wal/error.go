package wal

import "fmt"

var (
	ClosedErr                = fmt.Errorf("closed")
	RecordVersionNotFoundErr = fmt.Errorf("record version not found")
	RecordOutdatedErr        = fmt.Errorf("record (version) is outdated")
)
