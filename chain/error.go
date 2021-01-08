package chain

import (
	"bytes"
	"fmt"
)

var (
	ClosedErr = fmt.Errorf("segement chain is closed")
)

type CloseErr struct {
	Errs []error
}

func (e CloseErr) Error() string {
	closeTextBuf := bytes.NewBufferString("")
	for _, err := range e.Errs {
		closeTextBuf.WriteString(err.Error())
	}
	return "can't close chain: " + closeTextBuf.String()
}
