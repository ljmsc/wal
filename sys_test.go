package wal

import (
	"testing"

	"github.com/matryer/is"
)

func TestPageSize(t *testing.T) {
	is := is.New(t)
	ps, err := pageSize()
	is.NoErr(err)
	is.True(ps > 0)
}
