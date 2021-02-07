package wal

import (
	"os"
	"testing"

	"github.com/matryer/is"
)

func TestPageSize(t *testing.T) {
	is := is.New(t)
	ps, err := pageSize(".")
	is.NoErr(err)
	is.True(ps > 0)
}

func TestPages(t *testing.T) {
	is := is.New(t)
	dir := "tmp/pages/"
	testFile := dir + "test"
	blockC := int64(2)
	defer cleanup(dir)
	prepare(dir)

	ps, err := pageSize(dir)
	is.NoErr(err)

	f, err := os.Create(testFile)
	is.NoErr(err)
	defer f.Close()

	dummyData := make([]byte, ps*blockC)
	_, err = f.Write(dummyData)
	is.NoErr(err)

	p, err := pages(testFile)
	is.NoErr(err)
	is.Equal(p, blockC)
}
