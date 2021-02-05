package segment

import (
	"os"
	"testing"

	"github.com/matryer/is"
)

var testDir = "../tmp/segment/"

func prepare(dir string) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
}

func cleanup(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func TestPageOffset(t *testing.T) {
	is := is.New(t)
	testdata := map[uint64]uint64{
		5:    0,
		4096: 1,
		5000: 1,
		4095: 0,
		8192: 2,
	}

	s := segment{
		header: header{
			Page: 4096,
		},
	}
	for offset, pageOffset := range testdata {
		po := s.pageOffset(offset)
		is.Equal(po, pageOffset)
	}
}
