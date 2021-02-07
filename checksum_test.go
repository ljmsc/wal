package wal

import (
	"hash/fnv"
	"testing"

	"github.com/matryer/is"
)

func TestChecksum(t *testing.T) {
	is := is.New(t)
	payload := []byte("this is my long test dataset")
	h := fnv.New64a()
	_, _ = h.Write(payload)
	cs := h.Sum64()

	is.Equal(sum(payload), cs)
	is.True(check(payload, cs))
}
