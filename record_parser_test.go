package wal

import (
	"testing"

	"github.com/matryer/is"
)

func recordBytes(is *is.I, _block int64, payload string) []byte {
	out := make([]byte, _block)
	r := record{
		payload: []byte(payload),
	}
	raw, err := r.marshal()
	is.NoErr(err)
	copy(out, raw)
	return out
}

func TestWriterWrite(t *testing.T) {
	is := is.New(t)
	blks := int64(1024)
	tr1 := record{
		payload: []byte("this is the first test record"),
	}
	tr1Blk := make([]byte, blks)
	tr1Raw, _ := tr1.marshal()
	copy(tr1Blk, tr1Raw)
	w := recordParser{block: blks}

	complete, err := w.check()
	is.NoErr(err)
	is.Equal(complete, false)

	err = w.write(tr1Blk)
	is.NoErr(err)

	complete, err = w.check()
	is.NoErr(err)
	is.Equal(complete, true)

	tr1out := record{}
	offset, err := w.read(&tr1out)
	is.NoErr(err)
	is.Equal(offset%blks, int64(0))
	is.Equal(tr1out.payload, tr1.payload)
}

func TestWriterWrites(t *testing.T) {
	is := is.New(t)
	blks := int64(1024)
	w := recordParser{block: blks}
	err := w.write(recordBytes(is, blks, "first entry"))
	is.NoErr(err)

	err = w.write(recordBytes(is, blks, "second entry"))
	is.NoErr(err)

	err = w.write(recordBytes(is, blks, "third entry"))
	is.NoErr(err)

	complete, err := w.check()
	is.NoErr(err)
	is.Equal(complete, true)

	r1 := record{}
	off1, err := w.read(&r1)
	is.NoErr(err)
	is.Equal(off1%blks, int64(0))
	is.Equal(r1.isValid(), true)
	is.Equal(r1.payload, []byte("first entry"))

	r2 := record{}
	off2, err := w.read(&r2)
	is.NoErr(err)
	is.Equal(off2%blks, int64(0))
	is.Equal(r2.isValid(), true)
	is.Equal(r2.payload, []byte("second entry"))

	r3 := record{}
	off3, err := w.read(&r3)
	is.NoErr(err)
	is.Equal(off3%blks, int64(0))
	is.Equal(r3.isValid(), true)
	is.Equal(r3.payload, []byte("third entry"))
}
