package wal

import (
	"strconv"
	"testing"

	"github.com/matryer/is"
)

type testEntry []byte

func (t *testEntry) Marshal() (data []byte, err error) {
	return *t, nil
}

func (t *testEntry) Unmarshal(_data []byte) error {
	*t = _data
	return nil
}

func createTestWal(is *is.I, _dir string, _split int64, _size int64) Wal {
	w, err := Open(_dir+"test", _split, _size)
	is.NoErr(err)
	return w
}

func createTestWalFilled(is *is.I, _dir string, _size int64) Wal {
	w := createTestWal(is, _dir, 4, _size)

	for i := int64(0); i < _size*2; i++ {
		e := []byte("this is pre filled test data: " + strconv.FormatInt(i+1, 10))
		_, err := w.Write(e)
		is.NoErr(err)
	}

	return w
}

func TestWalOpen(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walopen/"
	defer cleanup(dir)
	prepare(dir)

	w, err := Open(dir+"test", 4, 20)
	is.NoErr(err)
	is.True(w != nil)

	is.Equal(w.Name(), dir+"test")

	err = w.Close()
	is.NoErr(err)
}

func TestWalWrite(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walwrite/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWal(is, dir, 4, _size)
	defer w.Close()

	for i := int64(0); i < _size; i++ {
		e := []byte("this is test data: " + strconv.FormatInt(i+1, 10))
		seqNum, err := w.Write(e)
		is.NoErr(err)
		is.True(seqNum > 0)
		is.Equal(seqNum, uint64(i+1))
	}

	is.Equal(w.SeqNum(), uint64(_size))
}

func TestWalSync(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walsync/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWalFilled(is, dir, _size)
	defer w.Close()

	seqNum, err := w.Sync()
	is.NoErr(err)
	is.Equal(seqNum, uint64(40))

	safeSeqNum := w.Safe()
	is.Equal(safeSeqNum, uint64(40))
}

func TestWalReadAt(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walread/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWalFilled(is, dir, _size)
	defer w.Close()

	for i := int64(0); i < _size*2; i++ {
		testData := []byte("this is pre filled test data: " + strconv.FormatInt(i+1, 10))
		e := testEntry{}
		err := w.ReadAt(&e, uint64(i+1))
		is.NoErr(err)
		is.Equal([]byte(e), testData)
	}
}

func TestWalReadFromStart(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walreadfromstart/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWalFilled(is, dir, _size)
	defer w.Close()

	out, err := w.ReadFrom(1, 40)
	is.NoErr(err)

	i := 1
	for envelope := range out {
		testData := []byte("this is pre filled test data: " + strconv.Itoa(i))
		is.NoErr(envelope.Err)
		is.Equal(envelope.SeqNum, uint64(i))
		is.Equal(string(envelope.Payload), string(testData))
		i++
	}
}

func TestWalReadFrom(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walreadfrom/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWalFilled(is, dir, _size)
	defer w.Close()

	out, err := w.ReadFrom(10, 10)
	is.NoErr(err)

	i := 10
	for envelope := range out {
		testData := []byte("this is pre filled test data: " + strconv.Itoa(i))
		is.NoErr(envelope.Err)
		is.Equal(envelope.SeqNum, uint64(i))
		is.Equal(string(envelope.Payload), string(testData))
		i++
	}
	is.Equal(i, 20)
}

func TestWalTruncate(t *testing.T) {
	is := is.New(t)
	dir := "tmp/waltruncate/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWalFilled(is, dir, _size)
	defer w.Close()

	truncSeqNum := uint64(16)
	e := testEntry{}
	err := w.ReadAt(&e, truncSeqNum)
	is.NoErr(err)

	err = w.Truncate(truncSeqNum)
	is.NoErr(err)

	e2 := testEntry{}
	err = w.ReadAt(&e2, truncSeqNum)
	is.Equal(err, ErrEntryNotFound)
}

func TestWalReopen(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walreopen/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWalFilled(is, dir, _size)
	err := w.Close()
	is.NoErr(err)

	wRe, err := Open(dir+"test", 4, _size)
	is.NoErr(err)
	defer wRe.Close()

	for i := int64(0); i < _size*2; i++ {
		testData := []byte("this is pre filled test data: " + strconv.FormatInt(i+1, 10))
		e := testEntry{}
		err := wRe.ReadAt(&e, uint64(i+1))
		is.NoErr(err)
		is.Equal([]byte(e), testData)
	}
}

func TestWalReopenWrite(t *testing.T) {
	is := is.New(t)
	dir := "tmp/walreopenwrite/"
	defer cleanup(dir)
	prepare(dir)
	_size := int64(20)
	w := createTestWalFilled(is, dir, _size)
	err := w.Close()
	is.NoErr(err)

	wRe, err := Open(dir+"test", 4, _size)
	is.NoErr(err)
	defer wRe.Close()

	for i := int64(1); i <= 5; i++ {
		data := []byte("this is pre filled test data: " + strconv.FormatInt(i+1, 10))
		seqNum, err := wRe.Write(data)
		is.NoErr(err)
		is.Equal(seqNum, uint64(i+40))
	}
}
