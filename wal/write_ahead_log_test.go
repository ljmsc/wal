package wal

import (
	"os"
	"strconv"
	"testing"

	"github.com/matryer/is"
)

var walTestDir = "../tmp/wal/"

func TestMain(m *testing.M) {
	os.Exit(func() int {
		defer cleanup(walTestDir)
		prepare(walTestDir)
		return m.Run()
	}())
}

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

func createTestWal(name string) Wal {
	w, err := Open(walTestDir+name, 0, 0, nil)
	if err != nil {
		panic(err)
	}
	return w
}

func createTestWalPreFilled(name string, loops int, versions int) Wal {
	w := createTestWal(name)
	for i := 1; i <= versions; i++ {
		for j := 1; j <= loops; j++ {
			r := record{
				key:  uint64(j),
				data: []byte("test data key: " + strconv.Itoa(j) + " version: " + strconv.Itoa(i)),
			}
			if _, err := w.Write(&r); err != nil {
				panic(err)
			}
		}
	}
	return w
}

func TestOpen(t *testing.T) {
	is := is.New(t)
	w, err := Open(walTestDir+"open_test", 0, 0, nil)
	is.NoErr(err)
	defer w.Close()
}

func TestWalWrite(t *testing.T) {
	is := is.New(t)
	l := 50
	w := createTestWal("write_test")
	defer w.Close()
	for i := 0; i < l; i++ {
		r := record{
			key:  uint64(i + 1),
			data: []byte("test data " + strconv.Itoa(i+1)),
		}
		seqNum, err := w.Write(&r)
		is.NoErr(err)
		is.Equal(seqNum, uint64(i+1))
	}
}

func TestWalRead(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	l := 10
	w := createTestWalPreFilled("read_test", l, 2)
	defer w.Close()
	for i := 1; i <= l; i++ {
		testKey := uint64(i)
		testData := []byte("test data key: " + strconv.Itoa(i) + " version: 2")
		r := record{}
		err := w.ReadKey(&r, testKey)
		is.NoErr(err)
		is.Equal(r.key, testKey)
		is.Equal(r.seqNum, uint64(i+l))
		is.Equal(r.version, uint64(2))
		is.Equal(len(r.data), len(testData))
		is.Equal(string(r.data), string(testData))
	}
}

func TestReadVersion(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	l := 10
	w := createTestWalPreFilled("read_version_test", l, 3)
	defer w.Close()
	for i := 1; i <= l; i++ {
		testKey := uint64(i)
		testData := []byte("test data key: " + strconv.Itoa(i) + " version: 2")
		r := record{}
		err := w.ReadVersion(&r, testKey, 2)
		is.NoErr(err)
		is.Equal(r.key, testKey)
		is.Equal(r.version, uint64(2))
		is.Equal(len(r.data), len(testData))
		is.Equal(string(r.data), string(testData))
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("version_test", 10, 3)
	defer w.Close()
	version := Version(w, 5)
	is.Equal(version, uint64(3))
}

func TestCompareAndWriteHappy(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("compare_write_test_happy", 10, 3)
	defer w.Close()
	r := record{
		key:  uint64(5),
		data: []byte("test data"),
	}
	_, err := w.CompareAndWrite(&r, 3)
	is.NoErr(err)
}

func TestCompareAndWriteOutdatedVersion(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("compare_write_test_outdated", 10, 3)
	defer w.Close()
	r := record{
		key:  uint64(5),
		data: []byte("test data"),
	}

	_, err := w.CompareAndWrite(&r, 2)
	if err == nil {
		is.Fail()
	}
	is.Equal(err.Error(), RecordOutdatedErr.Error())
}

func TestCompareAndWriteInvalidVersion(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("compare_write_test_invalid", 10, 3)
	defer w.Close()
	r := record{
		key:  uint64(5),
		data: []byte("test data"),
	}

	_, err := w.CompareAndWrite(&r, 10)
	if err == nil {
		is.Fail()
	}
	is.Equal(err.Error(), "invalid version")
}

func TestLength(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("length_test", 10, 3)
	defer w.Close()
	l := w.Length()
	is.Equal(l, uint64(30))
}

func TestFirstSeqNum(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("seqnum_test", 10, 3)
	defer w.Close()
	fs := w.FirstSeqNum()
	is.Equal(fs, uint64(1))

	ls := w.SeqNum()
	is.Equal(ls, uint64(30))
}

func TestKeySeqNums(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("keyseqnum_test", 10, 3)
	defer w.Close()
	ksn := w.KeySeqNums()

	is.Equal(len(ksn), 10)
	for key := uint64(1); key <= 10; key++ {
		if _, ok := ksn[key]; !ok {
			is.Fail()
		}
		is.Equal(len(ksn[key]), 3)
	}
}

func TestKeyVersionSeqNum(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("keyversionseqnum_test", 10, 3)
	defer w.Close()
	kvs := w.KeySeqNums()

	is.Equal(len(kvs), 10)
	for key := uint64(1); key <= 10; key++ {
		if _, ok := kvs[key]; !ok {
			is.Fail()
		}
		is.Equal(len(kvs[key]), 3)
	}
}

func TestIsClosed(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	w := createTestWalPreFilled("closed_test", 10, 3)
	err := w.Close()
	is.NoErr(err)

	is.True(w.IsClosed())
}
