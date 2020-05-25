package wal

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

var walTestDir = "../tmp/wal/"

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

func createTestLog(name string, dir string) *Wal {
	b, err := Open(dir+name, 500, true, nil)
	if err != nil {
		panic(err)
	}
	if b == nil {
		panic("wal " + name + " is nil")
	}
	return b
}

func createTestLogWithData(name string, dir string, loops int) *Wal {
	w := createTestLog(name, dir)
	for i := 1; i <= loops; i++ {
		eKey := []byte("key_" + strconv.Itoa(i))
		eData := []byte("data_" + strconv.Itoa(i))
		entry := CreateEntry(eKey, eData)
		if err := w.Write(entry); err != nil {
			panic(err)
		}
	}
	return w
}

func TestOpenWithHandler(t *testing.T) {
	prepare(walTestDir)
	defer cleanup(walTestDir)

	w, err := OpenWithHandler(walTestDir+"test_open", true, nil)
	require.NoError(t, err)
	require.NotNil(t, w)
	err = w.Close()
	assert.NoError(t, err)
}

func TestWalReadWrite(t *testing.T) {
	prepare(walTestDir)
	defer cleanup(walTestDir)

	w := createTestLog("test_read_write", walTestDir)
	defer w.Close()

	for i := 1; i <= 100; i++ {
		eKey := []byte("key_" + strconv.Itoa(i))
		eData := []byte("data_" + strconv.Itoa(i))
		entry := CreateEntry(eKey, eData)
		err := w.Write(entry)
		assert.NoError(t, err)
	}

	for i := 1; i <= 100; i++ {
		eKey := []byte("key_" + strconv.Itoa(i))
		entry := Entry{}
		err := w.ReadBySequenceNumber(uint64(i), true, &entry)
		assert.NoError(t, err)
		assert.EqualValues(t, eKey, entry.Key)
		assert.EqualValues(t, 1, entry.Version())

		entry2 := Entry{}
		err = w.ReadByKey(eKey, true, &entry2)
		assert.NoError(t, err)
		assert.EqualValues(t, eKey, entry2.Key)
		assert.EqualValues(t, 1, entry2.Version())
	}
}

func TestWalReadVersion(t *testing.T) {
	prepare(walTestDir)
	defer cleanup(walTestDir)
	w := createTestLogWithData("test_versions", walTestDir, 10)
	defer w.Close()

	for i := 1; i <= 5; i++ {
		eKey := []byte("key_" + strconv.Itoa(i))
		eData := []byte("data_" + strconv.Itoa(i))
		entry := CreateEntry(eKey, eData)
		err := w.Write(entry)
		assert.NoError(t, err)
	}

	for i := 1; i <= 5; i++ {
		eKey := []byte("key_" + strconv.Itoa(i))
		entry := Entry{}
		err := w.ReadByKeyAndVersion(eKey, 2, true, &entry)
		assert.NoError(t, err)
		assert.EqualValues(t, eKey, entry.Key)
		assert.EqualValues(t, 2, entry.Version())
	}

	eKey := []byte("key_not_exist")
	entry := Entry{}
	err := w.ReadByKeyAndVersion(eKey, 1, true, &entry)
	assert.Error(t, err)
}

func TestWalStreamEntries(t *testing.T) {
	prepare(walTestDir)
	defer cleanup(walTestDir)
	w := createTestLogWithData("test_versions", walTestDir, 25)
	defer w.Close()

	stream := w.StreamEntries(1, 0, true)
	count := 0
	for {
		item, ok := <-stream
		if !ok {
			break
		}
		assert.NoError(t, item.Err)
		count++
	}
	assert.EqualValues(t, 25, count)
}

func TestWalCompressWithFilter(t *testing.T) {
	prepare(walTestDir)
	defer cleanup(walTestDir)
	w := createTestLogWithData("test_compression", walTestDir, 20)
	defer w.Close()

	preComSize, err := w.Size()
	assert.NoError(t, err)

	err = w.CompressWithFilter(func(item *Entry) bool {
		if item.SequenceNumber()%2 == 0 {
			return true
		}
		return false
	})
	assert.NoError(t, err)

	postComSize, err := w.Size()
	assert.NoError(t, err)
	assert.LessOrEqual(t, postComSize, preComSize)

	stream := w.StreamEntries(1, 0, true)
	for {
		item, ok := <-stream
		if !ok {
			break
		}
		assert.NoError(t, item.Err)
	}
}

func TestWalCompareAndWrite(t *testing.T) {

}
