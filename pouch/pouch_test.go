package pouch

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDir = "../tmp/pouch/"

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

func fillPouch(p *Pouch, amount int, writeSuffix string) []int64 {
	offsets := make([]int64, 0, amount)
	for i := 0; i < amount; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i) + writeSuffix)
		testRecord := CreateRecord(testKey, testData)

		offset, err := p.WriteRecord(testRecord)
		if err != nil {
			panic(err)
		}
		offsets = append(offsets, offset)
	}
	return offsets
}

func createTestSegment(name string, dir string) *Pouch {
	prepare(testDir)
	defer cleanup(testDir)
	seg, err := Open(dir + name)
	if err != nil {
		panic(err)
	}
	if seg == nil {
		panic("segment " + name + " is nil")
	}
	return seg
}

func TestPouchOpenReadWrite(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	pou, err := Open(testDir + "test_open_read_write")
	require.NoError(t, err)

	for i := 0; i < 15; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i) + "WriteRecord")
		testRecord := CreateRecord(testKey, testData)

		_, err := pou.WriteRecord(testRecord)
		require.NoError(t, err)
	}

	for i := 15; i < 20; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i) + "write")

		_, err := pou.Write(testKey, testData)
		require.NoError(t, err)
	}

	_, err = pou.Size()
	assert.NoError(t, err)

	closeErr := pou.Close()
	require.NoError(t, closeErr)

	pou2, err := Open(testDir + "test_open_read_write")
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))

		testReadRecord := Record{}
		err := pou2.ReadByKey(testKey, true, &testReadRecord)
		require.NoError(t, err)
		assert.EqualValues(t, testKey, testReadRecord.Key)

		testReadRecord2 := Record{}
		err2 := pou2.ReadByKey(testKey, false, &testReadRecord2)
		if !assert.NoError(t, err2) {
			return
		}
		assert.EqualValues(t, testKey, testReadRecord2.Key)
	}

	for i := 15; i < 20; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i) + "write_new")

		_, err := pou2.Write(testKey, testData)
		require.NoError(t, err)
	}

	stream := pou2.StreamRecords(true)
	for {
		item, ok := <-stream
		if !ok {
			break
		}
		if !assert.NoError(t, item.Err) {
			continue
		}

		if !strings.HasPrefix(string(item.Record.Key), "test_key_") {
			t.Errorf("wrong record key: %s", string(item.Record.Key))
		}
	}

	closeErr = pou2.Close()
	require.NoError(t, closeErr)
}

func TestPouchSnapshot(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	name := testDir + "test_snapshot"
	snapName := name + "_copy"
	pou, err := Open(name)
	require.NoError(t, err)
	defer pou.Close()

	fillPouch(pou, 15, "")

	size, err := pou.Size()
	assert.NoError(t, err)

	err = pou.Snapshot(snapName)
	require.NoError(t, err)

	pou2, err := Open(snapName)
	require.NoError(t, err)
	defer pou2.Close()

	size2, err := pou2.Size()
	assert.NoError(t, err)

	assert.EqualValues(t, size, size2)
}

func TestPouchRemove(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	name := testDir + "test_remove"
	pou, err := Open(name)
	require.NoError(t, err)

	fillPouch(pou, 15, "")

	err = pou.Close()
	assert.NoError(t, err)

	err = pou.Remove()
	require.NoError(t, err)
}

func TestPouchTruncate(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	// defer

	name := testDir + "test_truncate"
	pou, err := Open(name)
	require.NoError(t, err)
	defer pou.Close()

	offsets := fillPouch(pou, 15, "")
	assert.EqualValues(t, 15, len(offsets))

	latestOffsets := pou.LastOffsets()
	assert.EqualValues(t, 15, len(latestOffsets))
	offset9 := offsets[9]

	latestOffset := pou.LastOffset()
	assert.EqualValues(t, 750, latestOffset)

	r := Record{}
	err = pou.ReadByOffset(offset9, true, &r)
	assert.NoError(t, err)

	err = pou.Truncate(offset9)
	require.NoError(t, err, "can' truncate pouch")

	r2 := Record{}
	err = pou.ReadByOffset(offset9, true, &r2)
	assert.EqualError(t, err, InvalidRecordOffsetErr.Error())

	offset8 := pou.LastOffset()
	r3 := Record{}
	err = pou.ReadByOffset(offset8, true, &r3)
	assert.NoError(t, err)

	latestOffsets = pou.LastOffsets()
	assert.EqualValues(t, 9, len(latestOffsets))
}
