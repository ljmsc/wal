package segment

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func fillSegment(p Segment, amount int, writeSuffix string) []int64 {
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

func createTestSegment(name string, dir string) Segment {
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

func TestOpenReadWrite(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	seg, err := Open(testDir + "test_open_read_write")
	require.NoError(t, err)

	for i := 0; i < 15; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i) + "WriteRecord")
		testRecord := CreateRecord(testKey, testData)

		_, err := seg.WriteRecord(testRecord)
		require.NoError(t, err)
	}

	for i := 15; i < 20; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i) + "write")

		_, err := seg.Write(testKey, testData)
		require.NoError(t, err)
	}

	_, err = seg.Size()
	assert.NoError(t, err)

	closeErr := seg.Close()
	require.NoError(t, closeErr)

	seg2, err := Open(testDir + "test_open_read_write")
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))

		testReadRecord := Record{}
		err := seg2.ReadByKey(testKey, true, &testReadRecord)
		require.NoError(t, err)
		assert.EqualValues(t, testKey, testReadRecord.Key)

		testReadRecord2 := Record{}
		err2 := seg2.ReadByKey(testKey, false, &testReadRecord2)
		if !assert.NoError(t, err2) {
			return
		}
		assert.EqualValues(t, testKey, testReadRecord2.Key)
	}

	for i := 15; i < 20; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i) + "write_new")

		_, err := seg2.Write(testKey, testData)
		require.NoError(t, err)
	}

	/*
		stream := seg2.StreamRecords(true)
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


	*/
	closeErr = seg2.Close()
	require.NoError(t, closeErr)
}

func TestSnapshot(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	name := testDir + "test_snapshot"
	snapName := name + "_copy"
	seg, err := Open(name)
	require.NoError(t, err)
	defer seg.Close()

	fillSegment(seg, 15, "")

	size, err := seg.Size()
	assert.NoError(t, err)

	err = seg.Snapshot(snapName)
	require.NoError(t, err)

	seg2, err := Open(snapName)
	require.NoError(t, err)
	defer seg2.Close()

	size2, err := seg2.Size()
	assert.NoError(t, err)

	assert.EqualValues(t, size, size2)
}

func TestRemove(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	name := testDir + "test_remove"
	seg, err := Open(name)
	require.NoError(t, err)

	fillSegment(seg, 15, "")

	err = seg.Close()
	assert.NoError(t, err)

	err = seg.Remove()
	require.NoError(t, err)
}

func TestTruncate(t *testing.T) {
	prepare(testDir)
	defer cleanup(testDir)
	// defer

	name := testDir + "test_truncate"
	seg, err := Open(name)
	require.NoError(t, err)
	defer seg.Close()

	offsets := fillSegment(seg, 15, "")
	assert.EqualValues(t, 15, len(offsets))

	latestOffsets := seg.LastOffsets()
	assert.EqualValues(t, 15, len(latestOffsets))
	offset9 := offsets[9]

	latestOffset := seg.LastOffset()
	assert.EqualValues(t, 750, latestOffset)

	r := Record{}
	err = seg.ReadByOffset(offset9, true, &r)
	assert.NoError(t, err)

	err = seg.Truncate(offset9)
	require.NoError(t, err, "can' truncate segment")

	r2 := Record{}
	err = seg.ReadByOffset(offset9, true, &r2)
	assert.EqualError(t, err, InvalidRecordOffsetErr.Error())

	offset8 := seg.LastOffset()
	r3 := Record{}
	err = seg.ReadByOffset(offset8, true, &r3)
	assert.NoError(t, err)

	latestOffsets = seg.LastOffsets()
	assert.EqualValues(t, 9, len(latestOffsets))
}
