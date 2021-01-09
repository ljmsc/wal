package segment

import (
	"os"
	"strconv"
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

func createRecord(_key uint64, _payload []byte) Record {
	r := record{
		key:  _key,
		data: _payload,
	}
	return &r
}

func fillSegment(p Segment, amount int, writeSuffix string) []int64 {
	offsets := make([]int64, 0, amount)
	for i := 0; i < amount; i++ {
		testKey := uint64(i + 1)
		testData := []byte("test_data_" + strconv.Itoa(i) + writeSuffix)
		testRecord := createRecord(testKey, testData)
		offset, err := p.Write(testRecord)
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
	seg, err := Open(dir+name, nil)
	if err != nil {
		panic(err)
	}
	if seg == nil {
		panic("segment " + name + " is nil")
	}
	return seg
}

func TestOpenReadWrite(t *testing.T) {
	is := is.New(t)
	prepare(testDir)
	defer cleanup(testDir)
	seg, err := Open(testDir+"test_open_read_write", nil)
	is.NoErr(err)

	for i := 0; i < 15; i++ {
		testKey := uint64(i + 1)
		testData := []byte("test_data_" + strconv.Itoa(i) + "WriteRecord")
		testRecord := createRecord(testKey, testData)

		_, err := seg.Write(testRecord)
		is.NoErr(err)
	}

	size, err := seg.Size()
	is.NoErr(err)
	is.Equal(size, int64(575))

	offsets := seg.Offsets()
	is.Equal(len(offsets), 15)

	closeErr := seg.Close()
	is.NoErr(closeErr)

	seg2, err := Open(testDir+"test_open_read_write", nil)
	is.NoErr(err)

	size, err = seg2.Size()
	is.NoErr(err)
	is.Equal(size, int64(575))

	offsets2 := seg2.Offsets()
	is.Equal(len(offsets2), 15)

	for i := 0; i < 15; i++ {
		testKey := uint64(i + 1)
		testData := []byte("test_data_" + strconv.Itoa(i) + "WriteRecord")
		testReadRecord := record{}
		err := seg2.ReadKey(&testReadRecord, testKey)
		is.NoErr(err)
		is.Equal(testKey, testReadRecord.Key())
		is.Equal(len(testData), len(testReadRecord.data))
		is.Equal(string(testData), string(testReadRecord.data))
	}

	for i := 15; i < 20; i++ {
		testKey := uint64(i + 1)
		testData := []byte("test_data_" + strconv.Itoa(i) + "write_new")

		r := createRecord(testKey, testData)
		_, err := seg2.Write(r)
		is.NoErr(err)
	}

	closeErr = seg2.Close()
	is.NoErr(closeErr)
}

func TestSnapshot(t *testing.T) {
	is := is.New(t)
	prepare(testDir)
	defer cleanup(testDir)
	name := testDir + "test_snapshot"
	snapName := name + "_copy"
	seg, err := Open(name, nil)
	is.NoErr(err)
	defer seg.Close()

	fillSegment(seg, 15, "")

	size, err := seg.Size()
	is.NoErr(err)

	err = Snapshot(seg, snapName)
	is.NoErr(err)

	seg2, err := Open(snapName, nil)
	is.NoErr(err)
	defer seg2.Close()

	size2, err := seg2.Size()
	is.NoErr(err)

	is.Equal(size, size2)
}

func TestRemove(t *testing.T) {
	is := is.New(t)
	prepare(testDir)
	defer cleanup(testDir)
	name := testDir + "test_remove"
	seg, err := Open(name, nil)
	is.NoErr(err)

	fillSegment(seg, 15, "")

	err = seg.Close()
	is.NoErr(err)

	err = Remove(seg)
	is.NoErr(err)
}

func TestTruncate(t *testing.T) {
	is := is.New(t)
	prepare(testDir)
	defer cleanup(testDir)
	// defer

	name := testDir + "test_truncate"
	seg, err := Open(name, nil)
	is.NoErr(err)
	defer seg.Close()

	offsets := fillSegment(seg, 15, "")
	is.Equal(15, len(offsets))
	offset9 := offsets[9]

	latestOffset := seg.Offset()
	is.Equal(int64(382), latestOffset)

	r := record{}
	err = seg.ReadAt(&r, offset9)
	is.NoErr(err)

	err = seg.Truncate(offset9)
	is.NoErr(err)

	r2 := record{}
	err = seg.ReadAt(&r2, offset9)
	if err == nil {
		is.Fail()
	}
	is.Equal(err.Error(), RecordNotFoundErr.Error())

	offset8 := seg.Offset()
	r3 := record{}
	err = seg.ReadAt(&r3, offset8)
	is.NoErr(err)

	offsets = seg.Offsets()
	is.Equal(9, len(offsets))
}
