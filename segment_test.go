package wal

import (
	"os"
	"strconv"
	"testing"

	"github.com/matryer/is"
)

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

func createTestSegment(is *is.I, _dir string, _split int64, _size int64) *segment {
	s, err := openSegment(_dir+"test", _split, _size)
	is.NoErr(err)
	return s
}

func createTestSegmentFilled(is *is.I, _dir string, _split int64, _size int64) *segment {
	s, err := openSegment(_dir+"test", _split, _size)
	is.NoErr(err)

	for i := int64(0); i < _size-1; i++ {
		_r := record{
			payload: []byte("this is my awesome test data " + strconv.FormatInt(i, 10)),
		}
		_, err := s.write(_r)
		is.NoErr(err)
	}

	return s
}

func TestCreateSegment(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentcreate/"
	filename := dir + "test"
	defer cleanup(dir)
	prepare(dir)

	_split := int64(4)
	_size := int64(100)

	s, err := openSegment(filename, _split, _size)
	is.NoErr(err)
	is.True(s != nil)
	if s == nil {
		t.FailNow()
		return
	}
	_page, _ := pageSize(filename)
	blksize := _page / _split

	err = s.readHeader()
	is.NoErr(err)

	is.Equal(s.header.Block, blksize)
	is.Equal(s.header.Size, _size)
	is.Equal(s.header.Page, _page)

	err = s.close()
	is.NoErr(err)
}

func TestSegmentWrite(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentwrite/"
	filename := dir + "test"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)
	_split := int64(4)

	s := createTestSegment(is, dir, _split, _size)
	defer s.close()

	// need to subtract 1 since the header is already written to the first block
	is.Equal(s.free(), _size-1)

	for i := int64(0); i < _size-1; i++ {
		_r := record{
			payload: []byte("this is my data" + strconv.FormatInt(i, 10)),
		}
		offset, err := s.write(_r)
		is.NoErr(err)
		is.True(offset > 0)
		is.Equal(offset%s.header.Block, int64(0))
	}

	err := s.sync()
	is.NoErr(err)

	ps, _ := pageSize(filename)
	is.Equal(s.free(), int64(0))

	segSize, err := s.size()
	is.NoErr(err)
	is.Equal(segSize, _size*(ps/_split))
	is.Equal(_size-1, int64(len(s.offsets)))
}

func TestSegmentBlockCount(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentblock/"
	filename := dir + "test"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)
	_split := int64(4)

	s := createTestSegmentFilled(is, dir, _split, _size)
	defer s.close()

	pages, err := pages(filename)
	is.NoErr(err)
	is.Equal(pages, _size/_split)
}

func TestSegmentRead(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentread/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)
	_split := int64(4)
	_page, _ := pageSizefs(dir)
	blksize := _page / _split

	s := createTestSegmentFilled(is, dir, _split, _size)
	defer s.close()

	for i := int64(0); i < _size-1; i++ {
		_data := []byte("this is my awesome test data " + strconv.FormatInt(i, 10))
		_r := record{}
		// i + 1 since the first block is reserved for header information
		offset := (i + 1) * blksize
		err := s.readAt(&_r, offset)
		is.NoErr(err)
		is.Equal(_r.payload, _data)
	}
}

func TestSegmentReadFromStart(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentreadfromstart/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)
	_split := int64(4)
	_page, _ := pageSizefs(dir)
	blksize := _page / _split

	s := createTestSegmentFilled(is, dir, _split, _size)
	defer s.close()

	out, err := s.readFrom(0, 10)
	is.NoErr(err)
	i := 0
	for envelope := range out {
		testData := []byte("this is my awesome test data " + strconv.Itoa(i))
		is.NoErr(envelope.err)
		is.True(envelope.offset > 0)
		is.Equal(envelope.offset%blksize, int64(0))
		is.True(envelope.record.isMetaValid())
		is.True(check(envelope.record.payload, envelope.record.checksum))
		is.Equal(string(envelope.record.payload), string(testData))
		r := record{}
		err := s.readAt(&r, envelope.offset)
		is.NoErr(err)
		i++
	}
}

func TestSegmentReadFrom(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentreadfrom/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)
	_split := int64(4)
	_page, _ := pageSizefs(dir)
	blksize := _page / _split

	s := createTestSegmentFilled(is, dir, _split, _size)
	defer s.close()

	out, err := s.readFrom(4096, 10)
	is.NoErr(err)
	i := 3
	for envelope := range out {
		testData := []byte("this is my awesome test data " + strconv.Itoa(i))
		is.NoErr(envelope.err)
		is.True(envelope.offset > 0)
		is.Equal(envelope.offset%blksize, int64(0))
		is.True(envelope.record.isMetaValid())
		is.True(check(envelope.record.payload, envelope.record.checksum))
		is.Equal(string(envelope.record.payload), string(testData))
		r := record{}
		err := s.readAt(&r, envelope.offset)
		is.NoErr(err)
		i++
	}
}

func TestSegmentReadFrom2(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentreadfrom2/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(5)
	_split := int64(4)
	_page, _ := pageSizefs(dir)
	blksize := _page / _split

	s := createTestSegmentFilled(is, dir, _split, _size)
	defer s.close()

	is.Equal(_size-1, int64(len(s.offsets)))

	out, err := s.readFrom(0, 5)
	is.NoErr(err)
	i := 0
	for envelope := range out {
		testData := []byte("this is my awesome test data " + strconv.Itoa(i))
		is.NoErr(envelope.err)
		is.True(envelope.offset > 0)
		is.Equal(envelope.offset%blksize, int64(0))
		is.True(envelope.record.isMetaValid())
		is.True(check(envelope.record.payload, envelope.record.checksum))
		is.Equal(string(envelope.record.payload), string(testData))
		r := record{}
		err := s.readAt(&r, envelope.offset)
		is.NoErr(err)
		i++
	}
}

func TestSegmentTruncate(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmenttruncate/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)

	s := createTestSegmentFilled(is, dir, 4, _size)
	defer s.close()

	is.Equal(s.free(), int64(0))

	err := s.truncate(s.header.Block * 10)
	is.NoErr(err)
	is.Equal(s.free(), int64(10))
}

func TestSegmentOffsetPos(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentoffsetpos/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)

	s := createTestSegmentFilled(is, dir, 4, _size)
	defer s.close()

	is.Equal(int64(len(s.offsets)), _size-1)
}

func TestSegmentScan(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentscan/"
	defer cleanup(dir)
	prepare(dir)

	_split := int64(4)
	_size := int64(20)

	s := createTestSegmentFilled(is, dir, _split, _size)
	err := s.close()
	is.NoErr(err)

	s2, err := openSegment(dir+"test", _split, _size)
	is.NoErr(err)

	is.Equal(int64(len(s2.offsets)), _size-1)
}
