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
	s, err := createSegment(_dir+"test", _split, _size)
	is.NoErr(err)
	return s
}

func createTestSegmentFilled(is *is.I, _dir string, _split int64, _size int64) *segment {
	s, err := createSegment(_dir+"test", _split, _size)
	is.NoErr(err)

	_blocks := make([]block, 0, _size-1)
	for i := int64(0); i < _size-1; i++ {
		_data := []byte("this is my awesome test data " + strconv.FormatInt(i, 10))
		_block := block{
			First:   true,
			Follow:  0,
			Payload: _data,
		}
		_blocks = append(_blocks, _block)
	}
	_, err = s.write(_blocks)
	is.NoErr(err)

	return s
}

func TestCreateSegment(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentcreate/"
	defer cleanup(dir)
	prepare(dir)

	_split := int64(4)
	_size := int64(100)
	_page, _ := pageSize()
	blksize := _page / _split

	s, err := createSegment(dir+"test", _split, _size)
	is.NoErr(err)
	is.True(s != nil)

	err = s.readHeader()
	is.NoErr(err)

	is.Equal(s.header.Block, blksize)
	is.Equal(s.header.Size, _size)
	is.Equal(s.header.Page, _page)

	err = s.close()
	is.NoErr(err)
}

func TestSegmentWriteSingleBlocks(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentwrite/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)
	_split := int64(4)

	s := createTestSegment(is, dir, _split, _size)
	defer s.close()

	// need to substract 1 since the header is already written to the first block
	is.Equal(s.free(), _size-1)

	for i := int64(0); i < _size-1; i++ {
		_data := []byte("this is my data" + strconv.FormatInt(i, 10))
		_block := block{
			First:   true,
			Follow:  0,
			Payload: _data,
		}
		offset, err := s.write([]block{_block})
		is.NoErr(err)
		is.True(offset > 0)
	}

	ps, _ := pageSize()
	is.Equal(s.free(), int64(0))

	segSize, err := s.size()
	is.NoErr(err)
	is.Equal(segSize, _size*(ps/_split))
}

func TestSegmentWriteMultiBlocks(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentwrite2/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)

	s := createTestSegment(is, dir, 4, _size)
	defer s.close()

	// need to substract 1 since the header is already written to the first block
	is.Equal(s.free(), _size-1)

	_blocks := make([]block, 0, _size-1)
	for i := int64(0); i < _size-1; i++ {
		_data := []byte("this is my data" + strconv.FormatUint(uint64(i), 10))
		_block := block{
			First:   true,
			Follow:  0,
			Payload: _data,
		}
		_blocks = append(_blocks, _block)
	}

	offset, err := s.write(_blocks)
	is.NoErr(err)
	is.True(offset > 0)
	is.Equal(s.free(), int64(0))
}

func TestSegmentRead(t *testing.T) {
	is := is.New(t)
	dir := "tmp/segmentread/"
	defer cleanup(dir)
	prepare(dir)

	_size := int64(20)

	s := createTestSegmentFilled(is, dir, 4, _size)
	defer s.close()

	for i := int64(0); i < _size-1; i++ {
		_blocks, err := s.readAt(s.header.Block * (i + 1))
		is.NoErr(err)

		is.Equal(len(_blocks), 1)
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
