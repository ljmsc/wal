package chain

import (
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/ljmsc/wal/segment"

	"github.com/matryer/is"

	"github.com/stretchr/testify/assert"
)

var chainTestDir = "../tmp/chain/"

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

func createTestChain(name string, dir string) Chain {
	b, err := Open(dir+name, 0, 300, nil)
	if err != nil {
		panic(err)
	}
	if b == nil {
		panic("chain " + name + " is nil")
	}
	return b
}

func TestOpen(t *testing.T) {
	is := is.New(t)
	prepare(chainTestDir)
	defer cleanup(chainTestDir)
	c, err := Open(chainTestDir+"chain_open", 0, 0, nil)

	is.NoErr(err)
	is.True(c != nil)

	err = c.Close()
	is.NoErr(err)
}

func TestOpenWriteRead(t *testing.T) {
	is := is.New(t)
	cleanup(chainTestDir)
	prepare(chainTestDir)
	c, err := Open(chainTestDir+"chain_read_write", 0, 300, nil)
	is.NoErr(err)

	for i := 1; i <= 30; i++ {
		testKey := uint64(i)
		testData := []byte("chain_test_data_" + strconv.Itoa(i))

		r := record{
			key:  testKey,
			data: testData,
		}
		_, err := c.Write(&r)
		is.NoErr(err)
	}

	is.Equal(uint64(30), c.Length())

	for i := 1; i <= 30; i++ {
		testKey := uint64(i)
		testData := []byte("chain_test_data_" + strconv.Itoa(i))
		r := record{}
		err := c.ReadAt(&r, testKey)
		is.NoErr(err)
		is.Equal(testKey, r.Key())
		is.Equal(uint64(i), r.SeqNum())
		is.Equal(len(testData), len(r.Data()))
		is.Equal(testData, r.Data())
	}

	err = c.Close()
	is.NoErr(err)

	c2, err := Open(chainTestDir+"chain_read_write", 0, 300, nil)
	is.NoErr(err)

	is.Equal(uint64(30), c2.Length())

	for i := 1; i <= 30; i++ {
		testKey := uint64(i)
		testData := []byte("chain_test_data_" + strconv.Itoa(i))
		r := record{}
		err := c2.ReadKey(&r, testKey)
		is.NoErr(err)
		is.Equal(testKey, r.Key())
		is.Equal(uint64(i), r.SeqNum())
		is.Equal(len(testData), len(r.Data()))
		is.Equal(testData, r.Data())
	}

	err = c2.Close()
	assert.NoError(t, err)
}

func TestKeys(t *testing.T) {
	is := is.New(t)
	prepare(chainTestDir)
	defer cleanup(chainTestDir)
	c := createTestChain("segment_test2", chainTestDir)
	defer c.Close()

	for i := 1; i <= 20; i++ {
		testKey := uint64(i)
		testData := []byte("test_data_" + strconv.Itoa(i))
		r := record{
			key:  testKey,
			data: testData,
		}

		_, err := c.Write(&r)
		is.NoErr(err)
	}
	keys := Keys(c)
	is.Equal(len(keys), 20)
	sort.Slice(keys, func(i, j int) bool {
		if keys[i] < keys[j] {
			return true
		}
		return false
	})
	for i := 1; i <= 20; i++ {
		is.Equal(uint64(i), keys[i-1])
	}
}

func TestStream(t *testing.T) {
	is := is.New(t)
	prepare(chainTestDir)
	defer cleanup(chainTestDir)
	c := createTestChain("segment_test2", chainTestDir)
	defer c.Close()

	for i := 1; i <= 20; i++ {
		testKey := uint64(((i - 1) % 5) + 1)
		testData := []byte("test_data_" + strconv.Itoa(i))
		r := record{
			key:  testKey,
			data: testData,
		}

		_, err := c.Write(&r)
		is.NoErr(err)
	}

	counter := 0
	stream := Stream(c, 0, 0)
	for envelope := range stream {
		if envelope.Err != nil {
			is.NoErr(envelope.Err)
			continue
		}
		counter++
	}
	is.Equal(20, counter)
}

func TestTruncateChain(t *testing.T) {
	is := is.New(t)
	prepare(chainTestDir)
	defer cleanup(chainTestDir)
	c := createTestChain("dump", chainTestDir)
	defer c.Close()

	for i := 1; i <= 20; i++ {
		testKey := uint64(i)
		testData := []byte("test_data_" + strconv.Itoa(i))
		r := record{
			key:  testKey,
			data: testData,
		}

		_, err := c.Write(&r)
		is.NoErr(err)
	}

	seqNumber := c.SeqNum()
	assert.EqualValues(t, 20, seqNumber)

	dumpNum := uint64(10)

	err := c.Truncate(dumpNum)
	is.NoErr(err)

	r := record{}
	err = c.ReadAt(&r, dumpNum)
	if err == nil {
		is.Fail()
	}
	is.Equal(err.Error(), segment.RecordNotFoundErr.Error())

	r2 := record{}
	err = c.ReadAt(&r2, c.SeqNum())
	is.NoErr(err)

	is.Equal(uint64(9), c.Length())

	is.Equal(c.Length(), uint64(len(c.Positions())))

}
