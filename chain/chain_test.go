package chain

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var bucketTestDir = "../tmp/chain/"

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

func createTestBucket(name string, dir string) Chain {
	b, err := OpenWithSize(dir+name, 300)
	if err != nil {
		panic(err)
	}
	if b == nil {
		panic("chain " + name + " is nil")
	}
	return b
}

func TestBucketOpen(t *testing.T) {
	prepare(bucketTestDir)
	defer cleanup(bucketTestDir)
	c, err := OpenWithHandler(bucketTestDir+"chain_open", true, nil)

	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotNil(t, c) {
		return
	}

	err = c.Close()
	assert.NoError(t, err)
}

func TestBucketOpenWriteRead(t *testing.T) {
	prepare(bucketTestDir)
	defer cleanup(bucketTestDir)
	c, err := OpenWithSize(bucketTestDir+"chain_read_write", 300)

	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotNil(t, c) {
		return
	}
	for i := 1; i <= 30; i++ {
		testKey := []byte("chain_test_key_" + strconv.Itoa(i))
		testData := []byte("chain_test_data_" + strconv.Itoa(i))
		err := c.Write(testKey, testData)
		assert.NoError(t, err)
	}

	for i := 1; i <= 30; i++ {
		testKey := []byte("chain_test_key_" + strconv.Itoa(i))
		r := Record{}
		err := c.ReadBySequenceNumber(uint64(i), false, &r)
		if assert.NoError(t, err) {
			assert.EqualValues(t, testKey, r.Key)
			assert.EqualValues(t, uint64(i), r.SeqNum())
		}
	}

	err = c.Close()
	if !assert.NoError(t, err) {
		return
	}

	c2, err := OpenWithSize(bucketTestDir+"chain_read_write", 300)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotNil(t, c2) {
		return
	}

	for i := 1; i <= 30; i++ {
		testKey := []byte("chain_test_key_" + strconv.Itoa(i))
		r := Record{}
		err := c2.ReadBySequenceNumber(uint64(i), false, &r)
		if assert.NoError(t, err) {
			assert.EqualValues(t, testKey, r.Key)
			assert.EqualValues(t, uint64(i), r.SeqNum())
		}
	}

	for i := 1; i <= 30; i++ {
		testKey := []byte("chain_test_key_" + strconv.Itoa(i))
		testData := []byte("chain_test_data_" + strconv.Itoa(i))
		r := Record{}
		err := c2.ReadByKey(testKey, false, &r)
		if assert.NoError(t, err) {
			assert.EqualValues(t, testKey, r.Key)
			assert.EqualValues(t, uint64(i), r.SeqNum())
			assert.EqualValues(t, testData, r.Data)
		}
	}

	err = c2.Close()
	assert.NoError(t, err)
}

func TestBucketStreamLatestRecords(t *testing.T) {
	prepare(bucketTestDir)
	defer cleanup(bucketTestDir)
	b := createTestBucket("segment_test2", bucketTestDir)
	defer b.Close()

	for i := 1; i <= 20; i++ {
		keySuffix := ((i - 1) % 5) + 1
		testKey := []byte("test_key_" + strconv.Itoa(keySuffix))
		testData := []byte("test_data_" + strconv.Itoa(i))

		err := b.Write(testKey, testData)
		assert.NoError(t, err)
	}

	counter := 0
	stream := b.StreamLatestRecords(true)
	for {
		item, ok := <-stream
		if !ok {
			break
		}
		if !assert.NoError(t, item.Err) {
			continue
		}
		counter++
	}
	assert.EqualValues(t, 5, counter)
}

func TestCompressBucket(t *testing.T) {
	prepare(bucketTestDir)
	defer cleanup(bucketTestDir)
	b := createTestBucket("compressor", bucketTestDir)
	defer b.Close()

	for i := 1; i <= 200; i++ {
		keySuffix := ((i - 1) % 5) + 1
		testKey := []byte("test_key_" + strconv.Itoa(keySuffix))
		testData := []byte("test_data_" + strconv.Itoa(i))

		err := b.Write(testKey, testData)
		assert.NoError(t, err)
	}

	sizeBefore, err := b.Size()

	assert.NoError(t, err)
	err = b.Compress()
	assert.NoError(t, err)
	sizeAfter, err := b.Size()
	assert.NoError(t, err)

	assert.LessOrEqual(t, sizeAfter, sizeBefore)
}

func TestDumpBucket(t *testing.T) {
	prepare(bucketTestDir)
	defer cleanup(bucketTestDir)
	b := createTestBucket("dump", bucketTestDir)
	defer b.Close()

	for i := 1; i <= 20; i++ {
		testKey := []byte("test_key_" + strconv.Itoa(i))
		testData := []byte("test_data_" + strconv.Itoa(i))

		err := b.Write(testKey, testData)
		assert.NoError(t, err)
	}

	seqNumbers := b.LatestSequenceNumbers()
	assert.EqualValues(t, 20, len(seqNumbers))

	dumpNum := seqNumbers[10]

	err := b.Dump(dumpNum)
	assert.NoError(t, err)

	r := Record{}
	err = b.ReadBySequenceNumber(dumpNum, true, &r)
	assert.EqualError(t, err, RecordNotFoundErr.Error())

	r2 := Record{}
	err = b.ReadBySequenceNumber(b.LatestSequenceNumber(), true, &r2)
	assert.Error(t, err)

	seqNumbers2 := b.LatestSequenceNumbers()
	assert.EqualValues(t, 10, len(seqNumbers2))
}
