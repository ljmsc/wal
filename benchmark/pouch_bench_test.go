package benchmark

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/ljmsc/wal/pouch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	benchTestDir       = "../tmp/pouch_bench/"
	benchBasicTestDir  = "../tmp/basic_bench/"
	benchBasicTestFile = "../tmp/basic_bench/basic.tmp"

	writeLoops = 100
)

func BenchmarkKeyHash(b *testing.B) {
	testKey := pouch.Key("key_test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = testKey.Hash()
	}
}

func BenchmarkBasicWrite(b *testing.B) {
	prepare(benchBasicTestDir)
	defer cleanup(benchBasicTestDir)

	basicFile, err := os.Create(benchBasicTestFile)
	require.NoError(b, err)
	defer basicFile.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := basicFile.Write([]byte("key_" + strconv.Itoa(i+1) + "_" + "data_" + strconv.Itoa(i+1)))
		if err != nil {
			assert.NoError(b, err)
		}
	}
}

func BenchmarkPouchWrite(b *testing.B) {
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	pou, err := pouch.Open(benchTestDir + "test_bench_write")
	require.NoError(b, err)
	defer pou.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pou.Write([]byte("key_"+strconv.Itoa(i+1)), []byte("data_"+strconv.Itoa(i+1)))
		if err != nil {
			assert.NoError(b, err)
		}
	}
}

func BenchmarkBasicRead(b *testing.B) {
	prepare(benchBasicTestDir)
	defer cleanup(benchBasicTestDir)

	basicFile, err := os.OpenFile(benchBasicTestFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	//basicFile, err := os.Create(benchBasicTestFile)
	require.NoError(b, err)
	defer basicFile.Close()

	offsets := make([]int64, 0, writeLoops)
	for j := 0; j < writeLoops; j++ {
		offset, _ := basicFile.Seek(0, 1)
		data := []byte("key_" + strconv.Itoa(j) + "_" + "data_" + strconv.Itoa(j))
		_, err := basicFile.Write(data)
		assert.NoError(b, err)

		err = basicFile.Sync()
		assert.NoError(b, err)
		offsets = append(offsets, offset)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randOffset := int64(rand.Intn(len(offsets) - 1))
		data := make([]byte, 12)
		_, err := basicFile.ReadAt(data, randOffset)
		if err != nil {
			assert.NoError(b, err)
		}
	}
}

func BenchmarkPouchRead(b *testing.B) {
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	pou, err := pouch.Open(benchTestDir + "test_bench_read")
	require.NoError(b, err)
	defer pou.Close()

	for i := 0; i < writeLoops; i++ {
		_, err := pou.Write([]byte("key_"+strconv.Itoa(i)), []byte("data_"+strconv.Itoa(i)))
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randKeyIndex := rand.Intn(writeLoops)
		r := pouch.Record{}
		err := pou.ReadByKey([]byte("key_"+strconv.Itoa(randKeyIndex)), false, &r)
		if err != nil {
			assert.NoError(b, err)
		}
	}
}

func BenchmarkPouchReadHeadOnly(b *testing.B) {
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	pou, err := pouch.Open(benchTestDir + "test_bench_read")
	require.NoError(b, err)
	defer pou.Close()

	for i := 0; i < writeLoops; i++ {
		_, err := pou.Write([]byte("key_"+strconv.Itoa(i)), []byte("data_"+strconv.Itoa(i)))
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randKeyIndex := rand.Intn(writeLoops)
		r := pouch.Record{}
		err := pou.ReadByKey([]byte("key_"+strconv.Itoa(randKeyIndex)), true, &r)
		if err != nil {
			assert.NoError(b, err)
		}
	}
}

func BenchmarkPouchReadByOffset(b *testing.B) {
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	pou, err := pouch.Open(benchTestDir + "test_bench_read")
	require.NoError(b, err)
	defer pou.Close()

	offsets := make([]int64, 0, writeLoops)
	for i := 0; i < writeLoops; i++ {
		offset, err := pou.Write([]byte("key_"+strconv.Itoa(i)), []byte("data_"+strconv.Itoa(i)))
		offsets = append(offsets, offset)
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randIndex := rand.Intn(writeLoops)
		r := pouch.Record{}
		err := pou.ReadByOffset(offsets[randIndex], false, &r)
		if err != nil {
			assert.NoError(b, err)
		}
	}
}
