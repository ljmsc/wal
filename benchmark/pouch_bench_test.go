package benchmark

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/ljmsc/wal/pouch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var benchTestDir = "../tmp/pouch_bench/"

func benchmarkPouchWrite(loops int, b *testing.B) {
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	pou, err := pouch.Open(benchTestDir + "test_bench_write")
	require.NoError(b, err)
	defer pou.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < loops; j++ {
			_, err := pou.Write([]byte("key_"+strconv.Itoa((i+1)+j)), []byte("data_"+strconv.Itoa((i+1)+j)))
			assert.NoError(b, err)
		}

	}
}

func BenchmarkPouchWrite1(b *testing.B) {
	benchmarkPouchWrite(1, b)
}

func BenchmarkPouchWrite100(b *testing.B) {
	benchmarkPouchWrite(100, b)
}

func BenchmarkPouchWrite1000(b *testing.B) {
	benchmarkPouchWrite(1000, b)
}

func benchmarkPouchRead(loops int, b *testing.B) {
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	pou, err := pouch.Open(benchTestDir + "test_bench_read")
	require.NoError(b, err)
	defer pou.Close()

	for i := 0; i < loops; i++ {
		_, err := pou.Write([]byte("key_"+strconv.Itoa(i)), []byte("data_"+strconv.Itoa(i)))
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < loops; j++ {
			randKeyIndex := rand.Intn(loops)
			r := pouch.Record{}
			err := pou.ReadByKey([]byte("key_"+strconv.Itoa(randKeyIndex)), false, &r)
			assert.NoError(b, err)
		}
	}
}

func BenchmarkPouchRead1(b *testing.B) {
	benchmarkPouchRead(1, b)
}

func BenchmarkPouchRead100(b *testing.B) {
	benchmarkPouchRead(100, b)
}

func BenchmarkPouchRead1000(b *testing.B) {
	benchmarkPouchRead(1000, b)
}

func benchmarkPouchReadHeadOnly(loops int, b *testing.B) {
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	pou, err := pouch.Open(benchTestDir + "test_bench_read")
	require.NoError(b, err)
	defer pou.Close()

	for i := 0; i < loops; i++ {
		_, err := pou.Write([]byte("key_"+strconv.Itoa(i)), []byte("data_"+strconv.Itoa(i)))
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < loops; j++ {
			randKeyIndex := rand.Intn(loops)
			r := pouch.Record{}
			err := pou.ReadByKey([]byte("key_"+strconv.Itoa(randKeyIndex)), true, &r)
			assert.NoError(b, err)
		}
	}
}

func BenchmarkPouchReadHeadOnly1(b *testing.B) {
	benchmarkPouchReadHeadOnly(1, b)
}

func BenchmarkPouchReadHeadOnly100(b *testing.B) {
	benchmarkPouchReadHeadOnly(100, b)
}

func BenchmarkPouchReadHeadOnly1000(b *testing.B) {
	benchmarkPouchReadHeadOnly(1000, b)
}
