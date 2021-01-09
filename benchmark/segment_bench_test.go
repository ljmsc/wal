package benchmark

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/matryer/is"

	"github.com/ljmsc/wal/segment"
)

const (
	benchTestDir       = "../tmp/segment_bench/"
	benchBasicTestDir  = "../tmp/basic_bench/"
	benchBasicTestFile = "../tmp/basic_bench/basic.tmp"

	writeLoops = 100
)

func BenchmarkWriteBasic(b *testing.B) {
	is := is.New(b)
	prepare(benchBasicTestDir)
	defer cleanup(benchBasicTestDir)

	basicFile, err := os.Create(benchBasicTestFile)
	is.NoErr(err)
	defer basicFile.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := basicFile.Write([]byte("key_" + strconv.Itoa(i+1) + "_" + "data_" + strconv.Itoa(i+1)))
		if err != nil {
			is.NoErr(err)
		}
		err = basicFile.Sync()
		if err != nil {
			is.NoErr(err)
		}
	}
}

func BenchmarkWriteSegment(b *testing.B) {
	is := is.New(b)
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	_seg, err := segment.Open(benchTestDir+"test_bench_write", nil)
	is.NoErr(err)
	defer _seg.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := segment.CreateRecord(uint64(i+1), []byte("data_"+strconv.Itoa(i+1)))
		_, err := _seg.Write(r)
		if err != nil {
			is.NoErr(err)
		}
	}
}

func BenchmarkReadBasic(b *testing.B) {
	is := is.New(b)
	prepare(benchBasicTestDir)
	defer cleanup(benchBasicTestDir)

	basicFile, err := os.OpenFile(benchBasicTestFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	is.NoErr(err)
	defer basicFile.Close()

	offsets := make([]int64, 0, writeLoops)
	for j := 0; j < writeLoops; j++ {
		offset, _ := basicFile.Seek(0, 1)
		data := []byte("key_" + strconv.Itoa(j) + "_" + "data_" + strconv.Itoa(j))
		_, err := basicFile.Write(data)
		if err != nil {
			is.NoErr(err)
		}

		err = basicFile.Sync()
		if err != nil {
			is.NoErr(err)
		}
		offsets = append(offsets, offset)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randOffset := int64(rand.Intn(len(offsets) - 1))
		data := make([]byte, 12)
		_, err := basicFile.ReadAt(data, randOffset)
		if err != nil {
			is.NoErr(err)
		}
	}
}

func BenchmarkReadSegment(b *testing.B) {
	is := is.New(b)
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	seg, err := segment.Open(benchTestDir+"test_bench_read", nil)
	is.NoErr(err)
	defer seg.Close()

	for i := 0; i < writeLoops; i++ {
		r := segment.CreateRecord(uint64(i+1), []byte("data_"+strconv.Itoa(i+1)))
		_, err := seg.Write(r)
		is.NoErr(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randKeyIndex := rand.Intn(writeLoops) + 1
		r := segment.CreateRecord(0, []byte{})
		err := seg.ReadKey(r, uint64(randKeyIndex))
		if err != nil {
			is.NoErr(err)
		}
	}
}

func BenchmarkReadByOffsetSegment(b *testing.B) {
	is := is.New(b)
	prepare(benchTestDir)
	defer cleanup(benchTestDir)
	seg, err := segment.Open(benchTestDir+"test_bench_read", nil)
	is.NoErr(err)
	defer seg.Close()

	offsets := make([]int64, 0, writeLoops)
	for i := 0; i < writeLoops; i++ {
		r := segment.CreateRecord(uint64(i+1), []byte("data_"+strconv.Itoa(i)))
		offset, err := seg.Write(r)
		offsets = append(offsets, offset)
		is.NoErr(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randIndex := rand.Intn(len(offsets) - 1)
		r := segment.CreateRecord(0, []byte{})
		err := seg.ReadAt(r, offsets[randIndex])
		if err != nil {
			is.NoErr(err)
		}
	}
}
