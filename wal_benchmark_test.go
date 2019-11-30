package wal

import (
	"strconv"
	"testing"
)

func prepareReadWal(config Config, amount int) Wal {
	wal := bootstrapHelper(config)

	for i := 0; i < amount; i++ {
		record := Record{
			Key:  []byte("key:" + strconv.Itoa(i)),
			Data: []byte("this is awesome data"),
		}
		if err := wal.Write(&record); err != nil {
			panic(err)
		}
	}
	return wal
}

func BenchmarkSegment_Write(b *testing.B) {
	wal := bootstrapHelper(Config{
		SegmentFileDir:    "./tmp/wal/",
		SegmentFilePrefix: "seg_bench",
	})
	defer closingHelper(wal)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := Record{
			Key:  []byte("testkey" + strconv.Itoa(i)),
			Data: []byte("test data 123"),
		}
		if err := wal.Write(&record); err != nil {
			b.Fatalf("can't write data - %v", err)
		}
	}
}

func BenchmarkSegment_ReadLatest(b *testing.B) {
	loops := 100
	wal := prepareReadWal(Config{
		SegmentMaxSizeBytes: 1000,
		SegmentFileDir:      "./tmp/wal/",
		SegmentFilePrefix:   "seg_bench_readlatest",
	}, loops)

	defer closingHelper(wal)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := Record{}
		key := []byte("key:" + strconv.Itoa(i%loops))
		if err := wal.ReadLatest(key, &record); err != nil {
			b.Errorf("can't read data from DiskWal for key: '%s' - %v", key, err)
		}
	}
}

func BenchmarkSegment_ReadAll(b *testing.B) {
	loops := 100
	wal := prepareReadWal(Config{
		SegmentMaxSizeBytes: 1000,
		SegmentFileDir:      "./tmp/wal/",
		SegmentFilePrefix:   "seg_bench_readall",
	}, loops)

	defer closingHelper(wal)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key:" + strconv.Itoa(i%loops))
		_, err := wal.ReadAll(key)
		if err != nil {
			b.Errorf("can't read data from DiskWal for key: '%s' - %v", key, err)
		}
	}
}

func BenchmarkSegment_ReadSequenceNum(b *testing.B) {
	loops := 100
	wal := prepareReadWal(Config{
		SegmentMaxSizeBytes: 1000,
		SegmentFileDir:      "./tmp/wal/",
		SegmentFilePrefix:   "seg_bench_readseq",
	}, loops)

	defer closingHelper(wal)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := Record{}
		if err := wal.ReadSequenceNum(uint64(i%loops), &record); err != nil {
			b.Errorf("can't read data from DiskWal - %v", err)
		}
	}
}
