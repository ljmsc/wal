package wal

import (
	"strconv"
	"testing"
)

func bootstrapHelper(config Config) Wal {
	wal, err := Bootstrap(config)
	if err != nil {
		panic(err)
	}
	return wal
}

func closingHelper(wal Wal) {
	if err := wal.Remove(); err != nil {
		panic(err)
	}
}

func TestBootstrap(t *testing.T) {
	wal, err := Bootstrap(Config{
		SegmentFileDir: "./tmp/DiskWal/",
	})

	if err != nil {
		t.Fatalf("can't create new log - %v", err)
	}

	if err := wal.Remove(); err != nil {
		t.Fatalf("can't close DiskWal - %v", err)
	}
}

func TestWriteReadWal(t *testing.T) {
	loops := 10
	wal := bootstrapHelper(Config{
		SegmentFileDir:    "./tmp/DiskWal/",
		SegmentFilePrefix: "seg_rw",
	})
	defer closingHelper(wal)

	for i := 0; i < loops; i++ {
		record := Record{
			Key:  []byte("key:" + strconv.Itoa(i)),
			Data: []byte("this is awesome data"),
		}
		if err := wal.Write(&record); err != nil {
			t.Errorf("can't write to DiskWal - %v", err)
		}
	}

	for i := 0; i < loops; i++ {
		record := Record{}
		key := []byte("key:" + strconv.Itoa(i))
		if err := wal.ReadLatest(key, &record); err != nil {
			t.Errorf("can't read data from DiskWal for key: '%s' - %v", key, err)
		}
	}
}

func TestWriteReadWalMultiSegment(t *testing.T) {
	records := 10
	versions := 3
	testData := "this is awesome test data"
	wal := bootstrapHelper(Config{
		SegmentMaxSizeBytes: 210,
		SegmentFileDir:      "./tmp/DiskWal/",
		SegmentFilePrefix:   "mul_seg_rw",
	})
	defer closingHelper(wal)

	for j := 0; j < versions; j++ {
		for i := 0; i < records; i++ {
			record := Record{
				Key:  []byte("key:" + strconv.Itoa(i)),
				Data: []byte(testData),
			}
			if err := wal.Write(&record); err != nil {
				t.Errorf("can't write to DiskWal - %v", err)
			}
		}
	}

	sequenceNum := make(map[string]uint64)
	for i := 0; i < records; i++ {
		key := "key:" + strconv.Itoa(i)
		records, err := wal.ReadAll([]byte(key))
		if err != nil {
			t.Errorf("can't read all records for key: '%v' - %v", key, err)
		}

		if len(records) != 3 {
			t.Errorf("not enough items for key: %v - only %d items", key, len(records))
		}
		for _, record := range records {
			if string(record.Data) != testData {
				t.Errorf("wrong data. got %s instead of %s", string(record.Data), testData)
			}
		}
		sequenceNum[key] = records[1].meta.sequenceNumber
	}

	for i := 0; i < records; i++ {
		key := "key:" + strconv.Itoa(i)
		record := Record{}
		if err := wal.ReadSequenceNum(sequenceNum[key], &record); err != nil {
			t.Errorf("can't read record from sequence number %d - %v", sequenceNum[key], err)
		}
	}
}

func TestRecordVersion(t *testing.T) {
	//todo: impl when record versions are implemented
}

func TestDeleteRecords(t *testing.T) {
	//todo: impl when log compaction is implemented
}
