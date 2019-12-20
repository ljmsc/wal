package wal

import (
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	if err := os.MkdirAll("./tmp/wal/", os.ModeDir|0744); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func bootstrapHelper(config Config) Wal {
	wal, err := Bootstrap(config)
	if err != nil {
		panic(err)
	}
	return wal
}

func closingHelper(wal Wal) {
	if wal == nil {
		return
	}
	if err := wal.Remove(); err != nil {
		panic(err)
	}
}

func writeHelper(wal Wal, records int, versions int, data string) {
	for j := 0; j < versions; j++ {
		for i := 0; i < records; i++ {
			record := Record{
				Key:  []byte("key:" + strconv.Itoa(i)),
				Data: []byte(data),
			}
			if err := wal.Write(&record); err != nil {
				panic(err)
			}
		}
	}
}

func TestBootstrap(t *testing.T) {
	wal, err := Bootstrap(Config{
		SegmentFileDir: "./tmp/wal/",
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
		SegmentFileDir:    "./tmp/wal/",
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

		assert.Equal(t, uint64(1), record.Version(), "wrong version for record")
	}

	record := Record{Key: []byte("key:1")}
	if err := wal.CompareAndWrite(1, &record); err != nil {
		t.Errorf("can't write record via CompareAndWrite function: %v", err)
	}

	if err := wal.CompareAndWrite(5, &record); err == nil {
		t.Errorf("write record with CompareAndWrite with wrong version")
	}

	for i := 0; i < loops; i++ {
		key := []byte("key:" + strconv.Itoa(i))
		if err := wal.Delete(key); err != nil {
			t.Errorf("can't delete record with key: '%v' - %v", key, err)
		}
	}
}

func TestWriteReadWalMultiSegment(t *testing.T) {
	records := 10
	versions := 3
	testData := "this is awesome test data"
	wal := bootstrapHelper(Config{
		SegmentMaxSizeBytes: 210,
		SegmentFileDir:      "./tmp/wal/",
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

		assert.NoErrorf(t, err, "can't read all records for key: '%v'", key)

		assert.Equalf(t, 3, len(records), "not enough items for key: %v - only %d items", key, len(records))

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

func TestBootstrapExistingWal(t *testing.T) {
	loops := 10
	config := Config{
		SegmentMaxSizeBytes: 210,
		SegmentFileDir:      "./tmp/wal/",
		SegmentFilePrefix:   "seg_exist",
	}
	wal := bootstrapHelper(config)

	for i := 0; i < loops; i++ {
		record := Record{
			Key:  []byte("key:" + strconv.Itoa(i)),
			Data: []byte("this is awesome data"),
		}
		if err := wal.Write(&record); err != nil {
			t.Errorf("can't write to DiskWal - %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("can't close wal - %v", err)
	}

	wal2, err := Bootstrap(config)
	if err != nil {
		t.Fatalf("can't bootstrap wal2 - %v", err)
	}
	defer closingHelper(wal2)

	for i := 0; i < loops; i++ {
		record := Record{}
		key := []byte("key:" + strconv.Itoa(i))
		if err := wal2.ReadLatest(key, &record); err != nil {
			t.Errorf("can't read data from DiskWal for key: '%s' - %v", key, err)
		}
	}
}

func TestCompactionManually(t *testing.T) {
	records := 9
	versions := 3
	testData := "this is awesome test data"
	wal := bootstrapHelper(Config{
		Compaction: CompactionConfig{
			Trigger:    TriggerManually,
			Strategy:   StrategyKeep,
			KeepAmount: 1,
		},
		SegmentMaxSizeBytes: 210,
		SegmentFileDir:      "./tmp/wal/",
		SegmentFilePrefix:   "seg_comp_rw",
	})
	defer closingHelper(wal)
	writeHelper(wal, records, versions, testData)

	segAmount := wal.SegmentAmount()

	if err := wal.Compact(); err != nil {
		t.Fatalf("can't compact wal: %v", err)
	}

	if wal.SegmentAmount() >= segAmount {
		t.Fatalf("to many segment files in wal after compaction: %d", wal.SegmentAmount())
	}

	for i := 0; i < records; i++ {
		key := []byte("key:" + strconv.Itoa(i))
		record := Record{}
		if err := wal.ReadLatest(key, &record); err != nil {
			t.Errorf("can't read record for key '%s' : %v", key, err)
		}

		if record.Version() < uint64(versions) {
			t.Errorf("wrong version for record. got %d instead if %d", record.Version(), versions)
		}
	}

	writeHelper(wal, records, 1, testData)

	for i := 0; i < records; i++ {
		key := []byte("key:" + strconv.Itoa(i))
		record := Record{}
		if err := wal.ReadLatest(key, &record); err != nil {
			t.Errorf("can't read record for key '%s' : %v", key, err)
		}

		if record.Version() < uint64(versions)+1 {
			t.Errorf("wrong version for record. got %d instead if %d", record.Version(), versions+1)
		}
	}
}

func TestCompactionTriggerTime(t *testing.T) {
	records := 9
	versions := 3
	testData := "this is awesome test data"
	wal := bootstrapHelper(Config{
		Compaction: CompactionConfig{
			Trigger:         TriggerTime,
			Strategy:        StrategyKeep,
			KeepAmount:      1,
			TriggerInterval: time.Second,
		},
		SegmentMaxSizeBytes: 210,
		SegmentFileDir:      "./tmp/wal/",
		SegmentFilePrefix:   "seg_comp_rw",
	})
	defer closingHelper(wal)
	writeHelper(wal, records, versions, testData)
	segAmount := wal.SegmentAmount()
	time.Sleep(time.Second)

	if wal.SegmentAmount() >= segAmount {
		t.Fatalf("to many segment files in wal after compaction: %d", wal.SegmentAmount())
	}
}

func TestCompactionStrategyExpire(t *testing.T) {
	records := 9
	versions := 3
	testData := "this is awesome test data"
	wal := bootstrapHelper(Config{
		Compaction: CompactionConfig{
			Trigger:             TriggerManually,
			Strategy:            StrategyExpire,
			ExpirationThreshold: time.Second,
		},
		SegmentMaxSizeBytes: 210,
		SegmentFileDir:      "./tmp/wal/",
		SegmentFilePrefix:   "seg_comp_rw",
	})
	defer closingHelper(wal)
	writeHelper(wal, records, versions, testData)
	segAmount := wal.SegmentAmount()

	time.Sleep(time.Second)
	if err := wal.Compact(); err != nil {
		t.Fatalf("can't compact wal: %v", err)
	}

	if wal.SegmentAmount() >= segAmount {
		t.Fatalf("to many segment files in wal after compaction: %d", wal.SegmentAmount())
	}

	for i := 0; i < records; i++ {
		key := []byte("key:" + strconv.Itoa(i))
		record := Record{}
		if err := wal.ReadLatest(key, &record); err != nil {
			if !errors.Is(err, ErrNoRecordFound) {
				t.Errorf("can't read record for key '%s' : %v", key, err)
			}
		}
	}

	if wal.SegmentAmount() > 1 {
		t.Errorf("to many segment files: %d", wal.SegmentAmount())
	}

	writeHelper(wal, records, 1, testData)
	if err := wal.Compact(); err != nil {
		t.Fatalf("can't compact wal: %v", err)
	}

	for i := 0; i < records; i++ {
		key := []byte("key:" + strconv.Itoa(i))
		record := Record{}
		if err := wal.ReadLatest(key, &record); err != nil {
			t.Errorf("can't read record for key '%s' : %v", key, err)
		}

		if record.Version() < uint64(versions)+1 {
			t.Errorf("wrong version for record. got %d instead if %d", record.Version(), versions+1)
		}
	}
}
