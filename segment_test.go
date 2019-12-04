package wal

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func createHelper(prefix string) segment {
	s, e := createSegment(1, 1, Config{
		SegmentMaxSizeBytes: 100e6,
		SegmentFileDir:      "./tmp",
		SegmentFilePrefix:   prefix,
	})
	if e != nil {
		panic(e)
	}
	return s
}

func removeHelper(prefix string) {
	filename := "./tmp/" + prefix + "1"
	if err := os.Remove(filename); err != nil {
		panic(err)
	}
}

func TestCreateSegment(t *testing.T) {
	fileDir := "./tmp"
	filename := "seg1"
	filePath := fileDir + "/" + filename
	s, e := createSegment(1, 1, Config{
		SegmentMaxSizeBytes: 100e6,
		SegmentFileDir:      fileDir,
		SegmentFilePrefix:   "seg",
	})
	if e != nil {
		t.Fatalf("can't create new segmentFile - %v", e)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("couldn't close segmentFile file - %v", err)
	}

	_, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("couldn't fetch file info - %v", err)
	}

	if err := os.Remove(filePath); err != nil {
		t.Fatalf("couldn't remove test segmentFile file - %v", err)
	}
}

func TestSegment_WriteReadRecords(t *testing.T) {
	s := createHelper("read_test")
	defer s.Close()
	defer removeHelper("read_test")
	loops := 10

	offsets := make([]int64, 0, loops)
	for i := 0; i < loops; i++ {
		record := Record{
			meta: recordMetadata{
				sequenceNumber: uint64(i + 1),
				version:        1,
			},
			Key:  []byte(strconv.Itoa(i)),
			Data: []byte("mydata"),
		}

		if err := s.Write(&record); err != nil {
			t.Fatalf("couldn't write to segmentFile file - %v", err)
		}
		offsets = append(offsets, record.Offset())
	}

	if len(offsets) != loops {
		t.Fatalf("offset slice has wrong size")
	}

	for i := 0; i < loops; i++ {
		record := Record{}
		if err := s.ReadOffset(offsets[i], &record); err != nil {
			t.Fatalf("couldn't read record for offset: %d - %v", offsets[i], err)
		}
		if string(record.Key) != strconv.Itoa(i) {
			t.Errorf("wrong key for record. got '%s' instead of '%d'", string(record.Key), i)
		}
	}

	for i := 0; i < loops; i++ {
		key := []byte(strconv.Itoa(i))
		record := Record{}
		if err := s.ReadLatest(key, &record); err != nil {
			t.Errorf("can't read record for key: '%s' - %v", key, err)
		}
	}
}

func TestSegment_OffsetsForKey(t *testing.T) {
	s := createHelper("offsets_test")
	defer s.Close()
	defer removeHelper("offsets_test")

	testkey := []byte("test")
	loops := 10
	for i := 0; i < loops; i++ {
		record := Record{
			meta: recordMetadata{
				sequenceNumber: uint64(i + 1),
				version:        uint64(i + 1),
			},
			Key:  testkey,
			Data: []byte("testdata" + strconv.FormatInt(rand.Int63(), 10)),
		}
		if err := s.Write(&record); err != nil {
			t.Fatalf("can't write record in loop step %d - %v", i, err)
		}
	}

	offsets, err := s.Offsets(testkey)
	if err != nil {
		t.Fatalf("can't get offsets - %v", err)
	}

	if len(offsets) != loops {
		t.Fatalf("wrong amount of offset items. got %d instead of %d", len(offsets), loops)
	}
}
