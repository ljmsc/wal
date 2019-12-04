package wal

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestRecord_FromBytes(t *testing.T) {
	sequenceNumber := uint64(3)
	version := uint64(1)
	timestamp := uint64(time.Now().UnixNano())
	key := "13"
	data := "this is test dataBytes"

	dataBytes := make([]byte, 0, 16)

	sequenceNumberBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sequenceNumberBytes, sequenceNumber)
	dataBytes = append(dataBytes, sequenceNumberBytes...)

	versionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBytes, version)
	dataBytes = append(dataBytes, versionBytes...)

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, timestamp)
	dataBytes = append(dataBytes, timestampBytes...)

	keyBytes := []byte(key)
	keyLengthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyLengthBytes, uint64(len(keyBytes)))
	dataBytes = append(dataBytes, keyLengthBytes...)

	dataBytes = append(dataBytes, keyBytes...)
	dataBytes = append(dataBytes, []byte(data)...)

	r := Record{}
	if err := r.FromBytes(dataBytes); err != nil {
		t.Fatalf("could not convert to record: %s", err.Error())
	}

	if r.meta.sequenceNumber != sequenceNumber {
		t.Fatalf("sequence number is not correct. got %d instead of %d", r.meta.sequenceNumber, sequenceNumber)
	}

	recordKey := string(r.Key)
	if recordKey != key {
		t.Fatalf("key is not correct. got '%s' instead of '%s'", recordKey, key)
	}

	recordData := string(r.Data)
	if recordData != data {
		t.Fatalf("data is not correct. got '%s' instead of '%s'", recordData, data)
	}
}

func TestRecord_ToBytes(t *testing.T) {
	recordSize := 51
	r := Record{
		meta: recordMetadata{
			sequenceNumber: 1,
			version:        1,
			createdAt:      time.Now(),
		},
		Key:  []byte("42"),
		Data: []byte("this is test data"),
	}
	data := r.ToBytes()
	if len(data) > recordSize {
		t.Fatalf("record byte slice is to large: %d", len(data))
	}

	if len(data) < recordSize {
		t.Fatalf("record byte slice is to small: %d", len(data))
	}

	r2 := Record{}
	if err := r2.FromBytes(data); err != nil {
		t.Fatalf("can't transfer data back to record: %s", err.Error())
	}

	if r.Size() != r2.Size() {
		t.Fatalf("records are not equal: %v != %v", r, r2)
	}

}
