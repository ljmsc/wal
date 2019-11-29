package wal

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestRecord_FromBytes(t *testing.T) {
	sequenceNumber := uint64(3)
	key := "13"
	data := "this is test dataBytes"

	dataBytes := make([]byte, 0, 16)

	sequenceNumberBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sequenceNumberBytes, sequenceNumber)
	dataBytes = append(dataBytes, sequenceNumberBytes...)

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

	if r.Meta.SequenceNumber != sequenceNumber {
		t.Fatalf("sequence number is not correct. got %d instead of %d", r.Meta.SequenceNumber, sequenceNumber)
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
	recordSize := 35
	r := Record{
		Meta: RecordMetadata{
			SequenceNumber: 1,
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

	if !reflect.DeepEqual(r, r2) {
		t.Fatalf("records are not equal: %v != %v", r, r2)
	}

}
