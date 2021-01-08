package wal

import (
	"encoding/binary"
	"testing"

	"github.com/ljmsc/wal/segment"

	"github.com/stretchr/testify/assert"
)

func TestCreateEntry(t *testing.T) {
	testKey := []byte("test_key")
	testData := []byte("test_data")

	entry := CreateEntry(testKey, testData, segment.MetaRecord{
		Name: "test1",
		Data: nil,
	})

	assert.EqualValues(t, testKey, entry.Key)
	assert.EqualValues(t, testData, entry.Data)
	assert.EqualValues(t, 1, len(entry.Metadata))
}

func TestSetEntryVersion(t *testing.T) {
	testKey := []byte("test_key")
	testData := []byte("test_data")
	entry := CreateEntry(testKey, testData)
	setVersion(2, entry)

	assert.EqualValues(t, 2, entry.Version())
}

func TestRecordToEntryToRecord(t *testing.T) {
	testKey := []byte("test_key")
	testData := []byte("test_data")
	testSeqNum := uint64(42)
	testVersion := uint64(2)
	seqNumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqNumBytes, testSeqNum)

	versionBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(versionBytes, testVersion)

	record := chain.CreateRecord(testKey, testData,
		segment.MetaRecord{
			Name: chain.SequenceNumberMetadataKey,
			Data: seqNumBytes,
		}, segment.MetaRecord{
			Name: VersionMetadataKey,
			Data: versionBytes,
		},
	)

	entry := Entry{}
	err := recordToEntry(*record, &entry)
	assert.NoError(t, err)

	assert.EqualValues(t, testKey, entry.Key, "key")
	assert.EqualValues(t, testData, entry.Data, "data")
	assert.EqualValues(t, testSeqNum, entry.SequenceNumber(), "sequence number")
	assert.EqualValues(t, testVersion, entry.Version(), "version")

	record2 := chain.Record{}
	entry.toRecord(&record2)

	assert.EqualValues(t, testKey, record2.Key)
	assert.EqualValues(t, testData, record2.Data)
	assert.EqualValues(t, testSeqNum, record2.SequenceNumber())
}

func TestEntryVersion(t *testing.T) {
	testKey := []byte("test_key")
	testData := []byte("test_data")
	testVersion := uint64(2)

	seqNumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqNumBytes, 42)

	entry := CreateEntry(testKey, testData, segment.MetaRecord{
		Name: chain.SequenceNumberMetadataKey,
		Data: seqNumBytes,
	})

	assert.Error(t, entry.Validate())

	setVersion(testVersion, entry)

	assert.NoError(t, entry.Validate())
}
