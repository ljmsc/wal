package segment

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateRecord(t *testing.T) {
	metadata := Metadata(make(map[string][]byte))
	metadata["metakey1"] = []byte("metadata1")
	metadata["metakey2"] = []byte("metadata2")
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	record := CreateRecordWithMetadata(metadata, key, data)

	assert.NotNil(t, record)
	assert.EqualValues(t, key, record.Key)
	assert.EqualValues(t, data, record.Data)

	record2 := CreateRecord(key, data)

	assert.EqualValues(t, key, record2.Key)
}

func TestRecordSize(t *testing.T) {
	metadata := Metadata(make(map[string][]byte))
	metadata["metakey1"] = []byte("metadata1")
	metadata["metakey2"] = []byte("metadata2")
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	record := CreateRecordWithMetadata(metadata, key, data)

	assert.EqualValues(t, len(record.HeaderBytes()), record.HeaderSize())
	assert.EqualValues(t, len(record.Bytes()), record.Size())
}

func TestParseRecord(t *testing.T) {
	metadataTest := Metadata(make(map[string][]byte))
	metadataTest["metakey1"] = []byte("metadata1")
	metadataTest["metakey2"] = []byte("metadata2")
	metadata := Metadata(make(map[string][]byte))
	metadata["metakey1"] = []byte("metadata1")
	metadata["metakey2"] = []byte("metadata2")
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	metadataSizeBytes := make([]byte, MetaMetadataSizeField)
	binary.LittleEndian.PutUint64(metadataSizeBytes, metadata.GetSize())

	keySize := len(key)
	dataSize := len(data)
	keySizeBytes := make([]byte, MetaRecordKeySizeField)
	binary.LittleEndian.PutUint64(keySizeBytes, uint64(keySize))

	recordSize := MetaMetadataSizeField + int(metadata.GetSize()) + MetaRecordKeySizeField + keySize + dataSize
	recordBytes := make([]byte, 0, recordSize)

	recordBytes = append(recordBytes, metadataSizeBytes...)
	recordBytes = append(recordBytes, metadata.Bytes()...)
	recordBytes = append(recordBytes, keySizeBytes...)
	recordBytes = append(recordBytes, key...)
	recordBytes = append(recordBytes, data...)

	record := Record{}
	err := ParseRecord(recordBytes, &record)
	assert.NoError(t, err)
	assert.EqualValues(t, key, record.Key)
	assert.EqualValues(t, data, record.Data)
	assert.EqualValues(t, recordSize, record.Size())
	assert.EqualValues(t, metadata.Bytes(), record.Metadata.Bytes())
	assert.EqualValues(t, recordBytes, record.Bytes())

	assert.EqualValues(t, 2, len(record.Metadata))
	assert.EqualValues(t, metadataTest, record.Metadata)
}

func TestParseRecordWithoutMetadata(t *testing.T) {
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	metadataSizeBytes := make([]byte, MetaMetadataSizeField)
	binary.LittleEndian.PutUint64(metadataSizeBytes, 0)

	keySize := len(key)
	dataSize := len(data)
	keySizeBytes := make([]byte, MetaRecordKeySizeField)
	binary.LittleEndian.PutUint64(keySizeBytes, uint64(keySize))

	recordSize := MetaMetadataSizeField + MetaRecordKeySizeField + keySize + dataSize
	recordBytes := make([]byte, 0, recordSize)

	recordBytes = append(recordBytes, metadataSizeBytes...)
	recordBytes = append(recordBytes, keySizeBytes...)
	recordBytes = append(recordBytes, key...)
	recordBytes = append(recordBytes, data...)

	record := Record{}
	err := ParseRecord(recordBytes, &record)
	assert.NoError(t, err)
	assert.EqualValues(t, key, record.Key)
	assert.EqualValues(t, data, record.Data)
	assert.EqualValues(t, recordSize, record.Size())
	assert.EqualValues(t, recordBytes, record.Bytes())
}

func TestRecordValidate(t *testing.T) {
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	record := CreateRecord(key, data)

	assert.NotNil(t, record)

	assert.NoError(t, record.Validate())
}
