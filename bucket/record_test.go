package bucket

import (
	"encoding/binary"
	"testing"

	"github.com/ljmsc/wal/pouch"
	"github.com/stretchr/testify/assert"
)

func TestCreateRecord(t *testing.T) {
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	record := CreateRecord(key, data)

	assert.NotNil(t, record)

	assert.EqualValues(t, key, record.Key)
	assert.EqualValues(t, data, record.Data)
}

func TestParseRecord(t *testing.T) {
	sn := uint64(42)
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	seqNumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqNumBytes, sn)

	metadata := pouch.Metadata(make(map[string][]byte))
	metadata[SequenceNumberMetadataKey] = seqNumBytes

	metadataSizeBytes := make([]byte, pouch.MetaMetadataSizeField)
	binary.LittleEndian.PutUint64(metadataSizeBytes, metadata.GetSize())

	keySize := len(key)
	dataSize := len(data)
	keySizeBytes := make([]byte, pouch.MetaRecordKeySizeField)
	binary.LittleEndian.PutUint64(keySizeBytes, uint64(keySize))

	recordBytes := make([]byte, 0, pouch.MetaMetadataSizeField+int(metadata.GetSize())+pouch.MetaRecordKeySizeField+keySize+dataSize)

	recordBytes = append(recordBytes, metadataSizeBytes...)
	recordBytes = append(recordBytes, metadata.Bytes()...)
	recordBytes = append(recordBytes, keySizeBytes...)
	recordBytes = append(recordBytes, key...)
	recordBytes = append(recordBytes, data...)

	sr := pouch.Record{}
	err := pouch.ParseRecord(recordBytes, &sr)

	if !assert.NoError(t, err) {
		return
	}

	record := Record{}
	err = toRecord(sr, &record)
	assert.NoError(t, err)

	assert.EqualValues(t, key, record.Key)

	assert.EqualValues(t, sn, record.SequenceNumber())

	assert.EqualValues(t, data, record.Data)
}

func TestRecordValidate(t *testing.T) {
	key := []byte("my_test_key")
	data := []byte("my_test_data")

	record := CreateRecord(key, data)
	setSequenceNumber(42, record)

	assert.NotNil(t, record)

	assert.NoError(t, record.Validate())
}

func TestToPouchRecord(t *testing.T) {
	r := CreateRecord([]byte("my_test_key"), []byte("my_test_data"))
	setSequenceNumber(42, r)
	r.Metadata["test1"] = []byte("test meta")

	pr := pouch.Record{}
	addressPr := &pr

	r.toPouchRecord(&pr)

	assert.Equal(t, addressPr, &pr)
	assert.EqualValues(t, []byte("my_test_key"), pr.Key)
	assert.EqualValues(t, []byte("test meta"), pr.Metadata["test1"])
}
