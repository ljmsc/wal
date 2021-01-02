package benchmark

import (
	"testing"

	"github.com/ljmsc/wal/segment"
)

func BenchmarkByteRecord(b *testing.B) {
	testKey := []byte("test_key")
	testData := []byte("this is my awesome test data in my record. 12345")

	testMetadata := segment.Metadata{}
	testMetadata.MetaPutString("md1", "important metadata")
	testMetadata.MetaPutUint32("md2", 32)

	testRecord := segment.CreateRecordWithMetadata(testMetadata, testKey, testData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = testRecord.Bytes()
	}
}

func BenchmarkParseRecord(b *testing.B) {
	testKey := []byte("test_key")
	testData := []byte("this is my awesome test data in my record. 12345")

	testMetadata := segment.Metadata{}
	testMetadata.MetaPutString("md1", "important metadata")
	testMetadata.MetaPutUint32("md2", 32)

	testRecord := segment.CreateRecordWithMetadata(testMetadata, testKey, testData)
	recordBytes := testRecord.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parsedRecord := segment.Record{}
		if err := segment.ParseRecord(recordBytes, &parsedRecord); err != nil {
			b.Errorf("can't parse record: %s", err.Error())
		}
	}
}
