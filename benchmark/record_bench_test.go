package benchmark

import (
	"testing"

	"github.com/ljmsc/wal/pouch"
)

func BenchmarkPouchByteRecord(b *testing.B) {
	testKey := []byte("test_key")
	testData := []byte("this is my awesome test data in my record. 12345")

	testMetadata := pouch.Metadata{}
	testMetadata.MetaPutString("md1", "important metadata")
	testMetadata.MetaPutUint32("md2", 32)

	testRecord := pouch.CreateRecordWithMetadata(testMetadata, testKey, testData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = testRecord.Bytes()
	}
}

func BenchmarkPouchParseRecord(b *testing.B) {
	testKey := []byte("test_key")
	testData := []byte("this is my awesome test data in my record. 12345")

	testMetadata := pouch.Metadata{}
	testMetadata.MetaPutString("md1", "important metadata")
	testMetadata.MetaPutUint32("md2", 32)

	testRecord := pouch.CreateRecordWithMetadata(testMetadata, testKey, testData)
	recordBytes := testRecord.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parsedRecord := pouch.Record{}
		if err := pouch.ParseRecord(recordBytes, &parsedRecord); err != nil {
			b.Errorf("can't parse record: %s", err.Error())
		}
	}
}
