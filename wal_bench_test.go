package wal

import (
	"testing"
)

func BenchmarkSegment_Write(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		/*record := Record{
			meta: RecordMetadata{
				sequenceNumber: uint64(i + 1),
				Version:        uint64(i + 1),
			},
			Key:  []byte("testkey" + strconv.Itoa(i)),
			Data: []byte("test data 123"),
		}*/

	}
}

func BenchmarkSegment_Read(b *testing.B) {

}
