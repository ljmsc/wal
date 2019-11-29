package main

import (
	"fmt"
	"github.com/ljmsc/wal"
)

func main() {
	storage, err := wal.Bootstrap(wal.Config{SegmentFileDir: "./storage/"})
	if err != nil {
		panic(err)
	}
	defer storage.Close()

	// record to write
	record := wal.Record{
		Key:  []byte("my key"),
		Data: []byte("my data"),
	}

	// write data to the storage
	if err := storage.Write(&record); err != nil {
		panic(err)
	}

	// after writing the record to disk the sequence number of the record in the storage and the offset on disk are available
	fmt.Printf("successful wrote record. Sequence Number: %d Offset: %d \n", record.SequenceNumber(), record.Offset())

	// clear the record
	record = wal.Record{}

	// read record from disk
	if err := storage.ReadLatest([]byte("my key"), &record); err != nil {
		panic(err)
	}

	fmt.Printf("successful read record from disk with key: %s and value: %s \n", record.Key, record.Data)
}
