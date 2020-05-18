package main

import (
	"fmt"

	"github.com/ljmsc/wal/bucket"
	"github.com/ljmsc/wal/wal"
)

func main() {
	w, err := wal.Open("./path/to/wal", bucket.DefaultMaxPouchSize, nil)
	if err != nil {
		// handle error
		panic(err)
	}
	defer w.Close()

	foo := wal.CreateEntry([]byte("my_key"), []byte("my_data"))

	if err := w.Write(foo); err != nil {
		// handle write error
		panic(err)
	}
	fmt.Printf("successful wrote entry. Sequence Number: %d Version: %d \n", foo.SequenceNumber(), foo.Version())

	bar := wal.Entry{}
	if err := w.ReadByKey([]byte("my_key"), false, &bar); err != nil {
		// handle error
		panic(err)
	}

	fmt.Printf("successful read entry from log with key: %s and value: %s \n", bar.Key, bar.Data)
}
