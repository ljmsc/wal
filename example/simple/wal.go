package main

import (
	"fmt"

	"github.com/ljmsc/wal/wal"
)

func main() {
	w, err := wal.Open("./path/to/wal", 0, 0, nil)
	if err != nil {
		// handle error
		panic(err)
	}
	defer w.Close()

	foo := wal.CreateRecord(1337, []byte("my_data"))

	seqNum, err := w.Write(foo)
	if err != nil {
		// handle write error
		panic(err)
	}
	fmt.Printf("successful wrote record. Sequence Number: %d Version: %d \n", seqNum, foo.Version())

	bar := wal.CreateRecord(0, nil)
	if err := w.ReadKey(bar, 1337); err != nil {
		// handle error
		panic(err)
	}

	fmt.Printf("successful read entry from log with key: %d and value: %d \n", bar.Key(), bar.Data())
}
