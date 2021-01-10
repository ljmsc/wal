# WAL (Write ahead log)
[![Go Report Card](https://goreportcard.com/badge/github.com/ljmsc/wal)](https://goreportcard.com/report/github.com/ljmsc/wal)
[![GoDoc](https://godoc.org/github.com/ljmsc/wal?status.svg)](https://pkg.go.dev/mod/github.com/ljmsc/wal?tab=overview)

wal is a write ahead log written in go.

## Installation
At least go version `1.15` is required.
```
go get -u github.com/ljmsc/wal
```

## Usage
> **Note:** since version 0.7.0 the wal is no longer safe for concurrent write.
> It is up to the developer to protect the wal. e.g. Mutex, Channels.
> Since this kind of functionality is not always needed I removed it for performance's sake.

### basic
```go
package main

import (
	"fmt"

	"github.com/ljmsc/wal/wal"
)

func main() {
	storage, err := wal.Open("./path/to/wal", 0, 0, nil)
	if err != nil {
		// handle error
		panic(err)
	}
	defer storage.Close()

	foo := wal.CreateRecord(1337, []byte("my_data"))

	seqNum, err := storage.Write(foo)
	if err != nil {
		// handle write error
		panic(err)
	}
	fmt.Printf("successful wrote record. Sequence Number: %d Version: %d \n", seqNum, foo.Version())

	bar := wal.CreateRecord(0, nil)
	if err := storage.ReadKey(bar, 1337); err != nil {
		// handle error
		panic(err)
	}

	fmt.Printf("successful read entry from log with key: %s and value: %s \n", bar.Key(), bar.Data())
}
```

### read options
read the record for a given sequence number with `ReadAt()`
```go
sequenceNum := uint64(42)
...
err := storage.ReadAt(&record, sequenceNum)
```

read the latest record for a given `key` with `ReadKey()`
```go
key := uint64(1337)
...
err := storage.ReadKey(&record, key)
```

read a defined version of a record with `ReadVersion()`
```go
version := uint64(2)
...
err := storage.ReadVersion( &record, version)
```

### write options

write record
```go
seqNum, err := storage.Write(&record)
```

write record only if `version` is equal to current version on disk
```go
version := uint64(2)
...
err := storage.CompareWrite(&record, version)
```

## License
The project (and all code) is licensed under the Mozilla Public License Version 2.0.