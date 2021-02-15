# WAL (Write ahead log)
[![Go Report Card](https://goreportcard.com/badge/github.com/ljmsc/wal)](https://goreportcard.com/report/github.com/ljmsc/wal)
[![GoDoc](https://godoc.org/github.com/ljmsc/wal?status.svg)](https://pkg.go.dev/mod/github.com/ljmsc/wal?tab=overview)

wal is a *write ahead log* written in go.

## Installation
The code was tested with go version `1.15`.
Older versions can also work but have not been tested.
```
go get -u github.com/ljmsc/wal
```
Currently, only unix systems are supported.

## Usage
> **Note:** since version 0.7.0 the wal is no longer safe for concurrent write.
> It is up to the developer to protect the wal. e.g. Mutex, Channels.
> Since this kind of functionality is not always needed I removed it for performance's sake.

### basic
```go
package main

import (
	"fmt"

	"github.com/ljmsc/wal"
)

type entry []byte

func (t *entry) Unmarshal(_data []byte) error {
	*t = _data
	return nil
}

func main() {
	storage, err := wal.Open("./path/to/wal", 4, 100)
	if err != nil {
		// handle error
		panic(err)
	}
	defer storage.Close()

	data := []byte("this is my awesome test data")
	seqNum, err := storage.Write(data)
	if err != nil {
		// handle write error
		panic(err)
	}
	fmt.Printf("successful wrote entry. Sequence Number: %d\n", seqNum)

	re := entry{}
	if err := storage.ReadAt(&re, seqNum); err != nil {
		// handle error
		panic(err)
	}

	fmt.Printf("successful read entry from log. value: %s \n", re)
}
```


## License
The project (and all code) is licensed under the Mozilla Public License Version 2.0.