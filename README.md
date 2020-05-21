# WAL (Write ahead log)
[![Go Report Card](https://goreportcard.com/badge/github.com/ljmsc/wal)](https://goreportcard.com/report/github.com/ljmsc/wal)
[![GoDoc](https://godoc.org/github.com/ljmsc/wal?status.svg)](https://pkg.go.dev/mod/github.com/ljmsc/wal?tab=overview)

wal is a write ahead log written in go.

## Installation
At least go version `1.13` is required.
```
go get -u github.com/ljmsc/wal
```

## Usage

### basic
```go
storage, err := wal.Open("./path/to/wal", bucket.DefaultMaxPouchSize, nil)
if err != nil {
    // handle error
    panic(err)
}
defer storage.Close()

foo := storage.CreateEntry([]byte("my_key"), []byte("my_data"))

if err := storage.Write(foo); err != nil {
    // handle write error
    panic(err)
}
fmt.Printf("successful wrote entry. Sequence Number: %d Version: %d \n", foo.SequenceNumber(), foo.Version())

bar := wal.Entry{}
if err := storage.ReadByKey([]byte("my_key"), false, &bar); err != nil {
    // handle error
    panic(err)
}

fmt.Printf("successful read entry from log with key: %s and value: %s \n", bar.Key, bar.Data)
```

### read options
read the latest entry for a given `key` with `ReadByKey()`
```go
err := storage.ReadByKey([]byte("my key"), false, &entry)
```

read the entry for a given sequence number with `ReadBySequenceNumber()`
```go
err := storage.ReadBySequenceNumber(sequenceNum, false, &entry)
```

read the entry for a given `key` and version with `ReadByKeyAndVersion()`
```go
err := storage.ReadByKeyAndVersion([]byte("my key"), 2, false, &entry)
```

### write options

write entry
```go
err := storage.Write(&entry)
```

write entry to disk only if `version` is equal to current version on disk
```go
err := storage.CompareAndWrite(version, &entry)
```

### remove log from disk
`Remove()` will remove all files of the log
```go
err := storage.Remove()
```

