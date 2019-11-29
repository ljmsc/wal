# WAL (Write ahead log)
wal is a write ahead log written in go.

## Example

### basic usage
```go
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
```

### read options
read the latest record for a given `key` with `ReadLatest()`
```go
err := storage.ReadLatest([]byte("my key"), &record)
```

read all records for a given `key` with `ReadAll()`
```go
err := storage.ReadAll([]byte("my key"), &record)
```

read the record for a given sequence number with `ReadSequenceNum()`
```go
err := storage.ReadSequenceNum(sequenceNum, &record)
```

### remove log from disk
`Remove()` will remove all files of the log
```go
err := storage.Remove()
```
