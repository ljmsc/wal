# WAL (Write ahead log)
[![Go Report Card](https://goreportcard.com/badge/github.com/ljmsc/wal)](https://goreportcard.com/report/github.com/ljmsc/wal)
[![GoDoc](https://godoc.org/github.com/ljmsc/wal?status.svg)](https://godoc.org/github.com/ljmsc/wal)

wal is a write ahead log written in go.

## Installation
At least version `1.13` of go is required.
```
go get -u github.com/ljmsc/wal
```

## Compaction
### Compaction trigger
there are three different ways to trigger compaction:
- no compaction at all `TriggerNone`
- manual trigger `TriggerManually`
- time trigger `TriggerTime`

### compaction strategies
there are two different compaction strategies:  

**Keep**  
the `StrategyKeep` strategy keeps the latest n versions of a record.
```go
CompactionConfig{
    Trigger:    TriggerManually,
    Strategy:   StrategyKeep,
    KeepAmount: 1,
}
```

**Expire**  
the `StrategyExpire` strategy remove all versions of a record which is older than `time.Now() - ExpirationThreshold`
```go
CompactionConfig{
    Trigger:             TriggerManually,
    Strategy:            StrategyExpire,
    ExpirationThreshold: time.Second * 5,
}
```

## Usage

### basic
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

### write options

write record
```go
err := storage.Write(&record)
```

write record to disk only if `version` is equal to current version on disk
```go
err := storage.CompareAndWrite(version, &record)
```

### remove log from disk
`Remove()` will remove all files of the log
```go
err := storage.Remove()
```

### compaction
```go
storage, err := wal.Bootstrap(
	Config{
		Compaction: CompactionConfig{
			Trigger:             TriggerManually,
			Strategy:            StrategyExpire,
			ExpirationThreshold: time.Second,
		},
		SegmentFileDir:      "./storage/",
	},
	)
...
if err := storage.Compact(); err != nil {
    // handle error 
}
...
```
