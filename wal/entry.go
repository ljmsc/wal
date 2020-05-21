package wal

import (
	"github.com/ljmsc/wal/bucket"
	"github.com/ljmsc/wal/pouch"
)

const (
	MetaVersionSize    = 8
	VersionMetadataKey = "ver"
)

// Entry is a log entry in the write ahead log
type Entry struct {
	bucket.Record
}

func CreateEntry(key pouch.Key, data pouch.Data, metaRecords ...pouch.MetaRecord) *Entry {
	return &Entry{Record: *bucket.CreateRecord(key, data, metaRecords...)}
}

func setVersion(version uint64, e *Entry) {
	pouch.MetaPutUint64(VersionMetadataKey, version, e.Metadata)
}

func recordToEntry(r bucket.Record, e *Entry) error {
	if _, ok := r.Metadata[VersionMetadataKey]; !ok {
		return MissingVersionErr
	}

	e.Record = r

	return nil
}

func (e Entry) toRecord(r *bucket.Record) {
	*r = e.Record
}

func (e Entry) Validate() error {
	if e.Version() == 0 {
		return VersionIsZeroErr
	}
	return e.Record.Validate()
}

func (r Entry) Version() uint64 {
	return r.Metadata.GetUint64(VersionMetadataKey)
}

type Envelope struct {
	Entry *Entry
	Err   error
}
