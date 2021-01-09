package wal

import (
	"github.com/ljmsc/wal/segment"
)

const (
	VersionMetadataKey = "ver"
)

// Entry is a log entry in the write ahead log
type Entry struct {
	chain.Record
}

func CreateEntry(key segment.Key, data segment.Data, metaRecords ...segment.MetaRecord) *Entry {
	return &Entry{Record: *chain.CreateRecord(key, data, metaRecords...)}
}

func setVersion(version uint64, e *Entry) {
	segment.MetaPutUint64(VersionMetadataKey, version, e.Metadata)
}

func recordToEntry(r chain.Record, e *Entry) error {
	if _, ok := r.Metadata[VersionMetadataKey]; !ok {
		return MissingVersionErr
	}

	e.Record = r

	return nil
}

func (e Entry) toRecord(r *chain.Record) {
	*r = e.Record
}

func (e Entry) Validate() error {
	if e.Version() == 0 {
		return VersionIsZeroErr
	}
	return nil
}

func (r Entry) Version() uint64 {
	return r.Metadata.GetUint64(VersionMetadataKey)
}

type Envelope struct {
	Entry *Entry
	Err   error
}