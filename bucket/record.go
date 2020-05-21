package bucket

import (
	"github.com/ljmsc/wal/pouch"
)

const (
	MetaSequenceNumberSize    = 8
	SequenceNumberMetadataKey = "sq"
)

// Record .
type Record struct {
	pouch.Record
}

func CreateRecord(key pouch.Key, data pouch.Data, metaRecords ...pouch.MetaRecord) *Record {
	return &Record{Record: *pouch.CreateRecord(key, data, metaRecords...)}
}

func setSequenceNumber(seqNum uint64, r *Record) {
	pouch.MetaPutUint64(SequenceNumberMetadataKey, seqNum, r.Metadata)
}

func toRecord(pr pouch.Record, r *Record) error {
	if _, ok := pr.Metadata[SequenceNumberMetadataKey]; !ok {
		return MissingSequenceNumberFieldErr
	}

	r.Record = pr
	return nil
}

func (r Record) toPouchRecord(pr *pouch.Record) {
	*pr = r.Record
}

func (r Record) Validate() error {
	if r.SequenceNumber() < 1 {
		return ZeroSequenceErr
	}

	return r.Record.Validate()
}

func (r Record) SequenceNumber() uint64 {
	return r.Metadata.GetUint64(SequenceNumberMetadataKey)
}

// Envelope .
type Envelope struct {
	Record *Record
	Err    error
}

// RecordPosition .
type RecordPosition struct {
	Offset int64
	Pouch  *pouch.Pouch
}
