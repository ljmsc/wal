package bucket

import "github.com/ljmsc/wal/segment"

const (
	SequenceNumberMetadataKey = "sq"
)

// Record .
type Record struct {
	segment.Record
}

func CreateRecord(key segment.Key, data segment.Data, metaRecords ...segment.MetaRecord) *Record {
	return &Record{Record: *segment.CreateRecord(key, data, metaRecords...)}
}

func setSequenceNumber(seqNum uint64, r *Record) {
	segment.MetaPutUint64(SequenceNumberMetadataKey, seqNum, r.Metadata)
}

func toRecord(pr segment.Record, r *Record) error {
	if _, ok := pr.Metadata[SequenceNumberMetadataKey]; !ok {
		return MissingSequenceNumberFieldErr
	}

	r.Record = pr
	return nil
}

func (r Record) toSegmentRecord(pr *segment.Record) {
	*pr = r.Record
}

func (r Record) Validate() error {
	if r.SequenceNumber() < 1 {
		return ZeroSequenceErr
	}

	return nil
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
	KeyHash uint64
	Offset  int64
	Segment segment.Segment
}
