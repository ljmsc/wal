package segment

import (
	"fmt"
)

func compress(p Segment, compression func(chan<- RecordEnvelope)) <-chan RecordEnvelope {
	if p.IsClosed() {
		return nil
	}
	stream := make(chan RecordEnvelope)
	go compression(stream)
	return stream
}

// CompressWithFilter returns a channel with all records based on given filter function
func CompressWithFilter(p Segment, filter func(_record Record) bool) <-chan RecordEnvelope {
	return compress(p, func(stream chan<- RecordEnvelope) {
		recordStream := Stream(p, p.Offsets())
		for envelope := range recordStream {
			if filter(envelope.Record) {
				stream <- envelope
			}
		}
		close(stream)
	})
}

// CompressToFile writes the latest records in a segment to a new segment file
func CompressToFile(_old Segment, _new Segment) error {
	stream := Stream(_old, LatestOffsets(_old))
	if stream == nil {
		return ClosedErr
	}

	var lastWrittenOffsetNew int64 = 0
	var lastWrittenOffsetOld int64 = 0

	for envelope := range stream {
		if envelope.Err != nil {
			return CompressionErr{
				LastWrittenOffsetNew: lastWrittenOffsetNew,
				LastWrittenOffsetOld: lastWrittenOffsetOld,
				Err:                  envelope.Err,
			}
		}
		writtenOffset, err := _new.Write(envelope.Record)
		if err != nil {
			return fmt.Errorf("can't write to segment: %w", err)
		}
		lastWrittenOffsetNew = writtenOffset
		lastWrittenOffsetOld = envelope.Offset
	}
	return nil
}
