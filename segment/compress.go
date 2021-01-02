package segment

import (
	"fmt"
)

func compress(p Segment, compression func(chan<- Envelope)) <-chan Envelope {
	if p.IsClosed() {
		return nil
	}
	stream := make(chan Envelope)
	go compression(stream)
	return stream
}

// CompressWithFilter returns a channel with all records based on given filter function
func CompressWithFilter(p Segment, filter func(record *Record) bool) <-chan Envelope {
	return compress(p, func(stream chan<- Envelope) {
		recordStream := p.StreamRecords(false)
		for {
			envelope, ok := <-recordStream
			if !ok {
				break
			}

			if filter(envelope.Record) {
				stream <- envelope
			}
		}
		close(stream)
	})
}

// CompressToFile writes the latest records in a segment to a new segment file
func CompressToFile(_old Segment, _new Segment) error {
	stream := _old.StreamLatestRecords(false)
	if stream == nil {
		return ClosedErr
	}

	var lastWrittenOffsetNew int64 = 0
	var lastWrittenOffsetOld int64 = 0
	for {
		item, ok := <-stream
		if !ok {
			break
		}
		if item.Err != nil {
			return CompressionErr{
				LastWrittenOffsetNew: lastWrittenOffsetNew,
				LastWrittenOffsetOld: lastWrittenOffsetOld,
				Err:                  item.Err,
			}
		}
		writtenOffset, err := _new.WriteRecord(item.Record)
		if err != nil {
			return fmt.Errorf("can't write to segment: %w", err)
		}
		lastWrittenOffsetNew = writtenOffset
		lastWrittenOffsetOld = item.Offset
	}
	return nil
}
