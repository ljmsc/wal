package pouch

import (
	"fmt"
)

func compress(p *Pouch, compression func(chan<- Envelope)) <-chan Envelope {
	if p.IsClosed() {
		return nil
	}
	stream := make(chan Envelope)
	go compression(stream)
	return stream
}

// CompressWithFilter returns a channel with all records based on given filter function
func CompressWithFilter(p *Pouch, filter func(record *Record) bool) <-chan Envelope {
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

// CompressToFile writes the latest records in a pouch to a new pouch file
func CompressToFile(oldPouch *Pouch, newPouch *Pouch) error {
	stream := oldPouch.StreamLatestRecords(false)
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
		writtenOffset, err := newPouch.WriteRecord(item.Record)
		if err != nil {
			return fmt.Errorf("can't write to pouch: %w", err)
		}
		lastWrittenOffsetNew = writtenOffset
		lastWrittenOffsetOld = item.Offset
	}
	return nil
}
