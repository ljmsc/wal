package wal

import (
	"fmt"
	"sync"

	"github.com/ljmsc/wal/bucket"
	"github.com/ljmsc/wal/pouch"
)

// Wal is a write ahead log
type Wal struct {
	// bucket is the file storage for the write ahead log
	bucket *bucket.Bucket
	// latestVersions stores for all key hashes the latest version of the entry
	latestVersions map[uint64]uint64
	// keyVersionSeqNumbers maps for a given hash key all existing versions to there sequence number in the bucket
	keyVersionSeqNumbers map[uint64]map[uint64]uint64
	// dataMutex is a mutex to protect the data structures in the log
	dataMutex sync.RWMutex
	// closed
	closed bool
}

// OpenWithHandler .
func OpenWithHandler(name string, handler func(e Entry) error) (*Wal, error) {
	return Open(name, 0, handler)
}

// Open opens a new or existing wal
func Open(name string, maxSegmentSize uint64, handler func(e Entry) error) (*Wal, error) {
	w := Wal{
		latestVersions:       make(map[uint64]uint64),
		keyVersionSeqNumbers: make(map[uint64]map[uint64]uint64),
		dataMutex:            sync.RWMutex{},
		closed:               false,
	}
	b, err := bucket.Open(name, maxSegmentSize, func(r bucket.Record) error {
		entry := Entry{}
		if err := recordToEntry(r, &entry); err != nil {
			return fmt.Errorf("can't convert record to entry: %w", err)
		}
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("entry is not valid: %w", err)
		}
		if err := handler(entry); err != nil {
			return fmt.Errorf("can't execute handler for entry: %w", err)
		}

		w.latestVersions[entry.Key.Hash()] = entry.Version()
		w.keyVersionSeqNumbers[entry.Key.Hash()][entry.Version()] = entry.SequenceNumber()
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("can't open bucket files for log: %w", err)
	}
	w.bucket = b

	return &w, nil
}

func convertRecordStream(recordStream <-chan bucket.Envelope) <-chan Envelope {
	stream := make(chan Envelope)
	go func() {
		for {
			env, ok := <-recordStream
			if !ok {
				break
			}
			if env.Err != nil {
				stream <- Envelope{Err: ReadErr{env.Err}}
				continue
			}
			e := Entry{}
			if err := recordToEntry(*env.Record, &e); err != nil {
				stream <- Envelope{Err: ReadErr{err}}
				continue
			}
			stream <- Envelope{Entry: &e}
		}
		close(stream)
	}()
	return stream
}

// Write writes a log entry to the write ahead log
func (w *Wal) Write(e *Entry) error {
	w.dataMutex.RLock()
	defer w.dataMutex.RUnlock()
	if w.closed {
		return ClosedErr
	}

	// remove the read lock to lock for write
	w.dataMutex.RUnlock()
	// add read lock to satisfy RUnlock from above
	defer w.dataMutex.RLock()

	w.dataMutex.Lock()
	defer w.dataMutex.Unlock()

	version := uint64(1)
	if currentVersion, ok := w.latestVersions[e.Key.Hash()]; ok {
		version = currentVersion + 1
	}
	setVersion(version, e)

	r := bucket.Record{}
	e.toRecord(&r)

	if err := w.bucket.WriteRecord(&r); err != nil {
		return WriteErr{err}
	}

	w.latestVersions[e.Key.Hash()] = version
	if version == 1 {
		w.keyVersionSeqNumbers[e.Key.Hash()] = make(map[uint64]uint64)
	}
	w.keyVersionSeqNumbers[e.Key.Hash()][version] = r.SequenceNumber()
	return nil
}

// CompareAndWrite checks the given version with the latest entry in the wal.
// if version > latest version => invalid version number
// if version < latest version => version is to old, there is a newer version in the wal
// if version == latest version => the given entry is written to the log
func (w *Wal) CompareAndWrite(version uint64, e *Entry) error {
	w.dataMutex.RLock()
	if _, ok := w.latestVersions[e.Key.Hash()]; !ok {
		w.dataMutex.RUnlock()
		return EntryNotFoundErr
	}
	latestVersion := w.latestVersions[e.Key.Hash()]
	w.dataMutex.RUnlock()
	if version > latestVersion {
		return InvalidVersionErr
	}
	if version < latestVersion {
		return OldVersionErr
	}
	return w.Write(e)
}

// ReadByKey reads the latest version of an entry by key
func (w *Wal) ReadByKey(key pouch.Key, headOnly bool, e *Entry) error {
	w.dataMutex.RLock()
	defer w.dataMutex.RUnlock()
	if w.closed {
		return ClosedErr
	}
	r := bucket.Record{}
	if err := w.bucket.ReadByKey(key, headOnly, &r); err != nil {
		return ReadErr{err}
	}
	if err := recordToEntry(r, e); err != nil {
		return ReadErr{err}
	}
	return nil
}

// ReadBySequenceNumber read an entry by the given sequence number
func (w *Wal) ReadBySequenceNumber(seqNum uint64, headOnly bool, e *Entry) error {
	w.dataMutex.RLock()
	defer w.dataMutex.RUnlock()
	if w.closed {
		return ClosedErr
	}
	r := bucket.Record{}
	if err := w.bucket.ReadBySequenceNumber(seqNum, headOnly, &r); err != nil {
		return ReadErr{err}
	}
	if err := recordToEntry(r, e); err != nil {
		return ReadErr{err}
	}
	return nil
}

func (w *Wal) ReadByKeyAndVersion(key pouch.Key, version uint64, headOnly bool, e *Entry) error {
	w.dataMutex.RLock()
	defer w.dataMutex.RUnlock()
	if w.closed {
		return ClosedErr
	}

	if _, ok := w.keyVersionSeqNumbers[key.Hash()]; !ok {
		return EntryNotFoundErr
	}

	if _, ok := w.keyVersionSeqNumbers[key.Hash()][version]; !ok {
		return EntryVersionNotFoundErr
	}

	seqNum := w.keyVersionSeqNumbers[key.Hash()][version]
	r := bucket.Record{}
	if err := w.bucket.ReadBySequenceNumber(seqNum, headOnly, &r); err != nil {
		return ReadErr{err}
	}
	if err := recordToEntry(r, e); err != nil {
		return ReadErr{err}
	}

	return nil
}

// StreamEntries streams entries from the log into the returning channel. startSeqNum defines the beginning.
// startSeqNum = 1 for all entries
// endSeqNum = 0 for all entries
func (w *Wal) StreamEntries(startSeqNum uint64, endSeqNum uint64, headOnly bool) <-chan Envelope {
	recordStream := w.bucket.StreamRecords(startSeqNum, endSeqNum, headOnly)
	return convertRecordStream(recordStream)
}

// StreamLatestEntries .
func (w *Wal) StreamLatestEntries(headOnly bool) <-chan Envelope {
	recordStream := w.bucket.StreamLatestRecords(headOnly)
	return convertRecordStream(recordStream)
}

// Compress compresses the log
func (w *Wal) Compress() error {
	return w.bucket.Compress()
}

// CompressWithFilter compresses the bucket based on a given filter
// for filter = true remove item
// for filter = false keep item
func (w *Wal) CompressWithFilter(filter func(item *Entry) bool) error {
	bucketFilter := func(item *bucket.Record) bool {
		entry := Entry{}
		if err := recordToEntry(*item, &entry); err != nil {
			return true
		}
		return filter(&entry)
	}
	return w.bucket.CompressWithFilter(bucketFilter)
}

// Close closes the log
func (w *Wal) Close() error {
	w.dataMutex.Lock()
	defer w.dataMutex.Unlock()
	w.closed = true
	return w.bucket.Close()
}

// Remove removes the hole write ahead log from disk
func (w *Wal) Remove() error {
	w.dataMutex.RLock()
	defer w.dataMutex.RUnlock()
	if !w.closed {
		return NotClosedErr
	}
	return w.bucket.Remove()
}

// IsClosed returns true if the log is already closed
func (w *Wal) IsClosed() bool {
	return w.closed
}

// Size returns the size of the log in bytes
func (w *Wal) Size() (int64, error) {
	return w.bucket.Size()
}