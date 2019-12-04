package wal

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	// defining the length of the sequence number field in bytes in the record
	lengthOfSequenceNumberField = 8

	lengthOfVersionField = 8

	lengthOfTimestampField = 8

	// defining the length of the key length field in bytes in the record. this field contains the actual length of the key field itself
	lengthOfKeyLengthField = 8

	minimalSize = lengthOfKeyLengthField + lengthOfVersionField + lengthOfTimestampField + lengthOfSequenceNumberField
)

// recordMetadata contains metadata for the record.
type recordMetadata struct {
	sequenceNumber uint64
	version        uint64
	createdAt      time.Time
	// the following fields are not stored on disk but calculated
	offset int64
	size   int64
}

// Record represents a data entry from disk
type Record struct {
	meta recordMetadata
	Key  Key
	Data []byte
}

func (r Record) isReadyToWrite() error {
	if r.meta.sequenceNumber < 1 {
		return errors.New("currentSequenceNumber is zero")
	}

	if r.meta.version < 1 {
		return errors.New("version is < 1")
	}

	if r.meta.createdAt.IsZero() {
		return errors.New("timestamp is zero")
	}

	if len(r.Key) <= 0 {
		return errors.New("key is empty")
	}

	return nil
}

// FromBytes parses the given byte array to the record fields
func (r *Record) FromBytes(recordBytes []byte) error {
	if len(recordBytes) < minimalSize {
		return ErrNotEnoughBytes
	}
	r.meta.size = int64(len(recordBytes))

	r.meta.sequenceNumber = binary.BigEndian.Uint64(recordBytes[:lengthOfSequenceNumberField])
	recordBytes = recordBytes[lengthOfSequenceNumberField:]

	r.meta.version = binary.BigEndian.Uint64(recordBytes[:lengthOfVersionField])
	recordBytes = recordBytes[lengthOfVersionField:]

	timestamp := binary.BigEndian.Uint64(recordBytes[:lengthOfTimestampField])
	r.meta.createdAt = time.Unix(0, int64(timestamp))
	recordBytes = recordBytes[lengthOfTimestampField:]

	lengthOfKey := binary.BigEndian.Uint64(recordBytes[:lengthOfKeyLengthField])
	recordBytes = recordBytes[lengthOfKeyLengthField:]

	r.Key = recordBytes[:lengthOfKey]
	recordBytes = recordBytes[lengthOfKey:]

	r.Data = recordBytes

	return nil
}

// ToBytes converts the record to a byte slice
func (r *Record) ToBytes() []byte {
	recordBytes := make([]byte, 0, r.meta.size)

	sequenceNumberBytes := make([]byte, lengthOfSequenceNumberField)
	binary.BigEndian.PutUint64(sequenceNumberBytes, r.meta.sequenceNumber)
	recordBytes = append(recordBytes, sequenceNumberBytes...)

	versionBytes := make([]byte, lengthOfVersionField)
	binary.BigEndian.PutUint64(versionBytes, r.meta.version)
	recordBytes = append(recordBytes, versionBytes...)

	timestampBytes := make([]byte, lengthOfTimestampField)
	binary.BigEndian.PutUint64(timestampBytes, uint64(r.meta.createdAt.UnixNano()))
	recordBytes = append(recordBytes, timestampBytes...)

	keyLengthBytes := make([]byte, lengthOfKeyLengthField)
	binary.BigEndian.PutUint64(keyLengthBytes, uint64(len(r.Key)))
	recordBytes = append(recordBytes, keyLengthBytes...)
	recordBytes = append(recordBytes, r.Key...)
	recordBytes = append(recordBytes, r.Data...)

	r.meta.size = int64(len(recordBytes))
	return recordBytes
}

// Size returns the size of the record in bytes
func (r Record) Size() int64 {
	return r.meta.size
}

// Offset returns the offset of the record on disk
func (r Record) Offset() int64 {
	return r.meta.offset
}

// SequenceNumber returns the sequence number of the record in the log
func (r Record) SequenceNumber() uint64 {
	return r.meta.sequenceNumber
}

// Version returns the version of the record
func (r Record) Version() uint64 {
	return r.meta.version
}

// CreatedAt returns the time when the records was created
func (r Record) CreatedAt() time.Time {
	return r.meta.createdAt
}
