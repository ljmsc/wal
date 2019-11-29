package wal

import (
	"encoding/binary"
	"errors"
)

const (
	// defining the length of the sequence number field in bytes in the record
	lengthOfSequenceNumberField = 8

	// defining the length of the key length field in bytes in the record. this field contains the actual length of the key field itself
	lengthOfKeyLengthField = 8

	minimalSize = lengthOfKeyLengthField + lengthOfSequenceNumberField
)

type RecordMetadata struct {
	SequenceNumber uint64
	offset         int64
	size           int64
}

type Record struct {
	Meta RecordMetadata
	Key  Key
	Data []byte
}

func (r Record) IsReadyToWrite() error {
	if r.Meta.SequenceNumber < 1 {
		return errors.New("currentSequenceNumber is zero")
	}

	if len(r.Key) <= 0 {
		return errors.New("key is empty")
	}

	return nil
}

// FromBytes parses the given byte array to the record fields
func (r *Record) FromBytes(recordBytes []byte) error {
	if len(recordBytes) < minimalSize {
		return NotEnoughBytesErr
	}
	r.Meta.size = int64(len(recordBytes))

	r.Meta.SequenceNumber = binary.BigEndian.Uint64(recordBytes[:lengthOfSequenceNumberField])
	recordBytes = recordBytes[lengthOfSequenceNumberField:]

	lengthOfKey := binary.BigEndian.Uint64(recordBytes[:lengthOfKeyLengthField])
	recordBytes = recordBytes[lengthOfKeyLengthField:]

	r.Key = recordBytes[:lengthOfKey]
	recordBytes = recordBytes[lengthOfKey:]

	r.Data = recordBytes

	return nil
}

// ToBytes converts the record to a byte slice
func (r *Record) ToBytes() []byte {
	recordBytes := make([]byte, 0, r.Meta.size)

	sequenceNumberBytes := make([]byte, lengthOfSequenceNumberField)
	binary.BigEndian.PutUint64(sequenceNumberBytes, r.Meta.SequenceNumber)
	recordBytes = append(recordBytes, sequenceNumberBytes...)

	keyLengthBytes := make([]byte, lengthOfKeyLengthField)
	binary.BigEndian.PutUint64(keyLengthBytes, uint64(len(r.Key)))
	recordBytes = append(recordBytes, keyLengthBytes...)
	recordBytes = append(recordBytes, r.Key...)
	recordBytes = append(recordBytes, r.Data...)

	r.Meta.size = int64(len(recordBytes))
	return recordBytes
}

// Size returns the size of the record in bytes
func (r Record) Size() int64 {
	return r.Meta.size
}

func (r Record) Offset() int64 {
	return r.Meta.offset
}
