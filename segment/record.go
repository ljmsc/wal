package segment

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

// record construction
// [8 MetadataSize][Metadata][8 KeySize][Key][Data]

const (
	MetaRecordKeySizeField = 8
	MetaMetadataSizeField  = 8
)

// Key - record key
type Key []byte

// Data - record data
type Data []byte

// Hash .
func (k Key) Hash() uint64 {
	hasher := fnv.New64a()
	if _, err := hasher.Write(k); err != nil {
		panic(err)
	}
	return hasher.Sum64()
}

// Record is the representation of a single entry in a segment file
type Record struct {
	Metadata Metadata
	Key      Key
	Data     Data
	HeadOnly bool
}

// CreateRecord creates a new record
func CreateRecord(key Key, data Data, metaRecords ...MetaRecord) *Record {
	metadata := make(map[string][]byte)
	for _, metaRecord := range metaRecords {
		metadata[metaRecord.Name] = metaRecord.Data
	}
	return CreateRecordWithMetadata(metadata, key, data)
}

// CreateRecordWithMetadata creates a new record
func CreateRecordWithMetadata(metadata Metadata, key Key, data Data) *Record {
	return &Record{
		Metadata: metadata,
		Key:      key,
		Data:     data,
	}
}

// ParseRecord parses a byte array to fill the record
//
func ParseRecord(b []byte, record *Record) error {
	if len(b) < MetaMetadataSizeField {
		return fmt.Errorf("not enough bytes to parse metadata size. Bytes: %d", len(b))
	}
	metadataSize := binary.LittleEndian.Uint64(b[:MetaMetadataSizeField])
	b = b[MetaMetadataSizeField:]
	if metadataSize > 0 {
		record.Metadata = make(map[string][]byte)
		metadataBytes := b[:metadataSize]
		if err := ParseMetadata(metadataBytes, record.Metadata); err != nil {
			return fmt.Errorf("can't parse metadata: %w", err)
		}
		b = b[metadataSize:]
	}

	if len(b) < MetaRecordKeySizeField {
		return fmt.Errorf("not enough bytes to parse key size. Bytes: %d", len(b))
	}
	keySize := binary.LittleEndian.Uint64(b[:MetaRecordKeySizeField])
	if keySize < 1 {
		return fmt.Errorf("key size is less then 1: %d", keySize)
	}
	b = b[MetaRecordKeySizeField:]
	if uint64(len(b)) < keySize {
		return fmt.Errorf("not enough bytes to parse key. KeySize: %d Bytes: %d", keySize, len(b))
	}
	record.Key = b[:keySize]
	b = b[keySize:]
	if len(b) > 0 {
		record.Data = b
	}

	return nil
}

func (j Record) HeaderSize() uint64 {
	return MetaMetadataSizeField + j.Metadata.GetSize() + MetaRecordKeySizeField + uint64(len(j.Key))
}

// Size returns the size of the record
func (j Record) Size() uint64 {
	return j.HeaderSize() + uint64(len(j.Data))
}

func (j Record) HeaderBytes() []byte {
	b := make([]byte, 0, j.HeaderSize())

	metadataSize := make([]byte, MetaMetadataSizeField)
	binary.LittleEndian.PutUint64(metadataSize, j.Metadata.GetSize())
	b = append(b, metadataSize...)
	if j.Metadata.GetSize() > 0 {
		b = append(b, j.Metadata.Bytes()...)
	}

	keySizeBytes := make([]byte, MetaRecordKeySizeField)
	binary.LittleEndian.PutUint64(keySizeBytes, uint64(len(j.Key)))
	b = append(b, keySizeBytes...)
	b = append(b, j.Key...)
	return b
}

// Bytes returns the record as a byte array
func (j Record) Bytes() []byte {
	b := make([]byte, 0, j.Size())
	b = append(b, j.HeaderBytes()...)
	b = append(b, j.Data...)
	return b
}

// Validate .
func (j Record) Validate() error {
	if len(j.Key) == 0 {
		return KeyIsEmptyErr
	}

	return nil
}

// Envelope .
type Envelope struct {
	Offset int64
	Record *Record
	Err    error
}
