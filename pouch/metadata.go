package pouch

import (
	"encoding/binary"
	"sort"
)

// metadata construction
// [8 ItemCount][ [][8 ItemNameSize][ItemName][8 ItemDataSize][ItemData] ]

const (
	MetaMetadataItemCount         = 8
	MetaMetadataItemNameSizeField = 8
	MetaMetadataItemDataSizeField = 8
)

type MetaRecord struct {
	Name string
	Data []byte
}

// Metadata - metadata for a record
type Metadata map[string][]byte

func ParseMetadata(b []byte, m Metadata) error {
	if len(b) < MetaMetadataItemCount {
		return NotEnoughBytesErr
	}
	itemCount, _ := binary.Uvarint(b[:MetaMetadataItemCount])
	b = b[MetaMetadataItemCount:]

	for i := uint64(0); i < itemCount; i++ {
		if len(b) < MetaMetadataItemNameSizeField {
			return NotEnoughBytesErr
		}
		nameSize, _ := binary.Uvarint(b[:MetaMetadataItemNameSizeField])
		b = b[MetaMetadataItemNameSizeField:]
		itemName := string(b[:nameSize])
		b = b[nameSize:]

		if len(b) < MetaMetadataItemDataSizeField {
			return NotEnoughBytesErr
		}
		dataSize, _ := binary.Uvarint(b[:MetaMetadataItemDataSizeField])
		b = b[MetaMetadataItemDataSizeField:]
		itemData := b[:dataSize]
		b = b[dataSize:]
		m[itemName] = itemData
	}
	if len(b) > 0 {
		return TooManyBytesErr
	}

	return nil
}

func (m Metadata) GetSize() int64 {
	if len(m) == 0 {
		return 0
	}

	dataSize := int64(0)
	for name, data := range m {
		dataSize += MetaMetadataItemNameSizeField + int64(len([]byte(name)))
		dataSize += MetaMetadataItemDataSizeField + int64(len(data))
	}

	return MetaMetadataItemCount + dataSize
}

// Bytes
func (m Metadata) Bytes() []byte {
	if len(m) == 0 {
		return []byte{}
	}
	metaBytes := make([]byte, 0, m.GetSize())

	itemCountBytes := make([]byte, MetaMetadataItemCount)
	binary.PutUvarint(itemCountBytes, uint64(len(m)))

	metaBytes = append(metaBytes, itemCountBytes...)
	keys := make([]string, 0, len(m))
	for name := range m {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	for _, name := range keys {
		data := m[name]
		nameBytes := []byte(name)
		nameSizeBytes := make([]byte, MetaMetadataItemNameSizeField)
		binary.PutUvarint(nameSizeBytes, uint64(len(nameBytes)))

		dataSizeBytes := make([]byte, MetaMetadataItemDataSizeField)
		binary.PutUvarint(dataSizeBytes, uint64(len(data)))

		metaBytes = append(metaBytes, nameSizeBytes...)
		metaBytes = append(metaBytes, name...)
		metaBytes = append(metaBytes, dataSizeBytes...)
		metaBytes = append(metaBytes, data...)
	}
	return metaBytes
}

func (m Metadata) Get(name string) []byte {
	if _, ok := m[name]; !ok {
		return []byte{}
	}
	return m[name]
}
