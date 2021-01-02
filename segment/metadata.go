package segment

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

	int64Bytes = 8
	int32Bytes = 4
	int16Bytes = 2
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
	itemCount := binary.LittleEndian.Uint64(b[:MetaMetadataItemCount])
	b = b[MetaMetadataItemCount:]

	for i := uint64(0); i < itemCount; i++ {
		if len(b) < MetaMetadataItemNameSizeField {
			return NotEnoughBytesErr
		}
		nameSize := binary.LittleEndian.Uint64(b[:MetaMetadataItemNameSizeField])
		b = b[MetaMetadataItemNameSizeField:]
		itemName := string(b[:nameSize])
		b = b[nameSize:]

		if len(b) < MetaMetadataItemDataSizeField {
			return NotEnoughBytesErr
		}
		dataSize := binary.LittleEndian.Uint64(b[:MetaMetadataItemDataSizeField])
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

func (m Metadata) GetSize() uint64 {
	if len(m) == 0 {
		return 0
	}

	dataSize := uint64(0)
	for name, data := range m {
		dataSize += MetaMetadataItemNameSizeField + uint64(len([]byte(name)))
		dataSize += MetaMetadataItemDataSizeField + uint64(len(data))
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
	binary.LittleEndian.PutUint64(itemCountBytes, uint64(len(m)))

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
		binary.LittleEndian.PutUint64(nameSizeBytes, uint64(len(nameBytes)))

		dataSizeBytes := make([]byte, MetaMetadataItemDataSizeField)
		binary.LittleEndian.PutUint64(dataSizeBytes, uint64(len(data)))

		metaBytes = append(metaBytes, nameSizeBytes...)
		metaBytes = append(metaBytes, name...)
		metaBytes = append(metaBytes, dataSizeBytes...)
		metaBytes = append(metaBytes, data...)
	}
	return metaBytes
}

func (m Metadata) Get(name string) []byte {
	if _, ok := m[name]; !ok {
		return make([]byte, 8)
	}
	return m[name]
}

func (m Metadata) GetUint64(name string) uint64 {
	return binary.LittleEndian.Uint64(m.Get(name))
}

func (m Metadata) GetUint32(name string) uint32 {
	return binary.LittleEndian.Uint32(m.Get(name))
}

func (m Metadata) GetUint16(name string) uint16 {
	return binary.LittleEndian.Uint16(m.Get(name))
}

func (m Metadata) GetString(name string) string {
	return string(m.Get(name))
}

func (m Metadata) MetaPutUint64(name string, value uint64) {
	valueBytes := make([]byte, int64Bytes)
	binary.LittleEndian.PutUint64(valueBytes, value)
	m[name] = valueBytes
}

func (m Metadata) MetaPutUint32(name string, value uint32) {
	valueBytes := make([]byte, int32Bytes)
	binary.LittleEndian.PutUint32(valueBytes, value)
	m[name] = valueBytes
}

func (m Metadata) MetaPutUint16(name string, value uint16) {
	valueBytes := make([]byte, int16Bytes)
	binary.LittleEndian.PutUint16(valueBytes, value)
	m[name] = valueBytes
}

func (m Metadata) MetaPutString(name, value string) {
	m[name] = []byte(value)
}

func MetaPutUint64(name string, value uint64, metadata Metadata) {
	metadata.MetaPutUint64(name, value)
}

func MetaPutUint32(name string, value uint32, metadata Metadata) {
	metadata.MetaPutUint32(name, value)
}

func MetaPutUint16(name string, value uint16, metadata Metadata) {
	metadata.MetaPutUint16(name, value)
}

func MetaPutString(name string, value string, metadata Metadata) {
	metadata.MetaPutString(name, value)
}
