package segment

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestMetadata(t *testing.T) {
	metadataTest := Metadata(map[string][]byte{})
	metadataTest["item1name"] = []byte("item1data")
	metadataTest["item2name"] = []byte("item2data")

	m := make(map[string][]byte)
	itemCountBytes := make([]byte, MetaMetadataItemCount)
	binary.LittleEndian.PutUint64(itemCountBytes, 2)

	metadataBytes := make([]byte, 0, MetaMetadataItemCount+2*MetaMetadataItemNameSizeField+2*MetaMetadataItemDataSizeField)
	metadataBytes = append(metadataBytes, itemCountBytes...)

	for i := 1; i <= 2; i++ {
		itemName := []byte("item" + strconv.Itoa(i) + "name")
		itemNameSizeBytes := make([]byte, MetaMetadataItemNameSizeField)
		binary.LittleEndian.PutUint64(itemNameSizeBytes, uint64(len(itemName)))

		itemData := []byte("item" + strconv.Itoa(i) + "data")
		itemDataSizeBytes := make([]byte, MetaMetadataItemDataSizeField)
		binary.LittleEndian.PutUint64(itemDataSizeBytes, uint64(len(itemData)))

		metadataBytes = append(metadataBytes, itemNameSizeBytes...)
		metadataBytes = append(metadataBytes, itemName...)
		metadataBytes = append(metadataBytes, itemDataSizeBytes...)
		metadataBytes = append(metadataBytes, itemData...)
	}

	err := ParseMetadata(metadataBytes, m)
	if !assert.NoError(t, err) {
		return
	}

	assert.EqualValues(t, metadataTest, m)
}

func TestMetaPutDataIn(t *testing.T) {
	tm := Metadata{}
	tm.MetaPutString("stringKey", "test123")
	tm.MetaPutUint64("uint64Key", 1337)

	metaBytes := tm.Bytes()

	tmParse := Metadata{}
	err := ParseMetadata(metaBytes, tmParse)
	require.NoError(t, err)

	assert.EqualValues(t, "test123", tmParse.GetString("stringKey"), "String")
	assert.EqualValues(t, uint64(1337), tmParse.GetUint64("uint64Key"), "uint64")
}
