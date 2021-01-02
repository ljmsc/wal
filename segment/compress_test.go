package segment

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var compressTestDir = "../tmp/compress/"

func TestCompressSegmentToFile(t *testing.T) {
	prepare(compressTestDir)
	defer cleanup(compressTestDir)
	seg := createTestSegment("segment_test2", compressTestDir)
	defer seg.Close()

	targetSeg := createTestSegment("segment_test2_target", compressTestDir)
	defer targetSeg.Close()

	for i := 1; i <= 20; i++ {
		keySuffix := ((i - 1) % 5) + 1
		testKey := []byte("test_key_" + strconv.Itoa(keySuffix))
		testData := []byte("test_data_" + strconv.Itoa(i))

		_, err := seg.Write(testKey, testData)
		assert.NoError(t, err)
	}

	err := CompressToFile(seg, targetSeg)
	assert.NoError(t, err)

	assert.EqualValues(t, uint64(5), targetSeg.Count())
	fmt.Println(targetSeg.Count())
}
