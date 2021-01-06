package segment

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/matryer/is"
)

var compressTestDir = "../tmp/compress/"

func TestCompressSegmentToFile(t *testing.T) {
	is := is.New(t)
	prepare(compressTestDir)
	defer cleanup(compressTestDir)
	seg := createTestSegment("segment_test2", compressTestDir)
	defer seg.Close()

	targetSeg := createTestSegment("segment_test2_target", compressTestDir)
	defer targetSeg.Close()

	for i := 1; i <= 20; i++ {
		testKey := uint64(((i - 1) % 5) + 1)
		testData := []byte("test_data_" + strconv.Itoa(i))

		_, err := seg.Write(createRecord(testKey, testData))
		is.NoErr(err)
	}

	err := CompressToFile(seg, targetSeg)
	is.NoErr(err)

	length := len(targetSeg.Offsets())
	is.Equal(5, length)
	fmt.Println(length)
}
