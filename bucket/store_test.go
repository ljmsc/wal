package bucket

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var storeTestDir = "../tmp/store/"

func TestOpenStore(t *testing.T) {
	prepare(storeTestDir)
	defer cleanup(storeTestDir)
	store, err := openStore(storeTestDir + "test_open")
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotNil(t, store) {
		return
	}

	err = store.close()
	assert.NoError(t, err)
}

func TestUpdateAndGetStore(t *testing.T) {
	prepare(storeTestDir)
	defer cleanup(storeTestDir)
	testSegmentNames := []string{"segment1", "segment2", "segment3"}
	store, err := openStore(storeTestDir + "test_update_get")
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotNil(t, store) {
		return
	}

	err = store.update(testSegmentNames)
	assert.NoError(t, err)

	segmentNames := store.get()
	assert.EqualValues(t, testSegmentNames, segmentNames)

	err = store.close()
	assert.NoError(t, err)
}
