package chain

import (
	"testing"

	"github.com/matryer/is"
)

var storeTestDir = "../tmp/store/"

func TestOpenStore(t *testing.T) {
	is := is.New(t)
	prepare(storeTestDir)
	defer cleanup(storeTestDir)
	store, err := openStore(storeTestDir + "test_open")
	is.NoErr(err)
	is.True(store != nil)

	err = store.close()
	is.NoErr(err)
}

func TestUpdateAndGetStore(t *testing.T) {
	is := is.New(t)
	prepare(storeTestDir)
	defer cleanup(storeTestDir)
	testSegmentNames := []string{"segment1", "segment2", "segment3"}
	store, err := openStore(storeTestDir + "test_update_get")
	is.NoErr(err)
	is.True(store != nil)

	err = store.update(testSegmentNames)
	is.NoErr(err)

	is.Equal(uint64(3), store.length())

	is.Equal(testSegmentNames, store.get())

	err = store.close()
	is.NoErr(err)

	store2, err := openStore(storeTestDir + "test_update_get")
	is.NoErr(err)
	is.Equal(uint64(3), store2.length())

	is.Equal(testSegmentNames, store2.get())
}
