package segment

import (
	"testing"

	"github.com/matryer/is"
)

func TestEncodeDecodeHeader(t *testing.T) {
	is := is.New(t)

	defaultHeader := Header{
		Key:         1,
		PayloadSize: 1,
	}

	rawHeader, encErr := encodeHeader(defaultHeader)
	is.NoErr(encErr)

	targetDefaultHeader := Header{}

	decErr := decodeHeader(&targetDefaultHeader, rawHeader)
	is.NoErr(decErr)

	is.Equal(targetDefaultHeader.Key, defaultHeader.Key)
	is.Equal(targetDefaultHeader.PayloadSize, defaultHeader.PayloadSize)
}

func TestEncodeDecode(t *testing.T) {
	is := is.New(t)

	defaultRecord := record{}
	defaultRecord.SetKey(1)
	defaultRecord.SetData([]byte("this is my payload"))

	rawRecord, encErr := encode(&defaultRecord)
	is.NoErr(encErr)

	targetRecord := record{}

	decErr := decode(&targetRecord, rawRecord)
	is.NoErr(decErr)
}
