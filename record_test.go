package wal

import (
	"bytes"
	"testing"

	"github.com/matryer/is"
)

func TestRecordMarshal(t *testing.T) {
	is := is.New(t)

	payloadVal := []byte("this is my payload")
	sizeVal := uint64(len(payloadVal))
	checksumVal := sum(payloadVal)

	_r := record{
		payload: payloadVal,
	}

	raw, err := _r.marshal()
	is.NoErr(err)

	buff := bytes.Buffer{}
	buff.Write(encodeUint64(sizeVal))
	buff.Write(encodeUint64(checksumVal))
	buff.Write(payloadVal)
	is.Equal(raw, buff.Bytes())
}

func TestRecordUnmarshal(t *testing.T) {
	is := is.New(t)
	payloadVal := []byte("this is my payload")
	sizeVal := uint64(len(payloadVal))
	checksumVal := sum(payloadVal)

	buff := bytes.Buffer{}
	buff.Write(encodeUint64(sizeVal))
	buff.Write(encodeUint64(checksumVal))
	buff.Write(payloadVal)
	raw := buff.Bytes()

	_r := record{}
	err := _r.unmarshalMetadata(raw[:recordMetadataLength])
	is.NoErr(err)

	err = _r.unmarshalPayload(raw[recordMetadataLength:])
	is.NoErr(err)

	is.Equal(_r.size, sizeVal)
	is.Equal(_r.checksum, checksumVal)
	is.Equal(uint64(len(_r.payload)), _r.size)
}

func TestRecordBlockC(t *testing.T) {
	is := is.New(t)

	payloadVal := []byte("this is my payload")
	blksize := int64(1024)

	_r := record{
		payload: payloadVal,
	}

	is.Equal(_r.blockC(blksize), int64(1))
}
