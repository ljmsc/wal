package wal

import (
	"bytes"
	"testing"

	"github.com/matryer/is"
)

func TestRecordMarshal(t *testing.T) {
	is := is.New(t)

	seqNumVal := uint64(1)
	payloadVal := []byte("this is my payload")
	sizeVal := uint64(len(payloadVal))
	checksumVal := sum(payloadVal)

	_r := record{
		seqNum:  seqNumVal,
		payload: payloadVal,
	}

	raw, err := _r.marshal()
	is.NoErr(err)

	buff := bytes.Buffer{}
	buff.Write(encodeUint64(1))
	buff.Write(encodeUint64(sizeVal))
	buff.Write(encodeUint64(checksumVal))
	buff.Write(payloadVal)
	is.Equal(raw, buff.Bytes())

}

func TestRecordUnmarshal(t *testing.T) {
	is := is.New(t)
	seqNumVal := uint64(1)
	payloadVal := []byte("this is my payload")
	sizeVal := uint64(len(payloadVal))
	checksumVal := sum(payloadVal)

	buff := bytes.Buffer{}
	buff.Write(encodeUint64(1))
	buff.Write(encodeUint64(sizeVal))
	buff.Write(encodeUint64(checksumVal))
	buff.Write(payloadVal)

	_r := record{}
	err := _r.unmarshal(buff.Bytes())
	is.NoErr(err)

	is.Equal(_r.seqNum, seqNumVal)
	is.Equal(_r.size, sizeVal)
	is.Equal(_r.checksum, checksumVal)

}

func TestRecordBlockC(t *testing.T) {
	is := is.New(t)

	seqNumVal := uint64(1)
	payloadVal := []byte("this is my payload")
	blksize := int64(1024)

	_r := record{
		seqNum:  seqNumVal,
		payload: payloadVal,
	}

	is.Equal(_r.blockC(blksize), int64(1))

}
