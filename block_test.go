package wal

import (
	"bytes"
	"hash/fnv"
	"testing"

	"github.com/matryer/is"
)

func TestBlockMarshal(t *testing.T) {
	is := is.New(t)
	payload := []byte("this is the payload")
	block := block{
		First:   true,
		Follow:  0,
		Payload: payload,
	}
	raw, err := block.marshal()
	is.NoErr(err)
	is.Equal(len(raw), blockMetadataLength+len(payload))
}

func TestBlockUnmarshal(t *testing.T) {
	is := is.New(t)
	firstVal := true
	followVal := uint32(0)
	payloadVal := []byte("my payload")
	sizeVal := uint64(len(payloadVal))
	checksumVal := sum(payloadVal)
	buff := bytes.Buffer{}
	buff.WriteByte(1)
	buff.Write(encodeUint32(followVal))
	buff.Write(encodeUint64(sizeVal))
	buff.Write(encodeUint64(checksumVal))
	buff.Write(payloadVal)

	block := block{}
	err := block.unmarshal(buff.Bytes())
	is.NoErr(err)
	is.Equal(block.First, firstVal)
	is.Equal(block.Follow, followVal)
	is.Equal(block.Payload, payloadVal)
}

func TestBlockFragment(t *testing.T) {
	is := is.New(t)
	testSet := 3
	blksize := 32
	payloadSize := blksize - blockMetadataLength
	testData := []byte("this is my long test dataset") // 28 byte
	testDataStrip := make([]byte, len(testData))
	copy(testDataStrip, testData)

	raw := make([]byte, 0, testSet)
	for i := 0; i < testSet; i++ {
		ps := payloadSize
		if len(testDataStrip) < payloadSize {
			ps = len(testDataStrip)
		}
		_data := testDataStrip[:ps]
		_block := block{
			First:   i == 0,
			Follow:  uint32(testSet - (i + 1)),
			Payload: _data,
		}
		rawBlk, err := _block.marshal()
		is.NoErr(err)
		raw = append(raw, rawBlk...)
		testDataStrip = testDataStrip[ps:]
	}

	_blocks, err := fragment(raw, int64(blksize))
	is.NoErr(err)
	is.Equal(len(_blocks), testSet)
	buff := bytes.Buffer{}
	for _, b := range _blocks {
		buff.Write(b.Payload)
	}
	is.Equal(buff.String(), string(testData))
}

func TestBlockDeFragment(t *testing.T) {
	is := is.New(t)
	testSet := 3
	blksize := int64(32)
	payloadSize := blksize - blockMetadataLength
	testData := []byte("this is my long test dataset") // 28 byte
	testDataStrip := make([]byte, len(testData))
	copy(testDataStrip, testData)

	_blocks := make([]block, 0, testSet)
	for i := 0; i < testSet; i++ {
		ps := payloadSize
		if int64(len(testDataStrip)) < payloadSize {
			ps = int64(len(testDataStrip))
		}
		_block := block{
			First:   i == 0,
			Follow:  uint32(testSet - (i + 1)),
			Payload: testDataStrip[:ps],
		}
		_blocks = append(_blocks, _block)
		testDataStrip = testDataStrip[ps:]
	}
	raw, err := deFragment(_blocks, blksize)
	is.NoErr(err)
	is.Equal(int64(len(raw)), (blksize * int64(testSet)))
}

func TestBlockChecksum(t *testing.T) {
	is := is.New(t)
	payload := []byte("this is my long test dataset")
	h := fnv.New64a()
	_, _ = h.Write(payload)
	cs := h.Sum64()

	is.Equal(sum(payload), cs)
	is.True(check(payload, cs))
}
