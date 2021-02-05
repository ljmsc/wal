package wal

import (
	"bytes"
	"testing"

	"github.com/matryer/is"
)

func TestHeaderMarshal(t *testing.T) {
	is := is.New(t)
	header := header{
		Version: 1,
		Page:    1024,
		Block:   256,
		Size:    1024 * 1024,
	}

	raw, err := header.marshal()
	is.NoErr(err)
	is.Equal(len(raw), headerLength)
}

func TestHeaderMarshalInvalid(t *testing.T) {
	is := is.New(t)
	header := header{
		Version: 1,
		Page:    1024,
		Block:   250,
		Size:    1024 * 1024,
	}

	_, err := header.marshal()
	is.True(err != nil)
}

func TestHeaderUnmarshal(t *testing.T) {
	is := is.New(t)
	verVal := 1
	pageVal := int64(1024)
	blockVal := int64(256)
	sizeVal := int64(1024 * 1024)

	buff := bytes.Buffer{}
	buff.WriteByte(byte(verVal))
	buff.Write(encodeInt64(pageVal))
	buff.Write(encodeInt64(blockVal))
	buff.Write(encodeInt64(sizeVal))

	header := header{}
	err := header.unmarshal(buff.Bytes())
	is.NoErr(err)
	is.Equal(header.Version, formatVersion(verVal))
	is.Equal(header.Page, pageVal)
	is.Equal(header.Block, blockVal)
	is.Equal(header.Size, sizeVal)
}
