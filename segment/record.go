package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	headerKeyHashFieldLength = 8
)

type Record interface {
	// Key returns an unique identifier for the record
	Key() uint64
	// SetKey sets the key on the record
	SetKey(_key uint64)
	// Encode returns the hole payload of the record as byte slice.
	// The record Header (key and payload size) is not included.
	Encode() (_payload []byte, err error)
	// Decode receives the key and the payload byte slice which can be used to equip the record
	Decode(_payload []byte) error
}

func EncodeFields(_record Record) ([]byte, error) {
	if _record.Key() == 0 {
		return nil, fmt.Errorf("key must be greater than zero")
	}
	return EncodeUint64(_record.Key()), nil
}

func DecodeFields(_record Record, _raw []byte) ([]byte, error) {
	key, err := decodeKey(_raw[:headerKeyHashFieldLength])
	if err != nil {
		return nil, err
	}
	_record.SetKey(key)
	return _raw[headerKeyHashFieldLength:], nil
}

// CreateRecord returns a record instance filled with the given data
func CreateRecord(_key uint64, _data []byte) Record {
	return &record{
		key:  _key,
		data: _data,
	}
}

type record struct {
	key  uint64
	data []byte
}

func (r *record) Key() uint64 {
	return r.key
}

func (r *record) SetKey(_key uint64) {
	r.key = _key
}

// Encode .
func (r *record) Encode() ([]byte, error) {
	raw, err := EncodeFields(r)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(raw)
	buf.Write(r.data)
	return buf.Bytes(), nil
}

// Decode .
func (r *record) Decode(_raw []byte) error {
	data, err := DecodeFields(r, _raw)
	if err != nil {
		return err
	}
	r.data = data
	return nil
}

func decodeKey(_rawKey []byte) (uint64, error) {
	if len(_rawKey) != headerKeyHashFieldLength {
		return 0, fmt.Errorf("key decoding needs exactly %d bytes", headerKeyHashFieldLength)
	}
	key := binary.LittleEndian.Uint64(_rawKey)
	if key == 0 {
		return 0, fmt.Errorf("key is zero")
	}
	return key, nil
}
