package segment

import "encoding"

type Record interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type BasicRecord struct {
	Data []byte
}

func (b *BasicRecord) MarshalBinary() (data []byte, err error) {
	return b.Data, nil
}

func (b *BasicRecord) UnmarshalBinary(data []byte) error {
	b.Data = data
	return nil
}
