package segment

type Record interface {
	// Key returns an unique identifier for the record
	Key() uint64
	// Encode returns the hole payload of the record as byte slice.
	// The record Header (key and payload size) is not included.
	Encode() (_payload []byte, err error)
	// Decode receives the key and the payload byte slice which can be used to equip the record
	Decode(_key uint64, _payload []byte) error
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

func (r *record) SetData(_data []byte) {
	r.data = _data
}

func (r *record) Data() []byte {
	return r.data
}

// Encode .
func (r *record) Encode() ([]byte, error) {
	return r.data, nil
}

// Decode .
func (r *record) Decode(_keyHash uint64, _payload []byte) error {
	r.key = _keyHash
	r.data = _payload
	return nil
}
