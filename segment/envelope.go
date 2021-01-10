package segment

// RecordEnvelope is a wrapper objects for streamed records via a channel.
// if a record couldn't be read, the envelope contains an error
type RecordEnvelope struct {
	Offset int64
	Err    error
	Record Record
}

// HeaderEnvelope is a wrapper objects for streamed headers via a channel.
// if a Header couldn't be read, the envelope contains an error.
// the envelope could also contains parts of the data if a padding was provided
type HeaderEnvelope struct {
	Offset      int64
	Header      Header
	PaddingData []byte
	Err         error
}
