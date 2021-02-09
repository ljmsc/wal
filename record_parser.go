package wal

import (
	"bytes"
	"fmt"
)

var (
	errRecordNotComplete = fmt.Errorf("record not complete")
)

type recordParser struct {
	buff    bytes.Buffer
	record  record
	block   int64
	offset  int64
	roffset int64
}

func createRecordParser(_block int64) recordParser {
	return recordParser{
		buff:   bytes.Buffer{},
		record: record{},
		block:  _block,
	}
}

func (r *recordParser) len() uint64 {
	return uint64(r.buff.Len())
}

func (r *recordParser) check() (bool, error) {
	if r.record.isValid() {
		return true, nil
	}

	if !r.record.isMetaValid() {
		if r.len() < recordMetadataLength {
			return false, nil
		}

		r.roffset = r.offset
		metaData := make([]byte, recordMetadataLength)
		n, err := r.buff.Read(metaData)
		if err != nil {
			return false, fmt.Errorf("can't read from buffer: %w", err)
		}
		if err := r.record.unmarshalMetadata(metaData); err != nil {
			return false, err
		}
		r.offset += int64(n)
	}

	if r.block <= 0 {
		return false, fmt.Errorf("invalid block size %d", r.block)
	}

	readDataSize := uint64((r.record.blockC(r.block) * (r.block)) - recordMetadataLength)
	if r.len() < readDataSize {
		return false, nil
	}

	payloadData := make([]byte, readDataSize)
	n, err := r.buff.Read(payloadData)
	if err != nil {
		return false, fmt.Errorf("can't read from buffer: %w", err)
	}

	if err := r.record.unmarshalPayload(payloadData); err != nil {
		return false, err
	}
	r.offset += int64(n)
	return true, nil
}

func (r *recordParser) write(p []byte) error {
	if _, err := r.buff.Write(p); err != nil {
		return fmt.Errorf("can't write to buffer: %w", err)
	}
	return nil
}

func (r *recordParser) read(_record *record) (int64, error) {
	complete, err := r.check()
	if err != nil {
		return 0, err
	}
	if !complete {
		return 0, errRecordNotComplete
	}

	*_record = r.record
	r.record = record{}
	if r.len() == 0 {
		r.buff.Reset()
	}
	return r.roffset, nil
}
