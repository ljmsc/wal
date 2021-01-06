package bucket

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ljmsc/wal/segment"
)

type storeRecord struct {
	segmentNames []string
}

func (s storeRecord) Key() uint64 {
	// All records should have the same key
	return 1
}

func (s storeRecord) Encode() ([]byte, error) {
	segmentNames := strings.Join(s.segmentNames, ",")
	buf := bytes.Buffer{}
	_, err := buf.WriteString(segmentNames)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s storeRecord) Decode(_ uint64, _payload []byte) error {
	buf := bytes.NewBuffer(_payload)
	s.segmentNames = strings.Split(buf.String(), ",")
	return nil
}

type store struct {
	// name of the segment file
	name string
	// segment is the segment file to store segment names
	segment segment.Segment
	// latestRecord .
	latestRecord *storeRecord
}

// open .
func openStore(name string) (*store, error) {
	s := store{name: name}

	var err error
	s.segment, err = segment.Open(name, nil)
	if err != nil {
		return nil, fmt.Errorf("can't open segment name list store segment file: %w", err)
	}

	if len(s.segment.Offsets()) > 0 {
		r := storeRecord{}
		if err := s.segment.ReadAt(&r, s.segment.Offset()); err != nil {
			return nil, fmt.Errorf("can't read latest list of segment names from store file: %w", err)
		}
	}

	return &s, nil
}

func (s *store) get() []string {
	if s.latestRecord == nil {
		return []string{}
	}
	return s.latestRecord.segmentNames
}

func (s *store) update(nameList []string) error {
	r := storeRecord{segmentNames: nameList}
	if _, err := s.segment.Write(&r); err != nil {
		return err
	}
	s.latestRecord = &r
	return nil
}

func (s *store) close() error {
	return s.segment.Close()
}
