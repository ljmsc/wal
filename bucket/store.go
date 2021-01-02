package bucket

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ljmsc/wal/segment"
)

const (
	MetaNameSize       = 8
	SegmentListKeyName = "segment_list"
)

type store struct {
	// name of the segment file
	name string
	// segment is the segment file to store segment names
	segment segment.Segment
	// segmentNames is the list of segment names in the record
	segmentNames []string
}

// open .
func openStore(name string) (*store, error) {
	s := store{name: name}

	var err error
	s.segment, err = segment.Open(name)
	if err != nil {
		return nil, fmt.Errorf("can't open chain store segment file: %w", err)
	}

	s.segmentNames = make([]string, 0, 5)
	segmentListRecord := segment.Record{}
	if err := s.segment.ReadByKey([]byte(SegmentListKeyName), false, &segmentListRecord); err != nil {
		if !errors.Is(err, segment.RecordNotFoundErr) {
			return nil, fmt.Errorf("can't read segment list from store file: %w", err)
		}
	}

	listBytes := segmentListRecord.Data
	for {
		if len(listBytes) < MetaNameSize {
			break
		}
		nameSize := binary.LittleEndian.Uint64(listBytes[:MetaNameSize])
		listBytes = listBytes[MetaNameSize:]

		if uint64(len(listBytes)) < nameSize {
			return nil, fmt.Errorf("not enough bytes in segment list record. name size: %d bytes: %d", nameSize, len(listBytes))
		}

		segmentName := string(listBytes[:nameSize])
		s.segmentNames = append(s.segmentNames, segmentName)
		listBytes = listBytes[nameSize:]
	}

	return &s, nil
}

func (s *store) get() []string {
	return s.segmentNames
}

func (s *store) update(nameList []string) error {
	segmentListRecordData := make([]byte, 0, len(nameList)*50)
	for _, segmentName := range nameList {
		segmentNameBytes := []byte(segmentName)
		nameSize := uint64(len(segmentNameBytes))

		nameSizeBytes := make([]byte, MetaNameSize)
		binary.LittleEndian.PutUint64(nameSizeBytes, nameSize)

		segmentListRecordData = append(segmentListRecordData, nameSizeBytes...)
		segmentListRecordData = append(segmentListRecordData, segmentNameBytes...)
	}

	if _, err := s.segment.Write([]byte(SegmentListKeyName), segmentListRecordData); err != nil {
		return fmt.Errorf("can't write segment name list to store: %w", err)
	}

	s.segmentNames = nameList
	return nil
}

func (s *store) close() error {
	return s.segment.Close()
}
