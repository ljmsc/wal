package wal

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
)

// Key is the key of a record
type Key []byte

// HashSum64 .
func (k Key) HashSum64() uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write(k)
	return hash.Sum64()
}

// Config for a write ahead DiskWal
type Config struct {
	// max size of a segmentFile file
	// default = 100 MByte
	SegmentMaxSizeBytes int64

	// path to segmentFile files on disk. new segmentFile files will be created here. existing segmentFile files are read
	SegmentFileDir string

	// defines the initial size of the slice for a given key
	// default = 10
	SegmentIndexTableAlloc uint64

	// defines the initial size of the slice to collect records for a given key. This value is multiplied with the amount of segments
	// default = 10
	RecordCollectionSliceAlloc uint64

	// prefix for each segmentFile file. the full segmentFile name is the prefix plus an increasing integer
	// default = "seg"
	SegmentFilePrefix string

	// settings for log compaction
	Compaction CompactionConfig
}

// Validate validates the config
func (c Config) Validate() error {
	if c.SegmentFileDir == "" {
		return errors.New("segmentFile file dir must be set")
	}

	return nil
}

/*
TODO:
	- compaction
		* deletion
	- testing

*/

// Wal is the interface for the write ahead log
type Wal interface {
	SegmentAmount() uint64
	// Write data for a given key to the DiskWal
	Write(record *Record) error
	CompareAndWrite(version uint64, record *Record) error
	ReadLatest(key Key, record *Record) error
	ReadAll(key Key) ([]Record, error)
	ReadSequenceNum(sequenceNum uint64, record *Record) error
	Delete(key Key) error
	Compact() error
	Close() error
	Remove() error
}

// Bootstrap a DiskWal. existing segmentFile files are read. it is not safe to run Bootstrap on the same directory multiple times
func Bootstrap(config Config) (Wal, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	l := DiskWal{
		config: config,
		mutex:  sync.RWMutex{},
	}

	// set default values for unset config parameters
	if l.config.SegmentMaxSizeBytes == 0 {
		l.config.SegmentMaxSizeBytes = 100e6
	}

	if l.config.SegmentIndexTableAlloc == 0 {
		l.config.SegmentIndexTableAlloc = 10
	}

	if l.config.SegmentFilePrefix == "" {
		l.config.SegmentFilePrefix = "seg"
	}

	l.config.SegmentFileDir = strings.TrimRight(l.config.SegmentFileDir, "/")

	// check if dir exists
	info, err := os.Stat(l.config.SegmentFileDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		// create the dir
		if err := os.MkdirAll(l.config.SegmentFileDir, os.ModeDir|0700); err != nil {
			return nil, err
		}

	} else if !info.IsDir() {
		// file exists but is not a directory
		return nil, errors.New("SegmentFileDir exists but is not a directory")
	}

	// scan segmentFile dir for existing files
	if err := l.scan(); err != nil {
		_ = l.Close()
		return nil, err
	}

	if len(l.segments) < 1 {
		if err := l.addSegment(); err != nil {
			return nil, err
		}
	}

	return &l, nil
}

// DiskWal is the write ahead log implementation
type DiskWal struct {
	// the DiskWal configuration
	config Config
	// a list of segmentFile files on disk
	segments []segment
	// the mutex for the segments list to avoid concurrent writes
	mutex sync.RWMutex
	// the mutex to lock the write function
	writeMutex sync.Mutex
	// a list of all consumers for record changes
	consumers []chan Record
	// closed is false until the Close() method is called
	closed bool
	// versions stores the latest version for a given key (hash)
	versions map[uint64]uint64
	// the mutex to lock the compaction to avoid running multiple compaction processes
	compactionMutex sync.Mutex
	// deleteMarker stores records with are marked for deletion
	deleteMarker map[uint64]bool
}

func (l *DiskWal) SegmentAmount() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return uint64(len(l.segments))
}

// Write data for a given key to the DiskWal
func (l *DiskWal) Write(record *Record) error {
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()
	if l.closed {
		return ErrWalClosed
	}

	keyHash := record.Key.HashSum64()
	l.mutex.RLock()
	if _, ok := l.versions[keyHash]; !ok {
		l.versions[keyHash] = 0
	}
	record.meta.version = l.versions[keyHash] + 1
	l.mutex.RUnlock()

	for {
		segment := l.currentSegment()
		err := segment.Write(record)
		if err != nil && err != ErrSegmentFileClosed {
			return fmt.Errorf("could not write to segment file: %w", err)
		}
		if err == ErrSegmentFileClosed {
			if err := l.addSegment(); err != nil {
				return fmt.Errorf("could not add new segement file: %w", err)
			}
		}
		if err == nil {
			// successful written to disk
			break
		}
	}

	l.mutex.Lock()
	// increase version if write was successful
	l.versions[keyHash]++
	l.mutex.Unlock()
	return nil
}

// CompareAndWrite writes the data only if version is equal to the latest version on disk. if no record exists yet. version is ignored
func (l *DiskWal) CompareAndWrite(version uint64, record *Record) error {
	l.mutex.RLock()
	keyHash := record.Key.HashSum64()
	if currentVersion, ok := l.versions[keyHash]; ok {
		if version != currentVersion {
			l.mutex.RUnlock()
			return errors.New("version is not equal to current version on disk")
		}
	}
	l.mutex.RUnlock()
	return l.Write(record)
}

// ReadLatest reads the latest version of a record for a given key
func (l *DiskWal) ReadLatest(key Key, record *Record) error {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if _, ok := l.versions[key.HashSum64()]; !ok {
		return ErrNoRecordFound
	}
	latestVersion := l.versions[key.HashSum64()]
	if l.closed {
		return ErrWalClosed
	}
	for i := len(l.segments) - 1; i >= 0; i-- {
		segment := l.segments[i]
		err := segment.ReadLatest(key, record)
		if err == ErrNoRecordFound {
			continue
		}
		if err != nil {
			return fmt.Errorf("could not read latest record for key %s: %w", key, err)
		}

		if record.Version() < latestVersion {
			continue
		}

		return nil
	}
	return ErrNoRecordFound
}

// ReadAll reads all versions of a record for a given key
func (l *DiskWal) ReadAll(key Key) ([]Record, error) {
	//todo: this is O(n^2). can we do better?
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if l.closed {
		return nil, ErrWalClosed
	}
	segSize := len(l.segments)
	size := uint64(segSize) * l.config.RecordCollectionSliceAlloc
	records := make([]Record, 0, size)
	for _, segment := range l.segments {
		offsets, err := segment.Offsets(key)
		if err != nil {
			if err != ErrNoRecordFound {
				return nil, err
			}
			continue
		}
		for _, offset := range offsets {
			var record Record
			if err := segment.ReadOffset(offset, &record); err != nil {
				return nil, err
			}
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		return records, ErrNoRecordFound
	}
	return records, nil
}

// ReadSequenceNum reads a record by sequence number
func (l *DiskWal) ReadSequenceNum(sequenceNum uint64, record *Record) error {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if l.closed {
		return ErrWalClosed
	}
	for _, segment := range l.segments {
		startSeqNum, endSeqNum := segment.SequenceBoundaries()
		if startSeqNum > sequenceNum || endSeqNum < sequenceNum {
			continue
		}
		if err := segment.ReadSequenceNum(sequenceNum, record); err != nil {
			return err
		}
		break
	}
	return nil
}

func (l *DiskWal) Compact() error {
	switch l.config.Compaction.Trigger {
	case TriggerManually:
		return l.compact()
	default:
		return errors.New("compaction trigger is not manually")
	}
}

// Delete will mark a record for deletion. this function will not delete all records but create a delete record.
// with the next log compaction all entries will be deleted
func (l *DiskWal) Delete(key Key) error {
	l.mutex.Lock()
	l.deleteMarker[key.HashSum64()] = true
	l.mutex.Unlock()
	record := Record{
		Key:  key,
		Data: []byte{},
	}
	return l.Write(&record)
}

// Close will close all open file handler
func (l *DiskWal) Close() error {
	var err error
	for _, segment := range l.segments {
		err = segment.Close()
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.closed = true
	return err
}

// Destroy deletes all segmentFile file
func (l *DiskWal) Remove() error {
	for _, segment := range l.segments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	return nil
}

func (l *DiskWal) currentSegment() segment {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if l.segments == nil {
		return nil
	}

	if len(l.segments) < 1 {
		return nil
	}
	seg := l.segments[len(l.segments)-1]
	return seg
}

func (l *DiskWal) addSegment() error {
	currentSegment := l.currentSegment()
	if currentSegment != nil && currentSegment.IsWritable() {
		return nil
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var segmentNum, latestSeqNum uint64
	if currentSegment != nil {
		segmentNum = currentSegment.Num()
		_, latestSeqNum = currentSegment.SequenceBoundaries()
	}
	newSegment, err := createSegment(segmentNum+1, latestSeqNum+1, l.config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, newSegment)
	return nil
}

func (l *DiskWal) compact() error {
	// first copy all necessary data to avoid long locking
	l.mutex.RLock()
	readOnlySegments := make([]segment, 0, len(l.segments))
	for _, segment := range l.segments {
		if segment.IsWritable() {
			continue
		}
		readOnlySegments = append(readOnlySegments, segment)
	}
	currentVersions := make(map[uint64]uint64)
	for hash, version := range l.versions {
		currentVersions[hash] = version
	}

	currentDeletions := make(map[uint64]bool)
	for hash := range l.deleteMarker {
		currentDeletions[hash] = true
	}
	l.mutex.RUnlock()

	l.compactionMutex.Lock()
	defer l.compactionMutex.Unlock()

	compactor, err := setupCompaction(l.config.Compaction, currentVersions, currentDeletions)
	if err != nil {
		return fmt.Errorf("can't setup compaction: %w", err)
	}

	var tempSegmentNum uint64 = 1
	var currentCompactionSegment segment
	compactionSegments := make([]segment, 0, len(readOnlySegments))
	deletableSegments := make(map[uint64]segment)
	for _, segment := range readOnlySegments {
		keepOffsets, isCompactable := compactor.Scan(segment)
		if !isCompactable {
			continue
		}
		deletableSegments[segment.Num()] = segment
		for _, offset := range keepOffsets {
			record := Record{}
			if err := segment.ReadOffset(offset, &record); err != nil {
				return fmt.Errorf("can't read record from offset: %w", err)
			}
			for {
				if currentCompactionSegment == nil {
					currentCompactionSegment, err = createSegmentForCompaction(tempSegmentNum, record.SequenceNumber(), l.config)
					if err != nil {
						return fmt.Errorf("can't create compaction segment file: %w", err)
					}
				}
				if err := currentCompactionSegment.Write(&record); err != nil {
					if errors.Is(err, ErrSegmentFileClosed) {
						// compaction segment file is closed.
						compactionSegments = append(compactionSegments, currentCompactionSegment)
						tempSegmentNum++
						currentCompactionSegment = nil
						continue
					} else {
						return fmt.Errorf("can't write record to segment: %w", err)
					}
				}
				break
			}
		}
	}

	currentSegment := l.currentSegment()
	l.mutex.Lock()
	var segmentNum uint64 = 0
	if currentSegment != nil {
		segmentNum = currentSegment.Num()
	}

	for _, compSegment := range compactionSegments {
		if err := compSegment.FinishCompaction(segmentNum + 1); err != nil {
			l.mutex.Unlock()
			return fmt.Errorf("can't convert compaction segment to regular segment: %w", err)
		}
		regularSegment, err := parseSegmentByNumber(segmentNum+1, l.config)
		if err != nil {
			l.mutex.Unlock()
			return fmt.Errorf("can't parse new segment file by segment number: %w", err)
		}
		l.segments = append(l.segments, regularSegment)
		segmentNum++
	}
	if currentSegment != nil {
		if err := currentSegment.CloseForWriting(); err != nil {
			l.mutex.Unlock()
			return fmt.Errorf("can't close current write segment file: %w", err)
		}
	}

	newSegmentsList := make([]segment, 0, len(l.segments))
	for _, segment := range l.segments {
		if _, ok := deletableSegments[segment.Num()]; !ok {
			newSegmentsList = append(newSegmentsList, segment)
		}
	}
	l.segments = newSegmentsList
	l.mutex.Unlock()

	for _, segment := range deletableSegments {
		if err := segment.Remove(); err != nil {
			return fmt.Errorf("can't remove old segment file: %w", err)
		}
	}

	return nil
}

func (l *DiskWal) scan() error {
	info, err := os.Stat(l.config.SegmentFileDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", l.config.SegmentFileDir)
	}

	fileInfos, err := ioutil.ReadDir(l.config.SegmentFileDir)
	if err != nil {
		return err
	}

	segmentFilenames := make([]string, 0, len(fileInfos))
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		filename := fileInfo.Name()

		if !strings.Contains(filename, l.config.SegmentFilePrefix) {
			continue
		}

		segmentFilenames = append(segmentFilenames, filename)
	}

	sort.Strings(segmentFilenames)
	l.segments = make([]segment, 0, len(segmentFilenames))
	l.versions = make(map[uint64]uint64)
	l.deleteMarker = make(map[uint64]bool)
	i := 0
	for _, filename := range segmentFilenames {
		segment, err := parseSegment(filename, l.config)
		if err != nil {
			return err
		}
		if (i + 1) < len(segmentFilenames) {
			if err := segment.CloseForWriting(); err != nil {
				return err
			}
		}
		versions := segment.RecordVersions()
		for hash, version := range versions {
			if _, ok := l.versions[hash]; !ok {
				l.versions[hash] = 0
			}
			if l.versions[hash] < version {
				l.versions[hash] = version
			}
		}
		l.segments = append(l.segments, segment)
		i++
	}
	return nil
}
