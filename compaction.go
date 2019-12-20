package wal

import (
	"errors"
	"fmt"
	"time"
)

type CompactionTriggerType int8

const (
	TriggerNone     CompactionTriggerType = iota // no compaction at all
	TriggerManually                              // compaction is manually triggered with the wal.Compact() function
	TriggerTime                                  // compaction is triggered in time intervals
)

type CompactionStrategyType int8

const (
	StrategyKeepLatest CompactionStrategyType = iota // At least the latest version for unique key will be kept
	StrategyExpire                                   // all records which are older then time.Now - ExpirationThreshold will be deleted
)

type CompactionConfig struct {
	// define how the log compaction should be triggered
	// default = TriggerNone
	Trigger CompactionTriggerType

	// this is the compaction interval when compaction trigger is TriggerTime
	TriggerInterval time.Duration

	// define the strategy for log compaction
	// default = StrategyNone
	Strategy CompactionStrategyType

	// this is the amount of records per key which will be kept when compaction strategy is StrategyKeepLatest
	KeepAmount uint64

	// all record which are older than time.Now - ExpirationThreshold will be deleted when compaction strategy is StrategyExpire
	ExpirationThreshold time.Duration
}

type compaction interface {
	Validate() error
	Scan(segment) ([]int64, bool)
}

func setupCompaction(config CompactionConfig, versions map[uint64]uint64, deletions map[uint64]bool) (compaction, error) {
	var compaction compaction
	switch config.Strategy {
	case StrategyKeepLatest:
		compaction = &StrategyKeepLatestCompaction{
			config:    config,
			versions:  versions,
			deletions: deletions,
		}
	case StrategyExpire:
		compaction = &StrategyExpireCompaction{
			config:    config,
			versions:  versions,
			deletions: deletions,
		}
	default:
		return nil, fmt.Errorf("compaction strategy is not supported: %d", config.Strategy)
	}

	if err := compaction.Validate(); err != nil {
		return nil, fmt.Errorf("compaction is not valid: %w", err)
	}
	return compaction, nil
}

// StrategyKeepLatestCompaction .
type StrategyKeepLatestCompaction struct {
	config    CompactionConfig
	versions  map[uint64]uint64
	deletions map[uint64]bool
}

// Setup for StrategyKeepLatestCompaction
func (s *StrategyKeepLatestCompaction) Validate() error {
	if s.config.KeepAmount == 0 {
		return errors.New("CompactionConfig.KeepAmount must be greater than zero")
	}
	return nil
}

// Scan returns a list of offsets to keep and if more than half of the records in the segment can be deleted
func (s *StrategyKeepLatestCompaction) Scan(segment segment) ([]int64, bool) {
	seqFirst, seqLast := segment.SequenceBoundaries()
	recordCount := seqLast - seqFirst

	segVersions := segment.RecordVersions()
	keepOffsets := make([]int64, 0, recordCount)
	for hash, version := range segVersions {
		if _, ok := s.deletions[hash]; ok {
			// if record is marked for deletion no further checks are applied
			continue
		}
		if _, ok := s.versions[hash]; !ok {
			// if record hash is unknown in global versions list the record can be deleted
			continue
		}
		if version <= (s.versions[hash] - s.config.KeepAmount) {
			// record version in this segment is old enough to delete
			continue
		}

		offsets, err := segment.OffsetsByHash(hash)
		if err != nil {
			// no offsets stores for record hash. this should not happen.
			continue
		}

		// amount of offsets to keep from the offset list
		offsetAmount := s.config.KeepAmount - (s.versions[hash] - version)
		offsetLen := uint64(len(offsets))
		if offsetLen < offsetAmount {
			// if offset list is small than offsetAmount, keep all existing offsets
			offsetAmount = offsetLen
		}
		startIndex := offsetLen - offsetAmount

		for i := startIndex; i < offsetLen; i++ {
			keepOffsets = append(keepOffsets, offsets[i])
		}
	}

	if (recordCount / 2) > uint64(len(keepOffsets)) {
		return keepOffsets, true
	}
	return keepOffsets, false
}

// StrategyExpireCompaction .
type StrategyExpireCompaction struct {
	config    CompactionConfig
	versions  map[uint64]uint64
	deletions map[uint64]bool
}

// Setup for StrategyExpireCompaction
func (s *StrategyExpireCompaction) Validate() error {
	if s.config.ExpirationThreshold == 0 {
		return errors.New("CompactionConfig.ExpirationThreshold must be greater than zero")
	}

	return nil
}

// Scan returns a list of offsets to keep and if more than half of the records in the segment can be deleted
func (s *StrategyExpireCompaction) Scan(segment segment) ([]int64, bool) {
	seqFirst, seqLast := segment.SequenceBoundaries()
	recordCount := seqLast - seqFirst

	compactTime := time.Now().Add(-s.config.ExpirationThreshold)

	keepOffsets := make([]int64, 0, recordCount)

	for i := seqLast; i >= seqFirst; i-- {
		record := Record{}
		if err := segment.ReadSequenceNum(i, &record); err != nil {
			continue
		}

		if record.CreatedAt().Before(compactTime) {
			// record is older than (Now - ExpirationThreshold)
			continue
		}
		keepOffsets = append(keepOffsets, record.Offset())
	}

	if (recordCount / 2) > uint64(len(keepOffsets)) {
		return keepOffsets, true
	}
	return keepOffsets, false
}
