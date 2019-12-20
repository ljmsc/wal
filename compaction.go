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
	StrategyKeep   CompactionStrategyType = iota // At least the latest version for unique key will be kept
	StrategyExpire                               // all records which are older then time.Now - ExpirationThreshold will be deleted
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

	// this is the amount of records per key which will be kept when compaction strategy is StrategyKeep
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
	case StrategyKeep:
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

	keepOffsets := make([]int64, 0, recordCount)

	for i := seqFirst; i <= seqLast; i++ {
		record := Record{}
		if err := segment.ReadSequenceNum(i, &record); err != nil {
			continue
		}

		hash := record.Key.HashSum64()
		if _, ok := s.versions[hash]; !ok {
			// if record hash is unknown in global versions list the record can be deleted
			continue
		}

		latestRecordVersion := s.versions[hash]

		// keep deletion marker if there are no newer records
		if !record.DeletionMarker() && record.Version() < latestRecordVersion {
			if _, ok := s.deletions[hash]; ok {
				// if record is marked for deletion no further checks are applied
				continue
			}

			if record.Version() <= (latestRecordVersion - s.config.KeepAmount) {
				// record version in this segment is old enough to delete
				continue
			}
		}

		keepOffsets = append(keepOffsets, record.Offset())
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

		// keep deletion marker
		if !record.DeletionMarker() {
			if record.CreatedAt().Before(compactTime) {
				// record is older than (Now - ExpirationThreshold)
				continue
			}

			if _, ok := s.deletions[record.Key.HashSum64()]; ok {
				// if record is marked for deletion no further checks are applied
				continue
			}
		}

		keepOffsets = append(keepOffsets, record.Offset())
	}

	//todo: revert keepOffsets

	if (recordCount / 2) > uint64(len(keepOffsets)) {
		return keepOffsets, true
	}
	return keepOffsets, false
}
