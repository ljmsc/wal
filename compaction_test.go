package wal

import "testing"

func TestCompactionSetup(t *testing.T) {
	comp, err := setupCompaction(CompactionConfig{
		Trigger:    TriggerManually,
		Strategy:   StrategyKeepLatest,
		KeepAmount: 1,
	},
		map[uint64]uint64{},
		map[uint64]bool{},
	)
	if err != nil {
		t.Fatalf("can't setup compaction: %v", err)
	}

	if _, ok := comp.(*StrategyKeepLatestCompaction); !ok {
		t.Fatalf("wrong type of compaction")
	}
}
