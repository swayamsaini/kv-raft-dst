package sim

import (
	"testing"
)

func TestLinearizabilityCheckerCatchesStaleRead(t *testing.T) {
	// Valid history: Put(x=1) [10..30], Get(x=1) [20..40], Get(x=1) [50..60]
	valid := []HistoryOp{
		{Op: OpPut, Key: "x", Value: "1", InvokeTime: 10, ReturnTime: 30},
		{Op: OpGet, Key: "x", Value: "1", Found: true, InvokeTime: 20, ReturnTime: 40},
		{Op: OpGet, Key: "x", Value: "1", Found: true, InvokeTime: 50, ReturnTime: 60},
	}
	if ok, reason := CheckLinearizable(valid); !ok {
		t.Fatalf("valid history rejected: %s", reason)
	}

	// Invalid history: Get(x=1) finishes at 40, then Get(x=0/not-found) starts at 50!
	invalid := []HistoryOp{
		{Op: OpPut, Key: "x", Value: "1", InvokeTime: 10, ReturnTime: 30},
		{Op: OpGet, Key: "x", Value: "1", Found: true, InvokeTime: 20, ReturnTime: 40},
		{Op: OpGet, Key: "x", Value: "", Found: false, InvokeTime: 50, ReturnTime: 60},
	}
	if ok, _ := CheckLinearizable(invalid); ok {
		t.Fatalf("linearizability checker failed to catch stale read!")
	}
}

func TestDeterministicSimulationSwarm(t *testing.T) {
	// Run 30 distinct seeded simulations across varied chaos profiles
	// (packet drops, duplicates, reordering, asymmetric partitions, crashes, snapshots).
	profiles := []FaultConfig{
		{DropProb: 0.05, DuplicateProb: 0.05, MinDelayMs: 1, MaxDelayMs: 25, PartitionProb: 0.25, CrashProb: 0.15, SnapshotThreshold: 8},
		{DropProb: 0.15, DuplicateProb: 0.10, MinDelayMs: 2, MaxDelayMs: 50, PartitionProb: 0.35, CrashProb: 0.20, SnapshotThreshold: 5},
		{DropProb: 0.02, DuplicateProb: 0.20, MinDelayMs: 1, MaxDelayMs: 80, PartitionProb: 0.20, CrashProb: 0.25, SnapshotThreshold: 12},
	}

	totalOps := 0
	for i := 0; i < 30; i++ {
		seed := int64(20261124 + i*7919)
		faults := profiles[i%len(profiles)]
		sim := NewSimulator(seed, 5, faults)
		if err := sim.Run(6000); err != nil {
			t.Fatalf("DST failed: %v", err)
		}
		ops, _ := sim.Stats()
		totalOps += ops
	}
	if totalOps < 100 {
		t.Fatalf("expected substantial client traffic across 30 DST runs, got %d ops", totalOps)
	}
	t.Logf("Verified %d linearizable client operations across 30 seeded 5-node DST runs!", totalOps)
}
