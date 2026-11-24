package txn

import (
	"errors"
	"testing"
)

func TestPercolatorCrossShardAtomicTransferAndSnapshotIsolation(t *testing.T) {
	coord := NewCoordinator(4)

	// Seed Alice=100, Bob=100 across shards.
	if _, err := coord.Execute([]WriteOp{
		{Key: "acct:alice", Value: "100"},
		{Key: "acct:bob", Value: "100"},
	}); err != nil {
		t.Fatalf("seed txn failed: %v", err)
	}

	// Capture a snapshot timestamp BEFORE the transfer.
	beforeTS := coord.StartReadTS()

	// Atomic transfer: Alice -> 50, Bob -> 150.
	if _, err := coord.Execute([]WriteOp{
		{Key: "acct:alice", Value: "50"},
		{Key: "acct:bob", Value: "150"},
	}); err != nil {
		t.Fatalf("transfer txn failed: %v", err)
	}

	// Snapshot reader at beforeTS MUST still see Alice=100, Bob=100!
	aOld, _, _ := coord.ReadAt("acct:alice", beforeTS)
	bOld, _, _ := coord.ReadAt("acct:bob", beforeTS)
	if aOld != "100" || bOld != "100" {
		t.Fatalf("Snapshot Isolation violated: beforeTS saw alice=%q bob=%q", aOld, bOld)
	}

	// Fresh reader sees atomic post-transfer state Alice=50, Bob=150.
	afterTS := coord.StartReadTS()
	aNew, _, _ := coord.ReadAt("acct:alice", afterTS)
	bNew, _, _ := coord.ReadAt("acct:bob", afterTS)
	if aNew != "50" || bNew != "150" {
		t.Fatalf("afterTS saw alice=%q bob=%q, want 50/150", aNew, bNew)
	}
}

func TestPercolatorWriteConflictRollsBackCleanly(t *testing.T) {
	coord := NewCoordinator(4)
	_, _ = coord.Execute([]WriteOp{{Key: "k1", Value: "init"}, {Key: "k2", Value: "init"}})

	// Hold an uncommitted lock on k2 to force a conflict on the second key of a 2-key txn.
	blockerTS := coord.StartReadTS()
	if err := coord.shardFor("k2").Prewrite("k2", "locked", "k2", blockerTS); err != nil {
		t.Fatalf("blocker prewrite failed: %v", err)
	}

	// Txn trying to update (k1, k2) MUST fail AND roll back k1's lock cleanly!
	_, err := coord.Execute([]WriteOp{{Key: "k1", Value: "should-rollback"}, {Key: "k2", Value: "boom"}})
	if !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("expected ErrWriteConflict, got %v", err)
	}

	// Verify k1 is unlocked and still holds "init".
	readTS := coord.StartReadTS()
	v1, ok, err := coord.ReadAt("k1", readTS)
	if err != nil || !ok || v1 != "init" {
		t.Fatalf("k1 was not rolled back cleanly: val=%q ok=%v err=%v", v1, ok, err)
	}
}
