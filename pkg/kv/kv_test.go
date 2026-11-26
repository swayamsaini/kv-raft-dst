package kv

import (
	"testing"

	"github.com/swayamsaini/tardidb/pkg/raft"
)

func TestKVStateMachineDedupAndSnapshot(t *testing.T) {
	sm := NewStateMachine()

	raw1, _ := EncodeCommand(Command{Kind: OpPut, Key: "bal", Value: "100", ClientID: 7, SeqNum: 1})
	rep1, err := sm.Apply(&raft.LogEntry{Index: 1, Term: 1, Command: raw1})
	if err != nil || rep1.Value != "100" {
		t.Fatalf("Apply #1 failed: %v, %+v", err, rep1)
	}

	// Overwrite key with SeqNum 2.
	raw2, _ := EncodeCommand(Command{Kind: OpPut, Key: "bal", Value: "200", ClientID: 7, SeqNum: 2})
	_, _ = sm.Apply(&raft.LogEntry{Index: 2, Term: 1, Command: raw2})

	// Retry old SeqNum 1 (must be ignored and return cached SeqNum 1/2 protection without reverting "200"!).
	_, _ = sm.Apply(&raft.LogEntry{Index: 3, Term: 1, Command: raw1})
	if val, ok := sm.Get("bal"); !ok || val != "200" {
		t.Fatalf("duplicate retry overwrote newer state: got (%q, %v), want (200, true)", val, ok)
	}

	// Snapshot and restore onto a fresh StateMachine; dedup table MUST survive!
	snap, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	sm2 := NewStateMachine()
	if err := sm2.RestoreSnapshot(snap); err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}
	if val, ok := sm2.Get("bal"); !ok || val != "200" {
		t.Fatalf("restored state mismatch: got (%q, %v)", val, ok)
	}
	// Replaying SeqNum 1 on restored SM must still be deduplicated.
	_, _ = sm2.Apply(&raft.LogEntry{Index: 4, Term: 2, Command: raw1})
	if val, _ := sm2.Get("bal"); val != "200" {
		t.Fatalf("dedup table was lost across snapshot restore: val=%q", val)
	}
}
