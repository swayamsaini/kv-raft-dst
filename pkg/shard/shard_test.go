package shard

import (
	"errors"
	"testing"
)

func TestShardControllerBalancedJoinLeaveMove(t *testing.T) {
	ctrl := NewController()

	// 1. Join Group 1 -> owns all 10 shards.
	cfg1 := ctrl.Join(map[int][]int{1: {1, 2, 3}})
	for s := 0; s < NumShards; s++ {
		if cfg1.Shards[s] != 1 {
			t.Fatalf("expected shard %d -> group 1, got %d", s, cfg1.Shards[s])
		}
	}

	// 2. Join Group 2 -> 5 shards each, minimal movement (only 5 moved).
	cfg2 := ctrl.Join(map[int][]int{2: {4, 5, 6}})
	counts := map[int]int{}
	moved := 0
	for s := 0; s < NumShards; s++ {
		counts[cfg2.Shards[s]]++
		if cfg2.Shards[s] != cfg1.Shards[s] {
			moved++
		}
	}
	if counts[1] != 5 || counts[2] != 5 {
		t.Fatalf("unbalanced join: %+v", counts)
	}
	if moved != 5 {
		t.Fatalf("expected minimal 5 shard moves, got %d", moved)
	}

	// 3. Explicit Move shard 0 -> Group 2.
	cfg3 := ctrl.Move(0, 2)
	if cfg3.Shards[0] != 2 {
		t.Fatalf("Move(0, 2) failed: got %d", cfg3.Shards[0])
	}

	// 4. Leave Group 1 -> Group 2 owns all 10 shards.
	cfg4 := ctrl.Leave([]int{1})
	for s := 0; s < NumShards; s++ {
		if cfg4.Shards[s] != 2 {
			t.Fatalf("after Leave(1), shard %d -> %d, want 2", s, cfg4.Shards[s])
		}
	}
}

func TestOnlineShardMigrationPreservesDataAndDedup(t *testing.T) {
	ctrl := NewController()
	g1 := NewShardGroup(1)
	g2 := NewShardGroup(2)

	// Config 1: Group 1 owns all shards.
	cfg1 := ctrl.Join(map[int][]int{1: {1, 2, 3}})
	_ = g1.ApplyConfig(cfg1)
	_ = g2.ApplyConfig(cfg1)

	// Write a key and find which shard it landed on.
	key := "account:swayamsaini"
	targetShard := Key2Shard(key)
	if err := g1.Put(key, "500", 99, 1); err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	// Config 2: Join Group 2, and explicitly Move targetShard -> Group 2 if needed.
	cfg2 := ctrl.Join(map[int][]int{2: {4, 5, 6}})
	_ = g1.ApplyConfig(cfg2)
	_ = g2.ApplyConfig(cfg2)

	if cfg2.Shards[targetShard] != 2 {
		cfg2 = ctrl.Move(targetShard, 2)
		_ = g1.ApplyConfig(cfg2)
		_ = g2.ApplyConfig(cfg2)
	}

	// Group 1 must now reject writes to migrated shard with ErrWrongGroup.
	if err := g1.Put(key, "999", 99, 2); !errors.Is(err, ErrWrongGroup) {
		t.Fatalf("expected ErrWrongGroup on old owner, got %v", err)
	}

	// Group 2 must reject reads with ErrNotReady until InstallShard completes.
	if _, _, err := g2.Get(key); !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady before InstallShard, got %v", err)
	}

	// Execute Pull -> Install -> GC migration.
	data, err := g1.ExportShard(targetShard)
	if err != nil {
		t.Fatalf("ExportShard failed: %v", err)
	}
	if err := g2.InstallShard(targetShard, data); err != nil {
		t.Fatalf("InstallShard failed: %v", err)
	}
	g1.DeleteShard(targetShard)

	// Verify data migrated cleanly to Group 2.
	if val, ok, err := g2.Get(key); err != nil || !ok || val != "500" {
		t.Fatalf("Get after migration = (%q, %v, %v), want (500, true, nil)", val, ok, err)
	}

	// Verify client dedup table migrated too: replaying old SeqNum=1 must NOT overwrite newer writes!
	_ = g2.Put(key, "750", 99, 2)
	_ = g2.Put(key, "500", 99, 1) // Stale retry of SeqNum=1
	if val, _, _ := g2.Get(key); val != "750" {
		t.Fatalf("dedup table lost during shard migration: got %q, want 750", val)
	}
}
