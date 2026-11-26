package shard

import (
	"errors"
	"sync"
)

var (
	ErrWrongGroup = errors.New("shard: key does not belong to this replica group")
	ErrNotReady   = errors.New("shard: migration in progress, retry")
)

// ShardStatus tracks the lifecycle of a shard inside a ShardGroup during online migration.
type ShardStatus uint8

const (
	StatusInactive ShardStatus = iota // Not owned by this group
	StatusServing                     // Owned and serving reads/writes
	StatusPushing                     // Freeze writes; waiting for new owner to pull data
	StatusPulling                     // Waiting to pull data from old owner
)

// ShardData holds both the KV entries and the client deduplication table for a single shard,
// ensuring at-most-once execution is preserved across live shard migrations.
type ShardData struct {
	KV      map[string]string
	LastSeq map[uint64]uint64 // ClientID -> last executed SeqNum
}

func newShardData() *ShardData {
	return &ShardData{
		KV:      make(map[string]string),
		LastSeq: make(map[uint64]uint64),
	}
}

func (sd *ShardData) Clone() ShardData {
	kvCopy := make(map[string]string, len(sd.KV))
	for k, v := range sd.KV {
		kvCopy[k] = v
	}
	seqCopy := make(map[uint64]uint64, len(sd.LastSeq))
	for cid, seq := range sd.LastSeq {
		seqCopy[cid] = seq
	}
	return ShardData{KV: kvCopy, LastSeq: seqCopy}
}

// ShardGroup represents one Multi-Raft replica group's sharded KV state machine.
type ShardGroup struct {
	mu      sync.RWMutex
	gid     int
	config  Config
	status  [NumShards]ShardStatus
	shards  [NumShards]*ShardData
}

// NewShardGroup creates a Multi-Raft replica group with ID gid.
func NewShardGroup(gid int) *ShardGroup {
	sg := &ShardGroup{gid: gid}
	for i := 0; i < NumShards; i++ {
		sg.shards[i] = newShardData()
	}
	return sg
}

// ApplyConfig transitions this group sequentially from Config(N) -> Config(N+1).
func (sg *ShardGroup) ApplyConfig(next Config) error {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if next.Num != sg.config.Num+1 {
		return errors.New("shard: configs must be applied sequentially one version at a time")
	}

	prev := sg.config
	for s := 0; s < NumShards; s++ {
		wasMine := prev.Shards[s] == sg.gid
		isMine := next.Shards[s] == sg.gid

		switch {
		case !wasMine && isMine:
			if prev.Shards[s] == 0 {
				// Brand new shard assignment (initial cluster bootstrap): serve immediately.
				sg.status[s] = StatusServing
			} else {
				// Owned previously by another group: must pull data before serving.
				sg.status[s] = StatusPulling
			}
		case wasMine && !isMine:
			// Freeze shard immediately so no new writes land while new owner pulls it.
			sg.status[s] = StatusPushing
		}
	}

	sg.config = next.Clone()
	return nil
}

// Put executes a linearizable write if this group currently owns and is serving the key's shard.
func (sg *ShardGroup) Put(key, value string, clientID, seqNum uint64) error {
	s := Key2Shard(key)
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if sg.config.Shards[s] != sg.gid {
		return ErrWrongGroup
	}
	if sg.status[s] != StatusServing {
		return ErrNotReady
	}

	sd := sg.shards[s]
	if last, ok := sd.LastSeq[clientID]; ok && seqNum <= last {
		return nil // Idempotent duplicate retry
	}
	sd.KV[key] = value
	sd.LastSeq[clientID] = seqNum
	return nil
}

// Get reads a key if this group currently owns and is serving the key's shard.
func (sg *ShardGroup) Get(key string) (string, bool, error) {
	s := Key2Shard(key)
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	if sg.config.Shards[s] != sg.gid {
		return "", false, ErrWrongGroup
	}
	if sg.status[s] != StatusServing {
		return "", false, ErrNotReady
	}
	val, ok := sg.shards[s].KV[key]
	return val, ok, nil
}

// ExportShard is called on the old owner (StatusPushing) to hand off KV + dedup state.
func (sg *ShardGroup) ExportShard(shardID int) (ShardData, error) {
	sg.mu.RLock()
	defer sg.mu.RUnlock()

	if sg.status[shardID] != StatusPushing {
		return ShardData{}, ErrNotReady
	}
	return sg.shards[shardID].Clone(), nil
}

// InstallShard is called on the new owner (StatusPulling) to atomically activate the shard.
func (sg *ShardGroup) InstallShard(shardID int, data ShardData) error {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if sg.status[shardID] != StatusPulling {
		return nil // Already installed
	}
	cloned := data.Clone()
	sg.shards[shardID] = &cloned
	sg.status[shardID] = StatusServing
	return nil
}

// DeleteShard garbage-collects an exported shard on the old owner after migration completes.
func (sg *ShardGroup) DeleteShard(shardID int) {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	if sg.status[shardID] == StatusPushing {
		sg.shards[shardID] = newShardData()
		sg.status[shardID] = StatusInactive
	}
}
