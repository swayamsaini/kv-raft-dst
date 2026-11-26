package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/swayamsaini/tardidb/pkg/raft"
)

// OpKind identifies a client KV operation.
type OpKind uint8

const (
	OpPut OpKind = iota + 1
	OpDelete
)

// Command is the replicated state-machine payload committed through Raft.
// ClientID + SeqNum ensure linearizable at-most-once execution across retries.
type Command struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID uint64
	SeqNum   uint64
}

// Reply is the deterministic result of applying a Command.
type Reply struct {
	Value string
	Found bool
}

type dedupEntry struct {
	SeqNum uint64
	Reply  Reply
}

// StateMachine is the deterministic KV state machine driven by Raft's committed log.
// It stores keys, values, and the per-client deduplication table (all included in snapshots).
type StateMachine struct {
	mu      sync.RWMutex
	data    map[string]string
	lastSeq map[uint64]dedupEntry
}

// NewStateMachine creates an empty KV state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		data:    make(map[string]string),
		lastSeq: make(map[uint64]dedupEntry),
	}
}

// EncodeCommand serializes a Command for Raft.Propose().
func EncodeCommand(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, fmt.Errorf("kv: encode command: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodeCommand deserializes a Raft log payload back into a Command.
func DecodeCommand(raw []byte) (Command, error) {
	var cmd Command
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&cmd); err != nil {
		return Command{}, fmt.Errorf("kv: decode command: %w", err)
	}
	return cmd, nil
}

// Apply executes a committed Raft entry against the KV map with idempotency deduplication.
func (sm *StateMachine) Apply(entry *raft.LogEntry) (Reply, error) {
	if entry == nil || entry.Command == nil {
		return Reply{}, nil
	}
	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		return Reply{}, err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if prev, ok := sm.lastSeq[cmd.ClientID]; ok && cmd.SeqNum <= prev.SeqNum {
		// Duplicate retried command: return cached reply without re-executing!
		return prev.Reply, nil
	}

	var rep Reply
	switch cmd.Kind {
	case OpPut:
		sm.data[cmd.Key] = cmd.Value
		rep = Reply{Value: cmd.Value, Found: true}
	case OpDelete:
		delete(sm.data, cmd.Key)
		rep = Reply{Found: false}
	default:
		return Reply{}, fmt.Errorf("kv: unknown op %d", cmd.Kind)
	}

	sm.lastSeq[cmd.ClientID] = dedupEntry{
		SeqNum: cmd.SeqNum,
		Reply:  rep,
	}
	return rep, nil
}

// Get reads a key locally (must only be called after Raft ReadIndex confirmation for linearizability).
func (sm *StateMachine) Get(key string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	val, ok := sm.data[key]
	return val, ok
}

type snapshotPayload struct {
	Data    map[string]string
	LastSeq map[uint64]dedupEntry
}

// Snapshot serializes both the KV data and the client deduplication table.
func (sm *StateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotPayload{
		Data:    sm.data,
		LastSeq: sm.lastSeq,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// RestoreSnapshot replaces the state machine contents from a Raft snapshot.
func (sm *StateMachine) RestoreSnapshot(raw []byte) error {
	var snap snapshotPayload
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&snap); err != nil {
		return err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if snap.Data == nil {
		snap.Data = make(map[string]string)
	}
	if snap.LastSeq == nil {
		snap.LastSeq = make(map[uint64]dedupEntry)
	}
	sm.data = snap.Data
	sm.lastSeq = snap.LastSeq
	return nil
}
