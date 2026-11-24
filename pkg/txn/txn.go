package txn

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrWriteConflict = errors.New("txn: write-write conflict or key locked, abort and retry")
)

// WriteOp is a single key mutation inside an atomic multi-key transaction.
type WriteOp struct {
	Key   string
	Value string
}

// TimestampOracle hands out strictly monotonic logical timestamps (start_ts / commit_ts).
type TimestampOracle struct {
	counter atomic.Uint64
}

func (tso *TimestampOracle) Next() uint64 {
	return tso.counter.Add(1)
}

type lockRecord struct {
	primaryKey string
	startTS    uint64
}

type writeRecord struct {
	commitTS uint64
	startTS  uint64
}

// MVCCShard represents one shard's multi-version storage supporting Percolator 2PC.
type MVCCShard struct {
	mu     sync.Mutex
	data   map[string]map[uint64]string // key -> startTS -> value
	locks  map[string]*lockRecord       // key -> active prewrite lock
	writes map[string][]writeRecord     // key -> committed versions (ascending commitTS)
}

func NewMVCCShard() *MVCCShard {
	return &MVCCShard{
		data:   make(map[string]map[uint64]string),
		locks:  make(map[string]*lockRecord),
		writes: make(map[string][]writeRecord),
	}
}

// Prewrite acquires a lock on key pointing to primaryKey and stages value at startTS.
// Fails with ErrWriteConflict if another transaction holds a lock or committed >= startTS.
func (s *MVCCShard) Prewrite(key, value, primaryKey string, startTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lock, exists := s.locks[key]; exists && lock.startTS != startTS {
		return ErrWriteConflict
	}
	for _, w := range s.writes[key] {
		if w.commitTS >= startTS {
			return ErrWriteConflict
		}
	}

	if s.data[key] == nil {
		s.data[key] = make(map[uint64]string)
	}
	s.data[key][startTS] = value
	s.locks[key] = &lockRecord{primaryKey: primaryKey, startTS: startTS}
	return nil
}

// Commit atomically converts key's lock into a visible commit record at commitTS.
func (s *MVCCShard) Commit(key string, startTS, commitTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lock, exists := s.locks[key]
	if !exists || lock.startTS != startTS {
		return ErrWriteConflict
	}
	delete(s.locks, key)
	s.writes[key] = append(s.writes[key], writeRecord{
		commitTS: commitTS,
		startTS:  startTS,
	})
	return nil
}

// Rollback removes a staged prewrite lock and uncommitted value on abort.
func (s *MVCCShard) Rollback(key string, startTS uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lock, exists := s.locks[key]; exists && lock.startTS == startTS {
		delete(s.locks, key)
	}
	if s.data[key] != nil {
		delete(s.data[key], startTS)
	}
}

// GetAt reads the latest committed version of key with commitTS <= readTS (Snapshot Isolation).
func (s *MVCCShard) GetAt(key string, readTS uint64) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lock, exists := s.locks[key]; exists && lock.startTS <= readTS {
		return "", false, ErrWriteConflict
	}

	var best *writeRecord
	for i := range s.writes[key] {
		w := &s.writes[key][i]
		if w.commitTS <= readTS {
			best = w
		}
	}
	if best == nil {
		return "", false, nil
	}
	return s.data[key][best.startTS], true, nil
}

// Coordinator runs Percolator-style 2PC across multiple MVCCShards.
type Coordinator struct {
	tso    *TimestampOracle
	shards []*MVCCShard
}

func NewCoordinator(numShards int) *Coordinator {
	shards := make([]*MVCCShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = NewMVCCShard()
	}
	return &Coordinator{
		tso:    &TimestampOracle{},
		shards: shards,
	}
}

func (c *Coordinator) shardFor(key string) *MVCCShard {
	h := 0
	for _, b := range []byte(key) {
		h = h*31 + int(b)
	}
	if h < 0 {
		h = -h
	}
	return c.shards[h%len(c.shards)]
}

// StartReadTS allocates a snapshot timestamp for consistent multi-key reads.
func (c *Coordinator) StartReadTS() uint64 {
	return c.tso.Next()
}

// ReadAt reads key at snapshot timestamp readTS.
func (c *Coordinator) ReadAt(key string, readTS uint64) (string, bool, error) {
	return c.shardFor(key).GetAt(key, readTS)
}

// Execute runs an atomic multi-key transaction across shards using Percolator 2PC:
// Phase 1 (Prewrite all keys) -> Phase 2 (Commit Primary atomically -> Commit Secondaries).
func (c *Coordinator) Execute(ops []WriteOp) (uint64, error) {
	if len(ops) == 0 {
		return 0, nil
	}
	startTS := c.tso.Next()
	primaryKey := ops[0].Key

	// Phase 1: Prewrite primary and all secondary keys.
	prewritten := make([]WriteOp, 0, len(ops))
	for _, op := range ops {
		if err := c.shardFor(op.Key).Prewrite(op.Key, op.Value, primaryKey, startTS); err != nil {
			// Roll back every staged prewrite so zero partial writes leak!
			for _, done := range prewritten {
				c.shardFor(done.Key).Rollback(done.Key, startTS)
			}
			return 0, err
		}
		prewritten = append(prewritten, op)
	}

	// Phase 2: Obtain commitTS and commit Primary key FIRST (the atomic commit point!).
	commitTS := c.tso.Next()
	if err := c.shardFor(primaryKey).Commit(primaryKey, startTS, commitTS); err != nil {
		for _, op := range prewritten {
			c.shardFor(op.Key).Rollback(op.Key, startTS)
		}
		return 0, err
	}

	// Primary committed -> transaction is permanently committed; commit secondaries.
	for _, op := range ops[1:] {
		_ = c.shardFor(op.Key).Commit(op.Key, startTS, commitTS)
	}
	return commitTS, nil
}
