package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const defaultMemTableMaxBytes = 4 * 1024 * 1024 // 4 MB default flush threshold

// Options configures LSM-tree storage engine thresholds.
type Options struct {
	MemTableMaxBytes int
}

// KV represents a live key-value pair returned to callers.
type KV struct {
	Key   []byte
	Value []byte
}

// Engine is a single-node LSM-Tree Key-Value storage engine backed by:
// active WAL + sorted SkipList MemTable + immutable SSTables with Bloom filters + compaction.
type Engine struct {
	mu          sync.RWMutex
	dir         string
	opts        Options
	wal         *WAL
	mem         *MemTable
	sstables    []*SSTable // Ordered oldest -> newest (reads search newest -> oldest)
	nextTableID uint64
}

// OpenEngine opens a storage engine in dir with default options.
func OpenEngine(dir string) (*Engine, error) {
	return OpenEngineWithOptions(dir, Options{MemTableMaxBytes: defaultMemTableMaxBytes})
}

// OpenEngineWithOptions opens (or creates) an LSM-tree storage engine in dir,
// loading existing SSTables and replaying active.wal into the MemTable.
func OpenEngineWithOptions(dir string, opts Options) (*Engine, error) {
	if opts.MemTableMaxBytes <= 0 {
		opts.MemTableMaxBytes = defaultMemTableMaxBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: mkdir %s: %w", dir, err)
	}

	// 1. Discover and open existing immutable SSTables (*.sst) ordered by ID.
	sstables, maxID, err := loadExistingSSTables(dir)
	if err != nil {
		return nil, err
	}

	// 2. Open and replay active WAL into MemTable.
	walPath := filepath.Join(dir, "active.wal")
	wal, err := Open(walPath)
	if err != nil {
		for _, s := range sstables {
			_ = s.Close()
		}
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}

	mem := NewMemTable()
	for _, rec := range wal.Replay() {
		switch rec.Op {
		case OpPut:
			mem.Put(rec.Key, rec.Value)
		case OpDelete:
			mem.Delete(rec.Key)
		}
	}

	return &Engine{
		dir:         dir,
		opts:        opts,
		wal:         wal,
		mem:         mem,
		sstables:    sstables,
		nextTableID: maxID + 1,
	}, nil
}

func loadExistingSSTables(dir string) ([]*SSTable, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("engine: readdir: %w", err)
	}

	type sstFile struct {
		id   uint64
		path string
	}
	var found []sstFile
	for _, de := range entries {
		name := de.Name()
		if !strings.HasSuffix(name, ".sst") {
			continue
		}
		stem := strings.TrimSuffix(name, ".sst")
		id, err := strconv.ParseUint(stem, 10, 64)
		if err != nil {
			continue
		}
		found = append(found, sstFile{id: id, path: filepath.Join(dir, name)})
	}

	sort.Slice(found, func(i, j int) bool { return found[i].id < found[j].id })

	tables := make([]*SSTable, 0, len(found))
	var maxID uint64
	for _, f := range found {
		sst, err := OpenSSTable(f.path, f.id)
		if err != nil {
			for _, opened := range tables {
				_ = opened.Close()
			}
			return nil, 0, err
		}
		tables = append(tables, sst)
		if f.id > maxID {
			maxID = f.id
		}
	}
	return tables, maxID, nil
}

// Put durably persists key -> value to the WAL before updating the MemTable.
// Automatically flushes MemTable to an immutable SSTable if MemTableMaxBytes is reached.
func (e *Engine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.AppendAndSync(Record{Op: OpPut, Key: key, Value: value}); err != nil {
		return err
	}
	e.mem.Put(key, value)

	if e.mem.SizeBytes() >= e.opts.MemTableMaxBytes {
		return e.flushMemTableLocked()
	}
	return nil
}

// Delete durably appends a tombstone to the WAL before updating the MemTable.
func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.AppendAndSync(Record{Op: OpDelete, Key: key}); err != nil {
		return err
	}
	e.mem.Delete(key)

	if e.mem.SizeBytes() >= e.opts.MemTableMaxBytes {
		return e.flushMemTableLocked()
	}
	return nil
}

// flushMemTableLocked freezes the active MemTable into an immutable SSTable and rotates active.wal.
func (e *Engine) flushMemTableLocked() error {
	if e.mem.Len() == 0 {
		return nil
	}

	id := e.nextTableID
	e.nextTableID++
	sstPath := filepath.Join(e.dir, fmt.Sprintf("%06d.sst", id))

	sst, err := WriteSSTable(sstPath, id, e.mem.Entries())
	if err != nil {
		return err
	}
	e.sstables = append(e.sstables, sst)

	// Rotate WAL cleanly (close before remove for Windows compatibility).
	if err := e.wal.Close(); err != nil {
		return err
	}
	walPath := filepath.Join(e.dir, "active.wal")
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	newWAL, err := Open(walPath)
	if err != nil {
		return err
	}
	e.wal = newWAL
	e.mem = NewMemTable()
	return nil
}

// Get retrieves the newest value for key across MemTable and SSTables (newest to oldest).
func (e *Engine) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. Check active MemTable first.
	if val, found, deleted := e.mem.Get(key); found {
		if deleted {
			return nil, false
		}
		return val, true
	}

	// 2. Search immutable SSTables from newest to oldest.
	for i := len(e.sstables) - 1; i >= 0; i-- {
		val, found, deleted, err := e.sstables[i].Get(key)
		if err != nil || !found {
			continue
		}
		if deleted {
			return nil, false
		}
		return val, true
	}

	return nil, false
}

// Scan returns all live key-value pairs with start <= key < end in sorted key order,
// merging the MemTable and all SSTables so newer writes and tombstones shadow older ones.
func (e *Engine) Scan(start, end []byte) []KV {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Build a merged map from oldest -> newest so newer entries overwrite older ones.
	merged := make(map[string]Entry)
	for _, sst := range e.sstables {
		entries, err := sst.Entries()
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if start != nil && bytes.Compare(entry.Key, start) < 0 {
				continue
			}
			if end != nil && bytes.Compare(entry.Key, end) >= 0 {
				continue
			}
			merged[string(entry.Key)] = entry
		}
	}
	for _, entry := range e.mem.Scan(start, end) {
		merged[string(entry.Key)] = entry
	}

	results := make([]KV, 0, len(merged))
	for _, entry := range merged {
		if entry.Deleted {
			continue
		}
		results = append(results, KV{Key: entry.Key, Value: entry.Value})
	}
	sort.Slice(results, func(i, j int) bool {
		return bytes.Compare(results[i].Key, results[j].Key) < 0
	})
	return results
}

// Compact merges all immutable SSTables into a single clean SSTable,
// deduplicating overwritten keys and dropping delete tombstones to reclaim disk space.
func (e *Engine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.sstables) == 0 {
		return nil
	}

	// Merge oldest -> newest so latest value/tombstone wins.
	merged := make(map[string]Entry)
	for _, sst := range e.sstables {
		entries, err := sst.Entries()
		if err != nil {
			return fmt.Errorf("engine: compact read sst %d: %w", sst.id, err)
		}
		for _, entry := range entries {
			merged[string(entry.Key)] = entry
		}
	}

	// Drop tombstones (safe because we are compacting all SSTables into the bottommost level).
	live := make([]Entry, 0, len(merged))
	for _, entry := range merged {
		if entry.Deleted {
			continue
		}
		live = append(live, entry)
	}
	sort.Slice(live, func(i, j int) bool {
		return bytes.Compare(live[i].Key, live[j].Key) < 0
	})

	oldTables := e.sstables
	var newTables []*SSTable

	if len(live) > 0 {
		id := e.nextTableID
		e.nextTableID++
		sstPath := filepath.Join(e.dir, fmt.Sprintf("%06d.sst", id))
		compacted, err := WriteSSTable(sstPath, id, live)
		if err != nil {
			return err
		}
		newTables = append(newTables, compacted)
	}

	e.sstables = newTables

	// Close and remove old SSTables (close first for Windows compatibility).
	for _, old := range oldTables {
		_ = old.Close()
		_ = os.Remove(old.path)
	}
	return nil
}

// SSTableCount returns the number of on-disk SSTables currently managed by the engine.
func (e *Engine) SSTableCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.sstables)
}

// TotalSSTableBytes returns the combined on-disk byte size of all active SSTables.
func (e *Engine) TotalSSTableBytes() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var total int64
	for _, sst := range e.sstables {
		total += sst.SizeBytes()
	}
	return total
}

// Close flushes and closes the WAL and all open SSTable handles.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var firstErr error
	if err := e.wal.Close(); err != nil {
		firstErr = err
	}
	for _, sst := range e.sstables {
		if err := sst.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
