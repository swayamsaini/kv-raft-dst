package storage

import (
	"bytes"
	"math/rand"
)

const (
	maxSkipListLevel = 12
	skipListP        = 0.5
)

// Entry represents a key-value pair or a delete tombstone in sorted memory.
type Entry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

type skipNode struct {
	entry Entry
	next  []*skipNode
}

// MemTable is a sorted in-memory SkipList backing the active write buffer of an LSM-tree.
// Supports O(log N) Put/Get/Delete and O(log N + K) lexicographical range scans.
type MemTable struct {
	head      *skipNode
	level     int
	sizeBytes int
	count     int
	rng       *rand.Rand
}

// NewMemTable creates an empty MemTable with a deterministic PRNG for SkipList heights.
func NewMemTable() *MemTable {
	return &MemTable{
		head:  &skipNode{next: make([]*skipNode, maxSkipListLevel)},
		level: 1,
		rng:   rand.New(rand.NewSource(42)),
	}
}

func (m *MemTable) randomLevel() int {
	lvl := 1
	for lvl < maxSkipListLevel && m.rng.Float64() < skipListP {
		lvl++
	}
	return lvl
}

// Put inserts or updates a live key-value pair in the MemTable.
func (m *MemTable) Put(key, value []byte) {
	m.upsert(Entry{
		Key:     cloneBytes(key),
		Value:   cloneBytes(value),
		Deleted: false,
	})
}

// Delete writes a tombstone marker for key so older SSTable levels are shadowed.
func (m *MemTable) Delete(key []byte) {
	m.upsert(Entry{
		Key:     cloneBytes(key),
		Value:   nil,
		Deleted: true,
	})
}

func (m *MemTable) upsert(entry Entry) {
	update := make([]*skipNode, maxSkipListLevel)
	curr := m.head

	for i := m.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].entry.Key, entry.Key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	candidate := curr.next[0]
	if candidate != nil && bytes.Equal(candidate.entry.Key, entry.Key) {
		// Key already exists: update value/tombstone in place and adjust byte size.
		m.sizeBytes -= len(candidate.entry.Value)
		candidate.entry.Value = entry.Value
		candidate.entry.Deleted = entry.Deleted
		m.sizeBytes += len(entry.Value)
		return
	}

	newLevel := m.randomLevel()
	if newLevel > m.level {
		for i := m.level; i < newLevel; i++ {
			update[i] = m.head
		}
		m.level = newLevel
	}

	node := &skipNode{
		entry: entry,
		next:  make([]*skipNode, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	m.count++
	m.sizeBytes += len(entry.Key) + len(entry.Value)
}

// Get looks up key in the MemTable.
// Returns (value, found, deleted). If found && deleted, the key was explicitly deleted (tombstone).
func (m *MemTable) Get(key []byte) ([]byte, bool, bool) {
	curr := m.head
	for i := m.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].entry.Key, key) < 0 {
			curr = curr.next[i]
		}
	}

	candidate := curr.next[0]
	if candidate != nil && bytes.Equal(candidate.entry.Key, key) {
		if candidate.entry.Deleted {
			return nil, true, true
		}
		return cloneBytes(candidate.entry.Value), true, false
	}
	return nil, false, false
}

// Scan returns all entries (including tombstones) with start <= key < end in sorted order.
// Passing nil for start starts from the first key; passing nil for end scans through the last key.
func (m *MemTable) Scan(start, end []byte) []Entry {
	curr := m.head
	if start != nil {
		for i := m.level - 1; i >= 0; i-- {
			for curr.next[i] != nil && bytes.Compare(curr.next[i].entry.Key, start) < 0 {
				curr = curr.next[i]
			}
		}
	}

	var results []Entry
	for node := curr.next[0]; node != nil; node = node.next[0] {
		if end != nil && bytes.Compare(node.entry.Key, end) >= 0 {
			break
		}
		results = append(results, Entry{
			Key:     cloneBytes(node.entry.Key),
			Value:   cloneBytes(node.entry.Value),
			Deleted: node.entry.Deleted,
		})
	}
	return results
}

// Entries returns all entries in sorted key order (used when flushing to an SSTable).
func (m *MemTable) Entries() []Entry {
	return m.Scan(nil, nil)
}

// SizeBytes returns the approximate byte size of keys and values stored in this MemTable.
func (m *MemTable) SizeBytes() int {
	return m.sizeBytes
}

// Len returns the number of distinct keys (including tombstones) in the MemTable.
func (m *MemTable) Len() int {
	return m.count
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
