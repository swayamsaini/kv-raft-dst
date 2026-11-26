package storage

import (
	"fmt"
	"testing"
)

func TestBloomFilterAccuracy(t *testing.T) {
	n := 1000
	bf := NewBloomFilter(n)
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("present-key-%04d", i)))
	}

	// Zero false negatives allowed!
	for i := 0; i < n; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("present-key-%04d", i))) {
			t.Fatalf("Bloom filter false negative on key %d", i)
		}
	}

	// False positive rate should be well under 3% (~0.8% theoretical).
	fp := 0
	trials := 2000
	for i := 0; i < trials; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("absent-key-%04d", i))) {
			fp++
		}
	}
	fpRate := float64(fp) / float64(trials)
	if fpRate > 0.03 {
		t.Fatalf("Bloom filter false positive rate too high: %.2f%% (%d/%d)", fpRate*100, fp, trials)
	}
}

func TestLSMFlushReopenAndRead(t *testing.T) {
	dir := t.TempDir()
	// Tiny 512-byte MemTable threshold to force multiple SSTable flushes.
	opts := Options{MemTableMaxBytes: 512}

	eng, err := OpenEngineWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("OpenEngineWithOptions failed: %v", err)
	}

	for i := 0; i < 60; i++ {
		k := []byte(fmt.Sprintf("key-%03d", i))
		v := []byte(fmt.Sprintf("val-v1-%03d-padding-bytes", i))
		if err := eng.Put(k, v); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}
	// Overwrite key-005 and delete key-010 (these land in newer SSTables / MemTable).
	_ = eng.Put([]byte("key-005"), []byte("val-v2-updated"))
	_ = eng.Delete([]byte("key-010"))

	if eng.SSTableCount() < 2 {
		t.Fatalf("expected >= 2 SSTables after flushing, got %d", eng.SSTableCount())
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen from disk and verify SSTables + WAL recovery.
	eng2, err := OpenEngineWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer eng2.Close()

	if val, ok := eng2.Get([]byte("key-005")); !ok || string(val) != "val-v2-updated" {
		t.Fatalf("Get(key-005) = (%q, %v), want (val-v2-updated, true)", val, ok)
	}
	if _, ok := eng2.Get([]byte("key-010")); ok {
		t.Fatalf("Get(key-010) should be deleted across SSTables")
	}
	if val, ok := eng2.Get([]byte("key-059")); !ok || string(val) != "val-v1-059-padding-bytes" {
		t.Fatalf("Get(key-059) = (%q, %v), want original value", val, ok)
	}
}

func TestLSMCompactionReclaimsTombstonesAndSpace(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemTableMaxBytes: 512}

	eng, err := OpenEngineWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("OpenEngineWithOptions failed: %v", err)
	}

	// Write 40 keys, overwrite all of them twice, then delete half -> lots of stale SSTable bloat.
	for round := 1; round <= 3; round++ {
		for i := 0; i < 40; i++ {
			k := []byte(fmt.Sprintf("k-%03d", i))
			v := []byte(fmt.Sprintf("round-%d-value-%03d-payload", round, i))
			_ = eng.Put(k, v)
		}
	}
	for i := 0; i < 20; i++ {
		_ = eng.Delete([]byte(fmt.Sprintf("k-%03d", i)))
	}

	beforeCount := eng.SSTableCount()
	beforeBytes := eng.TotalSSTableBytes()
	if beforeCount < 3 {
		t.Fatalf("expected >= 3 SSTables before compaction, got %d", beforeCount)
	}

	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	afterCount := eng.SSTableCount()
	afterBytes := eng.TotalSSTableBytes()
	if afterCount != 1 {
		t.Fatalf("expected 1 SSTable after compaction, got %d", afterCount)
	}
	if afterBytes >= beforeBytes {
		t.Fatalf("expected compaction to shrink disk usage: before=%d, after=%d", beforeBytes, afterBytes)
	}

	// Verify deleted keys stay gone and live keys return round-3 values.
	for i := 0; i < 20; i++ {
		if _, ok := eng.Get([]byte(fmt.Sprintf("k-%03d", i))); ok {
			t.Fatalf("deleted key k-%03d resurrected after compaction!", i)
		}
	}
	for i := 20; i < 40; i++ {
		want := fmt.Sprintf("round-3-value-%03d-payload", i)
		if val, ok := eng.Get([]byte(fmt.Sprintf("k-%03d", i))); !ok || string(val) != want {
			t.Fatalf("Get(k-%03d) = (%q, %v), want (%q, true)", i, val, ok, want)
		}
	}

	_ = eng.Close()
}
