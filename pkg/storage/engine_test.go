package storage

import (
	"testing"
)

func TestEnginePutGetDeleteScan(t *testing.T) {
	eng, err := OpenEngine(t.TempDir())
	if err != nil {
		t.Fatalf("OpenEngine failed: %v", err)
	}
	defer eng.Close()

	// Put keys out of order to verify sorted Scan().
	_ = eng.Put([]byte("c"), []byte("3"))
	_ = eng.Put([]byte("a"), []byte("1"))
	_ = eng.Put([]byte("d"), []byte("4"))
	_ = eng.Put([]byte("b"), []byte("2"))
	_ = eng.Put([]byte("b"), []byte("2-updated"))
	_ = eng.Delete([]byte("c"))

	// Verify Get on live, updated, deleted, and missing keys.
	if val, ok := eng.Get([]byte("b")); !ok || string(val) != "2-updated" {
		t.Fatalf("Get(b) = (%q, %v), want (2-updated, true)", val, ok)
	}
	if _, ok := eng.Get([]byte("c")); ok {
		t.Fatalf("Get(c) should return false after Delete")
	}
	if _, ok := eng.Get([]byte("missing")); ok {
		t.Fatalf("Get(missing) should return false")
	}

	// Verify range Scan ["a", "d") skips deleted "c" and excludes "d".
	got := eng.Scan([]byte("a"), []byte("d"))
	if len(got) != 2 || string(got[0].Key) != "a" || string(got[1].Key) != "b" || string(got[1].Value) != "2-updated" {
		t.Fatalf("Scan(a, d) = %+v, want [a:1, b:2-updated]", got)
	}
}

func TestEngineCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Session 1: write, overwrite, and delete, then close (simulating restart).
	eng1, err := OpenEngine(dir)
	if err != nil {
		t.Fatalf("OpenEngine #1 failed: %v", err)
	}
	_ = eng1.Put([]byte("user:1"), []byte("alice"))
	_ = eng1.Put([]byte("user:2"), []byte("bob"))
	_ = eng1.Put([]byte("user:1"), []byte("alice-v2"))
	_ = eng1.Delete([]byte("user:2"))
	if err := eng1.Close(); err != nil {
		t.Fatalf("Close #1 failed: %v", err)
	}

	// Session 2: reopen from disk and verify exact state was rebuilt from WAL.
	eng2, err := OpenEngine(dir)
	if err != nil {
		t.Fatalf("OpenEngine #2 failed: %v", err)
	}
	defer eng2.Close()

	if val, ok := eng2.Get([]byte("user:1")); !ok || string(val) != "alice-v2" {
		t.Fatalf("after recovery Get(user:1) = (%q, %v), want (alice-v2, true)", val, ok)
	}
	if _, ok := eng2.Get([]byte("user:2")); ok {
		t.Fatalf("after recovery Get(user:2) should be deleted")
	}
}
