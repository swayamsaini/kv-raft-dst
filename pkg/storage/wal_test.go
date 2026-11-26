package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWALBasicReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00001.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	want := []Record{
		{Op: OpPut, Key: []byte("user:1"), Value: []byte("alice")},
		{Op: OpPut, Key: []byte("user:2"), Value: []byte("bob")},
		{Op: OpDelete, Key: []byte("user:1"), Value: nil},
	}

	for _, rec := range want {
		if err := w.AppendAndSync(rec); err != nil {
			t.Fatalf("AppendAndSync failed: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify every record survived crash/restart.
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w2.Close()

	got := w2.Replay()
	if len(got) != len(want) {
		t.Fatalf("Replay count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Errorf("record[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestWALTornWriteRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := w.AppendAndSync(Record{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("AppendAndSync failed: %v", err)
	}
	validSize := w.Size()
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate a power cut halfway through writing record #2 (truncated bytes at EOF).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	partialFrame := encodeRecord(Record{Op: OpPut, Key: []byte("k2"), Value: []byte("partial-value-cut-off")})
	if _, err := f.Write(partialFrame[:len(partialFrame)/2]); err != nil {
		t.Fatalf("write partial frame failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close raw file failed: %v", err)
	}

	// Reopening must recover record #1 and truncate the torn tail cleanly.
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open after torn write failed: %v", err)
	}

	got := w2.Replay()
	if len(got) != 1 || string(got[0].Key) != "k1" || string(got[0].Value) != "v1" {
		t.Fatalf("unexpected replayed records: %+v", got)
	}
	if w2.Size() != validSize {
		t.Fatalf("Size after recovery = %d, want %d", w2.Size(), validSize)
	}

	// Subsequent appends must work cleanly after truncation.
	if err := w2.AppendAndSync(Record{Op: OpPut, Key: []byte("k3"), Value: []byte("v3")}); err != nil {
		t.Fatalf("AppendAndSync after recovery failed: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close w2 failed: %v", err)
	}

	w3, err := Open(path)
	if err != nil {
		t.Fatalf("Open w3 failed: %v", err)
	}
	defer w3.Close()
	if len(w3.Replay()) != 2 {
		t.Fatalf("Replay count after post-recovery append = %d, want 2", len(w3.Replay()))
	}
}

func TestWALMiddleCorruptionDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	_ = w.AppendAndSync(Record{Op: OpPut, Key: []byte("first"), Value: []byte("committed-data")})
	_ = w.AppendAndSync(Record{Op: OpPut, Key: []byte("second"), Value: []byte("more-data")})
	_ = w.Close()

	// Flip 1 bit inside the first record's value (simulating silent disk bit-rot).
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	raw[15] ^= 0x01
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open MUST detect mid-file corruption rather than silently truncating committed data.
	_, err = Open(path)
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("expected ErrCorruptRecord, got: %v", err)
	}
}

func BenchmarkWALAppendSync(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_sync.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	rec := Record{Op: OpPut, Key: []byte("bench-key-0001"), Value: bytes.Repeat([]byte("a"), 1024)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.AppendAndSync(rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALAppendNoSync(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_nosync.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	rec := Record{Op: OpPut, Key: []byte("bench-key-0001"), Value: bytes.Repeat([]byte("a"), 1024)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(rec); err != nil {
			b.Fatal(err)
		}
	}
}