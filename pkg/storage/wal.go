package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// OpType identifies the mutation stored in a WAL record.
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Binary record layout (Little-Endian):
// +------------+------------+------------+----------+---------+----------+---------+
// | CRC32 (4B) | TotalLen   | OpType(1B) | KeyLen   | Key     | ValLen   | Value   |
// | checksum   | (4B, u32)  | Put=1/Del=2| (4B, u32)| (N B)   | (4B, u32)| (M B)   |
// +------------+------------+------------+----------+---------+----------+---------+
//
// CRC32 covers [TotalLen .. Value].
// TotalLen = 1 (OpType) + 4 (KeyLen) + N + 4 (ValLen) + M.
const (
	crcSize       = 4
	lenSize       = 4
	headerSize    = crcSize + lenSize // 8 bytes before payload
	minPayloadLen = 1 + 4 + 4         // OpType + KeyLen + ValLen
	maxRecordSize = 64 * 1024 * 1024  // 64 MB sanity cap against corrupt length headers
)

var (
	// ErrCorruptRecord is returned when a non-tail record fails checksum or framing validation.
	ErrCorruptRecord = errors.New("wal: corrupt record detected in middle of log")
	// ErrInvalidOp is returned when an unknown OpType is encountered.
	ErrInvalidOp = errors.New("wal: invalid op type")
)

// Record represents a single durable KV mutation in the WAL.
type Record struct {
	Op    OpType
	Key   []byte
	Value []byte
}

// WAL is an append-only, crash-safe Write-Ahead Log with CRC32 framing.
type WAL struct {
	mu      sync.Mutex
	file    *os.File
	path    string
	offset  int64    // Current verified write offset in bytes
	records []Record // Records recovered during Open()
}

// Open opens (or creates) a WAL file, scans and validates every record,
// and repairs any torn write at EOF by truncating back to the last valid offset.
// If corruption is detected in the middle of the log (e.g., bit-flip on disk),
// Open returns ErrCorruptRecord rather than silently dropping committed data.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	w := &WAL{
		file: f,
		path: path,
	}

	if err := w.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}

	return w, nil
}

// recover scans the WAL from byte 0, verifying CRC32 checksums and framing.
func (w *WAL) recover() error {
	stat, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat: %w", err)
	}
	fileSize := stat.Size()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}

	var validOffset int64
	var recovered []Record
	hdr := make([]byte, headerSize)

	for validOffset < fileSize {
		// 1. Read the 8-byte header: [CRC32 (4B) | TotalLen (4B)]
		n, err := io.ReadFull(w.file, hdr)
		if err != nil {
			// Partial header at EOF => torn write from a crash mid-append.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if err := w.truncateTo(validOffset); err != nil {
					return err
				}
				break
			}
			return fmt.Errorf("wal: read header at offset %d (read %d): %w", validOffset, n, err)
		}

		expectedCRC := binary.LittleEndian.Uint32(hdr[0:4])
		payloadLen := binary.LittleEndian.Uint32(hdr[4:8])

		// Sanity-check payload length before allocating memory.
		if payloadLen < minPayloadLen || payloadLen > maxRecordSize {
			// If this corrupt header is the very tail of the file (remaining bytes <= header + bogus),
			// check whether any valid bytes could follow. For safety, treat impossible length as torn tail
			// only if we cannot even read payloadLen bytes from the file; otherwise it's mid-file corruption.
			if validOffset+int64(headerSize)+int64(payloadLen) > fileSize {
				if err := w.truncateTo(validOffset); err != nil {
					return err
				}
				break
			}
			return fmt.Errorf("%w: invalid payload length %d at offset %d", ErrCorruptRecord, payloadLen, validOffset)
		}

		// 2. Read the payload bytes.
		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(w.file, payload)
		if err != nil {
			// Incomplete payload at EOF => torn write from a crash mid-append.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if err := w.truncateTo(validOffset); err != nil {
					return err
				}
				break
			}
			return fmt.Errorf("wal: read payload at offset %d: %w", validOffset, err)
		}

		recordEnd := validOffset + int64(headerSize) + int64(payloadLen)

		// 3. Verify CRC32 over [TotalLen (4B) || payload].
		h := crc32.NewIEEE()
		_, _ = h.Write(hdr[4:8])
		_, _ = h.Write(payload)
		actualCRC := h.Sum32()

		if actualCRC != expectedCRC {
			// If this is the final record in the file, treat it as a torn write
			// (e.g., disk flushed some sectors of the last write before power loss).
			if recordEnd == fileSize {
				if err := w.truncateTo(validOffset); err != nil {
					return err
				}
				break
			}
			// There are more bytes after this record => mid-file disk corruption!
			return fmt.Errorf("%w: checksum mismatch at offset %d (got 0x%08x, want 0x%08x)",
				ErrCorruptRecord, validOffset, actualCRC, expectedCRC)
		}

		// 4. Decode payload into a Record.
		rec, err := decodePayload(payload)
		if err != nil {
			if recordEnd == fileSize {
				if err := w.truncateTo(validOffset); err != nil {
					return err
				}
				break
			}
			return fmt.Errorf("%w: decode error at offset %d: %v", ErrCorruptRecord, validOffset, err)
		}

		recovered = append(recovered, rec)
		validOffset = recordEnd
	}

	// Position file cursor at exact end of valid data for future appends.
	if _, err := w.file.Seek(validOffset, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek end: %w", err)
	}

	w.offset = validOffset
	w.records = recovered
	return nil
}

// truncateTo cleanly cuts off a torn tail record and fsyncs the file.
func (w *WAL) truncateTo(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		return fmt.Errorf("wal: truncate to %d: %w", offset, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync after truncate: %w", err)
	}
	return nil
}

// Replay returns a copy of all valid records recovered when the WAL was opened.
func (w *WAL) Replay() []Record {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([]Record, len(w.records))
	copy(out, w.records)
	return out
}

// Append encodes and writes a single record to the OS page cache.
// Call Sync() afterward to guarantee crash durability (or use AppendAndSync).
func (w *WAL) Append(rec Record) error {
	if rec.Op != OpPut && rec.Op != OpDelete {
		return fmt.Errorf("%w: %d", ErrInvalidOp, rec.Op)
	}

	buf := encodeRecord(rec)

	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	w.offset += int64(n)
	return nil
}

// Sync flushes the WAL file from the OS page cache to stable storage (fsync).
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

// AppendAndSync writes a record and immediately fsyncs it for crash durability.
func (w *WAL) AppendAndSync(rec Record) error {
	if err := w.Append(rec); err != nil {
		return err
	}
	return w.Sync()
}

// Size returns the current valid byte size of the WAL.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// Close flushes and closes the underlying WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	syncErr := w.file.Sync()
	closeErr := w.file.Close()
	w.file = nil
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func encodeRecord(rec Record) []byte {
	keyLen := len(rec.Key)
	valLen := len(rec.Value)
	payloadLen := 1 + 4 + keyLen + 4 + valLen
	buf := make([]byte, headerSize+payloadLen)

	// Header: TotalLen at [4:8]
	binary.LittleEndian.PutUint32(buf[4:8], uint32(payloadLen))

	// Payload starts at byte 8
	pos := headerSize
	buf[pos] = byte(rec.Op)
	pos++

	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(keyLen))
	pos += 4
	copy(buf[pos:pos+keyLen], rec.Key)
	pos += keyLen

	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(valLen))
	pos += 4
	copy(buf[pos:pos+valLen], rec.Value)

	// CRC32 over [TotalLen .. Value]
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return buf
}

func decodePayload(payload []byte) (Record, error) {
	if len(payload) < minPayloadLen {
		return Record{}, errors.New("payload shorter than minimum size")
	}

	op := OpType(payload[0])
	if op != OpPut && op != OpDelete {
		return Record{}, fmt.Errorf("%w: %d", ErrInvalidOp, op)
	}

	pos := 1
	keyLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if pos+keyLen+4 > len(payload) {
		return Record{}, errors.New("key length exceeds payload")
	}
	key := make([]byte, keyLen)
	copy(key, payload[pos:pos+keyLen])
	pos += keyLen

	valLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if pos+valLen != len(payload) {
		return Record{}, errors.New("value length mismatch")
	}
	val := make([]byte, valLen)
	copy(val, payload[pos:pos+valLen])

	return Record{
		Op:    op,
		Key:   key,
		Value: val,
	}, nil
}
