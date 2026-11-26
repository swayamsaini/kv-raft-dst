package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	sstMagic      uint32 = 0x53535431 // "SST1"
	sstFooterSize        = 28         // IndexOff(8) + IndexLen(4) + BloomOff(8) + BloomLen(4) + Magic(4)
)

type indexEntry struct {
	key    []byte
	offset int64
}

// SSTable is an immutable, sorted on-disk table with an in-memory index and Bloom filter.
type SSTable struct {
	id         uint64
	path       string
	file       *os.File
	dataEnd    int64
	index      []indexEntry
	bloom      *BloomFilter
	fileSize   int64
}

// WriteSSTable atomically writes sorted entries to path using a temp file + fsync + rename.
func WriteSSTable(path string, id uint64, entries []Entry) (*SSTable, error) {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %s: %w", tmpPath, err)
	}

	bloom := NewBloomFilter(len(entries))
	index := make([]indexEntry, 0, len(entries))
	var offset int64

	// 1. Write sorted Data Section: [Deleted(1B) | KeyLen(4B) | Key | ValLen(4B) | Value]
	for _, e := range entries {
		bloom.Add(e.Key)
		index = append(index, indexEntry{key: cloneBytes(e.Key), offset: offset})

		recLen := 1 + 4 + len(e.Key) + 4 + len(e.Value)
		buf := make([]byte, recLen)
		if e.Deleted {
			buf[0] = 1
		}
		pos := 1
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(e.Key)))
		pos += 4
		copy(buf[pos:pos+len(e.Key)], e.Key)
		pos += len(e.Key)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(e.Value)))
		pos += 4
		copy(buf[pos:pos+len(e.Value)], e.Value)

		n, err := f.Write(buf)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("sstable: write data: %w", err)
		}
		offset += int64(n)
	}

	// 2. Write Index Section: [KeyLen(4B) | Key | Offset(8B)]
	indexOffset := offset
	var indexBuf bytes.Buffer
	tmp4 := make([]byte, 4)
	tmp8 := make([]byte, 8)
	for _, idx := range index {
		binary.LittleEndian.PutUint32(tmp4, uint32(len(idx.key)))
		indexBuf.Write(tmp4)
		indexBuf.Write(idx.key)
		binary.LittleEndian.PutUint64(tmp8, uint64(idx.offset))
		indexBuf.Write(tmp8)
	}
	indexBytes := indexBuf.Bytes()
	if _, err := f.Write(indexBytes); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable: write index: %w", err)
	}
	offset += int64(len(indexBytes))

	// 3. Write Bloom Filter Section
	bloomOffset := offset
	bloomBytes := bloom.Encode()
	if _, err := f.Write(bloomBytes); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable: write bloom: %w", err)
	}
	offset += int64(len(bloomBytes))

	// 4. Write 28-byte Footer
	footer := make([]byte, sstFooterSize)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(len(indexBytes)))
	binary.LittleEndian.PutUint64(footer[12:20], uint64(bloomOffset))
	binary.LittleEndian.PutUint32(footer[20:24], uint32(len(bloomBytes)))
	binary.LittleEndian.PutUint32(footer[24:28], sstMagic)
	if _, err := f.Write(footer); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable: write footer: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable: close tmp: %w", err)
	}

	// Atomically promote temp file to final immutable SSTable path.
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("sstable: rename: %w", err)
	}

	return OpenSSTable(path, id)
}

// OpenSSTable opens an existing immutable SSTable and loads its index and Bloom filter into RAM.
func OpenSSTable(path string, id uint64) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := stat.Size()
	if size < sstFooterSize {
		_ = f.Close()
		return nil, errors.New("sstable: file too small for footer")
	}

	footer := make([]byte, sstFooterSize)
	if _, err := f.ReadAt(footer, size-sstFooterSize); err != nil {
		_ = f.Close()
		return nil, err
	}

	indexOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexLen := int(binary.LittleEndian.Uint32(footer[8:12]))
	bloomOff := int64(binary.LittleEndian.Uint64(footer[12:20]))
	bloomLen := int(binary.LittleEndian.Uint32(footer[20:24]))
	magic := binary.LittleEndian.Uint32(footer[24:28])
	if magic != sstMagic {
		_ = f.Close()
		return nil, fmt.Errorf("sstable: bad magic 0x%08x", magic)
	}

	// Load Index
	indexRaw := make([]byte, indexLen)
	if _, err := f.ReadAt(indexRaw, indexOff); err != nil {
		_ = f.Close()
		return nil, err
	}
	var index []indexEntry
	pos := 0
	for pos < len(indexRaw) {
		kLen := int(binary.LittleEndian.Uint32(indexRaw[pos : pos+4]))
		pos += 4
		key := make([]byte, kLen)
		copy(key, indexRaw[pos:pos+kLen])
		pos += kLen
		off := int64(binary.LittleEndian.Uint64(indexRaw[pos : pos+8]))
		pos += 8
		index = append(index, indexEntry{key: key, offset: off})
	}

	// Load Bloom Filter
	bloomRaw := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomRaw, bloomOff); err != nil {
		_ = f.Close()
		return nil, err
	}
	bloom, err := DecodeBloom(bloomRaw)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &SSTable{
		id:       id,
		path:     path,
		file:     f,
		dataEnd:  indexOff,
		index:    index,
		bloom:    bloom,
		fileSize: size,
	}, nil
}

// Get looks up key using the Bloom filter first, then binary search over the in-memory index.
func (s *SSTable) Get(key []byte) ([]byte, bool, bool, error) {
	if !s.bloom.MayContain(key) {
		return nil, false, false, nil
	}

	idx := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].key, key) >= 0
	})
	if idx >= len(s.index) || !bytes.Equal(s.index[idx].key, key) {
		return nil, false, false, nil
	}

	entry, _, err := s.readEntryAt(s.index[idx].offset)
	if err != nil {
		return nil, false, false, err
	}
	return entry.Value, true, entry.Deleted, nil
}

// Entries reads all sorted entries from the SSTable (used during Scan and Compaction).
func (s *SSTable) Entries() ([]Entry, error) {
	out := make([]Entry, 0, len(s.index))
	var off int64
	for off < s.dataEnd {
		entry, nextOff, err := s.readEntryAt(off)
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
		off = nextOff
	}
	return out, nil
}

func (s *SSTable) readEntryAt(offset int64) (Entry, int64, error) {
	hdr := make([]byte, 5) // Deleted(1B) + KeyLen(4B)
	if _, err := s.file.ReadAt(hdr, offset); err != nil {
		return Entry{}, 0, err
	}
	deleted := hdr[0] == 1
	kLen := int(binary.LittleEndian.Uint32(hdr[1:5]))

	keyAndValLen := make([]byte, kLen+4)
	if _, err := s.file.ReadAt(keyAndValLen, offset+5); err != nil {
		return Entry{}, 0, err
	}
	key := keyAndValLen[:kLen]
	vLen := int(binary.LittleEndian.Uint32(keyAndValLen[kLen : kLen+4]))

	var val []byte
	if vLen > 0 {
		val = make([]byte, vLen)
		if _, err := s.file.ReadAt(val, offset+5+int64(kLen)+4); err != nil {
			if !errors.Is(err, io.EOF) {
				return Entry{}, 0, err
			}
		}
	}

	nextOff := offset + 5 + int64(kLen) + 4 + int64(vLen)
	return Entry{
		Key:     cloneBytes(key),
		Value:   val,
		Deleted: deleted,
	}, nextOff, nil
}

// SizeBytes returns the on-disk file size of the SSTable.
func (s *SSTable) SizeBytes() int64 {
	return s.fileSize
}

// Close closes the underlying file descriptor.
func (s *SSTable) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}
