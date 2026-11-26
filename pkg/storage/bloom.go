package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// BloomFilter is a compact probabilistic set (~10 bits/key, 7 hashes, ~0.8% false positive rate)
// used by each SSTable to skip disk reads for keys that definitely do not exist.
type BloomFilter struct {
	bits []byte
	k    uint8
}

// NewBloomFilter builds a Bloom filter sized for expectedKeys.
func NewBloomFilter(expectedKeys int) *BloomFilter {
	if expectedKeys < 16 {
		expectedKeys = 16
	}
	numBits := expectedKeys * 10 // ~10 bits per key -> ~0.8% false positives with k=7
	numBytes := (numBits + 7) / 8
	return &BloomFilter{
		bits: make([]byte, numBytes),
		k:    7,
	}
}

// Add inserts key into the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := doubleHash(key)
	m := uint64(len(bf.bits) * 8)
	for i := uint64(0); i < uint64(bf.k); i++ {
		pos := (h1 + i*h2) % m
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain returns false if key is GUARANTEED absent, or true if it might be present.
func (bf *BloomFilter) MayContain(key []byte) bool {
	if len(bf.bits) == 0 {
		return true
	}
	h1, h2 := doubleHash(key)
	m := uint64(len(bf.bits) * 8)
	for i := uint64(0); i < uint64(bf.k); i++ {
		pos := (h1 + i*h2) % m
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the Bloom filter as [k (1B) | bitset].
func (bf *BloomFilter) Encode() []byte {
	out := make([]byte, 1+len(bf.bits))
	out[0] = bf.k
	copy(out[1:], bf.bits)
	return out
}

// DecodeBloom deserializes a Bloom filter created by Encode.
func DecodeBloom(data []byte) (*BloomFilter, error) {
	if len(data) < 2 {
		return nil, errors.New("bloom: data too short")
	}
	bits := make([]byte, len(data)-1)
	copy(bits, data[1:])
	return &BloomFilter{
		bits: bits,
		k:    data[0],
	}, nil
}

// doubleHash derives two independent, well-distributed 64-bit hashes from SHA-256.
func doubleHash(key []byte) (uint64, uint64) {
	sum := sha256.Sum256(key)
	h1 := binary.LittleEndian.Uint64(sum[0:8])
	h2 := binary.LittleEndian.Uint64(sum[8:16]) | 1 // odd stride avoids degenerate cycles
	return h1, h2
}
