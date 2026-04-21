// Package bptree 实现了 B+ 树数据结构
package bptree

import (
	"encoding/binary"
	"hash/fnv"
)

// BloomFilter Bloom过滤器
// 一种空间高效的概率数据结构，用于快速判断元素是否可能存在
// 可能存在误报（false positive），但不会漏报（false negative）
// 用于快速判断查询的键是否可能存在于叶子节点中，避免不必要的磁盘读取
type BloomFilter struct {
	bits    []uint64 // 位数组
	numBits uint32   // 位数量
	numHash uint8    // 哈希函数数量
}

// NewBloomFilter creates a BloomFilter sized for expectedKeys with the given
// bits-per-key ratio. Typical values: bitsPerKey=10 for ~1% FPR with 7 hashes.
func NewBloomFilter(expectedKeys int, bitsPerKey int) *BloomFilter {
	if expectedKeys <= 0 {
		expectedKeys = 1
	}
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}
	numBits := uint32(expectedKeys * bitsPerKey)
	if numBits < 64 {
		numBits = 64
	}
	// Round up to multiple of 64 for uint64 alignment.
	numBits = (numBits + 63) &^ 63

	numHash := optimalNumHash(bitsPerKey)

	return &BloomFilter{
		bits:    make([]uint64, numBits/64),
		numBits: numBits,
		numHash: numHash,
	}
}

func optimalNumHash(bitsPerKey int) uint8 {
	// k = (bitsPerKey) * ln(2). Common values:
	// 6 bits/key → 4 hashes (5% FPR)
	// 10 bits/key → 7 hashes (1% FPR)
	// 20 bits/key → 14 hashes (0.01% FPR)
	k := uint8(float64(bitsPerKey) * 0.6931471805599453) // ln(2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// Add inserts a key into the bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hashPair(key)
	for i := uint8(0); i < bf.numHash; i++ {
		idx := (h1 + uint32(i)*h2) % bf.numBits
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
}

// MayContain returns true if the key might be in the set, false if it is
// definitely not.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := hashPair(key)
	for i := uint8(0); i < bf.numHash; i++ {
		idx := (h1 + uint32(i)*h2) % bf.numBits
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

// Rebuild reconstructs the bloom filter from a set of sorted keys.
func (bf *BloomFilter) Rebuild(keys [][]byte) {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	for _, k := range keys {
		bf.Add(k)
	}
}

// Serialize writes the bloom filter to a byte slice.
// Format: [numHash 1B][numBits 4B][bits...]
func (bf *BloomFilter) Serialize() []byte {
	size := 1 + 4 + len(bf.bits)*8
	buf := make([]byte, size)
	buf[0] = bf.numHash
	binary.LittleEndian.PutUint32(buf[1:], bf.numBits)
	for i, word := range bf.bits {
		binary.LittleEndian.PutUint64(buf[5+i*8:], word)
	}
	return buf
}

// DeserializeBloomFilter reconstructs a bloom filter from serialized bytes.
func DeserializeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 5 {
		return nil
	}
	numHash := data[0]
	numBits := binary.LittleEndian.Uint32(data[1 : 4+1])
	if numBits == 0 || numBits%64 != 0 {
		return nil
	}
	words := int(numBits / 64)
	if len(data) < 5+words*8 {
		return nil
	}
	bits := make([]uint64, words)
	for i := 0; i < words; i++ {
		bits[i] = binary.LittleEndian.Uint64(data[5+i*8:])
	}
	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: numHash,
	}
}

// hashPair returns two independent 32-bit hashes using FNV-1a and FNV-0.
func hashPair(key []byte) (uint32, uint32) {
	h1 := fnv.New32a()
	h1.Write(key)
	h2 := fnv.New32()
	h2.Write(key)
	return h1.Sum32(), h2.Sum32()
}
