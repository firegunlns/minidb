package bptree

import (
	"github.com/golang/snappy"
)

// Compressor transparently compresses/decompresses node data at the
// cache-pager boundary.
type Compressor interface {
	Compress(data []byte) []byte
	Decompress(data []byte) ([]byte, error)
}

// SnappyCompressor uses snappy block compression.
// Format: [1B flag][payload] where flag=0 means raw, flag=1 means snappy.
type SnappyCompressor struct{}

func (c *SnappyCompressor) Compress(data []byte) []byte {
	compressed := snappy.Encode(nil, data)
	if len(compressed) >= len(data) {
		// Compression didn't help — store raw with flag=0.
		buf := make([]byte, 1+len(data))
		buf[0] = 0
		copy(buf[1:], data)
		return buf
	}
	buf := make([]byte, 1+len(compressed))
	buf[0] = 1
	copy(buf[1:], compressed)
	return buf
}

func (c *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	if data[0] == 0 {
		return data[1:], nil
	}
	return snappy.Decode(nil, data[1:])
}
