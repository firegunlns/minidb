// Package bptree 实现了 B+ 树数据结构
package bptree

import (
	"github.com/golang/snappy"
)

// Compressor 压缩器接口
// 在缓存-页面管理器边界透明地压缩/解压缩节点数据
type Compressor interface {
	Compress(data []byte) []byte            // 压缩数据
	Decompress(data []byte) ([]byte, error) // 解压缩数据
}

// SnappyCompressor 使用Snappy块压缩
// 格式：[1字节标志][数据] 标志=0表示原始，标志=1表示Snappy压缩
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
