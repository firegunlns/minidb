// Package bptree 实现了 B+ 树数据结构
package bptree

// Config 持久化B+树的可选配置
type Config struct {
	BloomEnabled    bool // 是否启用Bloom过滤器
	BloomBitsPerKey int  // 每个键的位数，默认10（约1%误报率）

	CompressionEnabled bool // 是否启用压缩
}

// Option applies a configuration to Config.
type Option func(*Config)

// WithBloomFilter enables per-leaf bloom filters with the given bits-per-key.
// Typical values: 10 for ~1% FPR, 20 for ~0.01% FPR.
func WithBloomFilter(bitsPerKey int) Option {
	return func(c *Config) {
		c.BloomEnabled = true
		c.BloomBitsPerKey = bitsPerKey
	}
}

// WithCompression enables snappy block compression for node pages.
func WithCompression() Option {
	return func(c *Config) {
		c.CompressionEnabled = true
	}
}

func defaultConfig() Config {
	return Config{
		BloomBitsPerKey: 10,
	}
}
