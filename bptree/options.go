package bptree

// Config holds optional features for PersistentBPTree.
type Config struct {
	BloomEnabled    bool
	BloomBitsPerKey int // default 10 (~1% FPR)

	CompressionEnabled bool
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
