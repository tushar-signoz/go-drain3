package drain

// Config holds the configuration parameters for Drain3
type Config struct {
	// Core Drain parameters
	Depth           int      // Depth of the parse tree (typically 4-6)
	SimTh           float64  // Similarity threshold (0-1)
	MaxChildren     int      // Maximum number of children per node
	MaxClusters     int      // Maximum number of log clusters
	ExtraDelimiters []string // Additional delimiters for tokenization
	ParamString     string   // Placeholder for parameters (e.g., "<*>")

	// Sharding for concurrent access
	ShardCount int // Number of shards for concurrent access (must be power of 2)
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Depth:           5,
		SimTh:           0.5,
		MaxChildren:     100,
		MaxClusters:     1024,
		ExtraDelimiters: []string{":", "=", ",", "|", ";", "/"},
		ParamString:     "<*>",
		ShardCount:      16, // Must be power of 2
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Depth < 2 {
		return ErrInvalidDepth
	}
	if c.SimTh < 0 || c.SimTh > 1 {
		return ErrInvalidSimThreshold
	}
	if c.MaxChildren < 1 {
		return ErrInvalidMaxChildren
	}
	if c.MaxClusters < 1 {
		return ErrInvalidMaxClusters
	}
	if c.ShardCount < 1 || (c.ShardCount&(c.ShardCount-1)) != 0 {
		return ErrInvalidShardCount
	}
	return nil
}
