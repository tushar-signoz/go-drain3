package drain

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// Drain represents the main Drain3 instance with concurrent support
type Drain struct {
	config            *Config
	tokenizer         *Tokenizer
	shards            []*Shard
	shardMask         uint32
	logCounter        int64
	globalClusterCount int64      // atomic: total clusters across all shards
	evictionMu        sync.Mutex  // mutex for coordinating global eviction
	closed            int32
}

// New creates a new Drain instance
func New(config *Config) (*Drain, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	d := &Drain{
		config:            config,
		tokenizer:         NewTokenizer(config.ExtraDelimiters),
		shards:            make([]*Shard, config.ShardCount),
		shardMask:         uint32(config.ShardCount - 1),
		logCounter:        0,
		globalClusterCount: 0,
		closed:            0,
	}

	// Initialize shards
	for i := 0; i < config.ShardCount; i++ {
		d.shards[i] = NewShard(config)
		d.shards[i].drain = d // Set parent reference for global eviction
	}

	return d, nil
}

// Start starts the Drain instance (no-op now, kept for API compatibility)
func (d *Drain) Start() error {
	if atomic.LoadInt32(&d.closed) == 1 {
		return ErrDrainClosed
	}
	return nil
}

// ProcessLog processes a single log message (renamed from ProcessLogSync)
func (d *Drain) ProcessLog(content string) (*LogCluster, error) {
	if atomic.LoadInt32(&d.closed) == 1 {
		return nil, ErrDrainClosed
	}

	logID := atomic.AddInt64(&d.logCounter, 1)
	tokens := d.tokenizer.Tokenize(content)

	if len(tokens) == 0 {
		return nil, ErrProcessingFailed
	}

	// Get shard for this log
	shard := d.getShard(tokens)

	// Process in shard
	return shard.AddLogMessage(logID, tokens)
}

// Match matches a log message to a cluster without adding it
func (d *Drain) Match(content string) (*LogCluster, error) {
	if atomic.LoadInt32(&d.closed) == 1 {
		return nil, ErrDrainClosed
	}

	tokens := d.tokenizer.Tokenize(content)
	if len(tokens) == 0 {
		return nil, ErrProcessingFailed
	}

	// Get shard for this log
	shard := d.getShard(tokens)

	// Navigate tree and find match
	leafNode := shard.treeSearch(tokens)
	return shard.fastMatch(leafNode, tokens), nil
}

// GetClusters returns all clusters from all shards
func (d *Drain) GetClusters() []*LogCluster {
	var allClusters []*LogCluster

	for _, shard := range d.shards {
		clusters := shard.GetClusters()
		allClusters = append(allClusters, clusters...)
	}

	return allClusters
}

// GetClusterByID retrieves a cluster by ID
func (d *Drain) GetClusterByID(id int64) (*LogCluster, bool) {
	// Search all shards
	for _, shard := range d.shards {
		if cluster, exists := shard.GetClusterByID(id); exists {
			return cluster, true
		}
	}
	return nil, false
}

// GetClusterCount returns the total number of clusters
func (d *Drain) GetClusterCount() int64 {
	var total int64
	for _, shard := range d.shards {
		total += shard.GetClusterCount()
	}
	return total
}

// GetStats returns statistics about the Drain instance
func (d *Drain) GetStats() Stats {
	return Stats{
		TotalLogs:     atomic.LoadInt64(&d.logCounter),
		TotalClusters: d.GetClusterCount(),
		ShardCount:    len(d.shards),
	}
}

// Close shuts down the Drain instance gracefully
func (d *Drain) Close() error {
	if !atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		return ErrDrainClosed
	}
	return nil
}

// getShard determines which shard should handle a log message
func (d *Drain) getShard(tokens []string) *Shard {
	if len(tokens) == 0 {
		return d.shards[0]
	}

	// Hash the token length for shard selection
	// This ensures logs with same length go to same shard for better clustering
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%d", len(tokens))))
	shardIdx := h.Sum32() & d.shardMask

	return d.shards[shardIdx]
}

// Stats contains statistics about Drain instance
type Stats struct {
	TotalLogs     int64
	TotalClusters int64
	ShardCount    int
}

// PrintStats prints statistics about the Drain instance
func (d *Drain) PrintStats() {
	stats := d.GetStats()
	fmt.Printf("Drain Statistics:\n")
	fmt.Printf("  Total Logs:     %d\n", stats.TotalLogs)
	fmt.Printf("  Total Clusters: %d\n", stats.TotalClusters)
	fmt.Printf("  Shard Count:    %d\n", stats.ShardCount)

	if stats.TotalLogs > 0 {
		compressionRatio := (1.0 - float64(stats.TotalClusters)/float64(stats.TotalLogs)) * 100
		fmt.Printf("  Compression:    %.2f%%\n", compressionRatio)
	}
}

// canCreateCluster checks if we can create a new cluster without exceeding global limit
func (d *Drain) canCreateCluster() bool {
	return atomic.LoadInt64(&d.globalClusterCount) < int64(d.config.MaxClusters)
}

// incrementClusterCount atomically increments the global cluster count
func (d *Drain) incrementClusterCount() {
	atomic.AddInt64(&d.globalClusterCount, 1)
}

// decrementClusterCount atomically decrements the global cluster count
func (d *Drain) decrementClusterCount() {
	atomic.AddInt64(&d.globalClusterCount, -1)
}

// evictGlobally performs global eviction across all shards
// Returns the shard index and cluster that was evicted
func (d *Drain) evictGlobally() (*Shard, *LogCluster) {
	d.evictionMu.Lock()
	defer d.evictionMu.Unlock()

	// Find the best cluster to evict across ALL shards using hybrid strategy
	var bestShard *Shard
	var bestCluster *LogCluster
	var bestScore float64 = -1

	const (
		sizeThreshold    = 10
		staleThresholdMs = 3600000 // 1 hour
	)

	now := currentTimestamp()

	// Scan all shards to find the best eviction candidate globally
	for _, shard := range d.shards {
		clusters := shard.GetClusters()

		for _, cluster := range clusters {
			size := cluster.Size()
			lastAccess := cluster.LastAccessed()
			ageMs := (now - lastAccess) * 1000

			// Calculate eviction score (higher = better candidate for eviction)
			var score float64

			// Phase 1: Prioritize stale + small clusters
			if size < sizeThreshold && ageMs > staleThresholdMs {
				score = float64(ageMs) / 1000000.0 // Normalize age
				score += float64(sizeThreshold-size) * 10.0 // Prefer smaller
			} else if size < sizeThreshold {
				// Phase 2: Small clusters (even if not stale)
				score = float64(sizeThreshold-size) * 5.0
			} else {
				// Phase 3: Just use age for large clusters
				score = float64(ageMs) / 10000000.0 // Lower priority
			}

			if score > bestScore {
				bestScore = score
				bestCluster = cluster
				bestShard = shard
			}
		}
	}

	return bestShard, bestCluster
}
