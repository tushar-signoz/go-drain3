package drain

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// LogCluster represents a cluster of similar log messages
type LogCluster struct {
	mu           sync.RWMutex
	id           int64
	tokens       []string
	logIDs       []int64
	size         int64 // atomic counter
	created      int64 // unix timestamp
	lastUpdated  int64 // unix timestamp
	lastAccessed int64 // atomic: unix timestamp of last access (for LRU)
}

// NewLogCluster creates a new log cluster
func NewLogCluster(id int64, tokens []string) *LogCluster {
	now := currentTimestamp()
	cluster := &LogCluster{
		id:           id,
		tokens:       make([]string, len(tokens)),
		logIDs:       make([]int64, 0, 100),
		size:         0,
		created:      now,
		lastUpdated:  now,
		lastAccessed: now,
	}
	copy(cluster.tokens, tokens)
	return cluster
}

// ID returns the cluster ID
func (c *LogCluster) ID() int64 {
	return c.id
}

// GetTokens returns a copy of the cluster tokens
func (c *LogCluster) GetTokens() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]string, len(c.tokens))
	copy(result, c.tokens)
	return result
}

// Size returns the number of log messages in this cluster
func (c *LogCluster) Size() int64 {
	return atomic.LoadInt64(&c.size)
}

// Add adds a log message to the cluster
func (c *LogCluster) Add(logID int64, tokens []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := currentTimestamp()

	// Update template tokens
	c.updateTemplate(tokens)

	// Add log ID
	c.logIDs = append(c.logIDs, logID)

	// Increment size atomically
	atomic.AddInt64(&c.size, 1)

	// Update timestamps
	c.lastUpdated = now
	atomic.StoreInt64(&c.lastAccessed, now)
}

// updateTemplate merges new tokens with existing template
// Tokens that differ become wildcards
func (c *LogCluster) updateTemplate(tokens []string) {
	if len(tokens) != len(c.tokens) {
		return
	}

	for i := 0; i < len(tokens); i++ {
		if c.tokens[i] != tokens[i] && c.tokens[i] != "<*>" {
			c.tokens[i] = "<*>"
		}
	}
}

// String returns the string representation of the cluster template
func (c *LogCluster) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return strings.Join(c.tokens, "")
}

// GetTemplate returns the template string
func (c *LogCluster) GetTemplate() string {
	return c.String()
}

// GetLogIDs returns a copy of log IDs in this cluster
func (c *LogCluster) GetLogIDs() []int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]int64, len(c.logIDs))
	copy(result, c.logIDs)
	return result
}

// CalculateSimilarity computes similarity between tokens and cluster template
// Returns a value between 0 and 1, where 1 is identical
func (c *LogCluster) CalculateSimilarity(tokens []string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(tokens) != len(c.tokens) {
		return 0
	}

	matchCount := 0
	for i := 0; i < len(tokens); i++ {
		if c.tokens[i] == tokens[i] || c.tokens[i] == "<*>" {
			matchCount++
		}
	}

	return float64(matchCount) / float64(len(tokens))
}

// Touch updates the last accessed timestamp (for LRU tracking)
func (c *LogCluster) Touch() {
	atomic.StoreInt64(&c.lastAccessed, currentTimestamp())
}

// LastAccessed returns the last access timestamp
func (c *LogCluster) LastAccessed() int64 {
	return atomic.LoadInt64(&c.lastAccessed)
}

// ClusterStats provides statistics about a cluster
type ClusterStats struct {
	ID          int64
	Template    string
	Size        int64
	Created     int64
	LastUpdated int64
}

// GetStats returns cluster statistics
func (c *LogCluster) GetStats() ClusterStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return ClusterStats{
		ID:          c.id,
		Template:    strings.Join(c.tokens, ""),
		Size:        atomic.LoadInt64(&c.size),
		Created:     c.created,
		LastUpdated: c.lastUpdated,
	}
}

// PrintCluster formats cluster for display
func (c *LogCluster) PrintCluster() string {
	stats := c.GetStats()
	return fmt.Sprintf("Cluster ID: %d | Size: %d | Template: %s",
		stats.ID, stats.Size, stats.Template)
}

func currentTimestamp() int64 {
	return nowFunc().Unix()
}
