package drain

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// Shard represents a partition of the Drain tree for concurrent access
type Shard struct {
	mu          sync.RWMutex
	root        *Node
	idToClusters map[int64]*LogCluster
	clusterCount int64
	config      *Config
	drain       *Drain // reference to parent for global eviction
}

// NewShard creates a new shard
func NewShard(config *Config) *Shard {
	return &Shard{
		root:        NewInternalNode(0),
		idToClusters: make(map[int64]*LogCluster),
		clusterCount: 0,
		config:      config,
		drain:       nil, // will be set by Drain.New()
	}
}

// AddLogMessage processes a log message in this shard
func (s *Shard) AddLogMessage(logID int64, tokens []string) (*LogCluster, error) {
	if len(tokens) == 0 {
		return nil, ErrProcessingFailed
	}

	// Navigate the tree
	leafNode := s.treeSearch(tokens)

	// Search for matching cluster in leaf node
	cluster := s.fastMatch(leafNode, tokens)

	if cluster != nil {
		// Add to existing cluster
		cluster.Add(logID, tokens)
		return cluster, nil
	}

	// Create new cluster - check global limit first
	if s.drain != nil && !s.drain.canCreateCluster() {
		// Global limit reached - need to evict globally
		evictShard, evictCluster := s.drain.evictGlobally()
		if evictShard != nil && evictCluster != nil {
			// Remove the evicted cluster from its shard
			evictShard.removeCluster(evictCluster)
			s.drain.decrementClusterCount()
		}
	}

	// Try to find best match if we still can't create
	cluster = s.findBestMatch(leafNode, tokens)
	if cluster != nil {
		cluster.Add(logID, tokens)
		return cluster, nil
	}

	// Create new cluster
	s.mu.Lock()
	defer s.mu.Unlock()

	newClusterID := atomic.AddInt64(&s.clusterCount, 1)
	newCluster := NewLogCluster(newClusterID, tokens)
	newCluster.Add(logID, tokens)

	s.idToClusters[newClusterID] = newCluster
	leafNode.AddCluster(newCluster)

	// Increment global counter
	if s.drain != nil {
		s.drain.incrementClusterCount()
	}

	return newCluster, nil
}

// treeSearch navigates the parse tree based on log tokens
func (s *Shard) treeSearch(tokens []string) *Node {
	currentNode := s.root
	depth := 0
	tokenCount := len(tokens)

	// Navigate tree based on token count and content
	for depth < s.config.Depth-1 {
		if currentNode.IsLeaf() {
			return currentNode
		}

		// First level: group by token count
		if depth == 0 {
			key := getTokenCountKey(tokenCount)
			currentNode = currentNode.GetOrCreateChild(key, false, s.config.Depth, s.config.MaxChildren)
			depth++
			continue
		}

		// Subsequent levels: group by token category
		tokenIdx := depth - 1
		if tokenIdx >= len(tokens) {
			// Create leaf node if we've exhausted tokens
			key := "<END>"
			currentNode = currentNode.GetOrCreateChild(key, true, s.config.Depth, s.config.MaxChildren)
			depth++
			continue
		}

		key := TokenCategory(tokens[tokenIdx])
		currentNode = currentNode.GetOrCreateChild(key, depth >= s.config.Depth-2, s.config.Depth, s.config.MaxChildren)
		depth++
	}

	// Ensure we return a leaf node
	if !currentNode.IsLeaf() {
		key := "<LEAF>"
		currentNode = currentNode.GetOrCreateChild(key, true, s.config.Depth, s.config.MaxChildren)
	}

	return currentNode
}

// fastMatch finds a matching cluster in a leaf node
func (s *Shard) fastMatch(node *Node, tokens []string) *LogCluster {
	clusters := node.GetClusters()

	var bestMatch *LogCluster
	var bestSim float64 = 0

	for _, cluster := range clusters {
		sim := cluster.CalculateSimilarity(tokens)
		if sim >= s.config.SimTh && sim > bestSim {
			bestSim = sim
			bestMatch = cluster
		}
	}

	// Update last accessed time for LRU tracking
	if bestMatch != nil {
		bestMatch.Touch()
	}

	return bestMatch
}

// findBestMatch finds the cluster with the highest similarity above threshold
func (s *Shard) findBestMatch(node *Node, tokens []string) *LogCluster {
	clusters := node.GetClusters()

	var bestMatch *LogCluster
	var bestSim float64 = 0

	for _, cluster := range clusters {
		sim := cluster.CalculateSimilarity(tokens)
		if sim > bestSim {
			bestSim = sim
			bestMatch = cluster
		}
	}

	if bestSim >= s.config.SimTh {
		return bestMatch
	}

	return nil
}

// GetClusters returns all clusters in this shard
func (s *Shard) GetClusters() []*LogCluster {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clusters := make([]*LogCluster, 0, len(s.idToClusters))
	for _, cluster := range s.idToClusters {
		clusters = append(clusters, cluster)
	}
	return clusters
}

// GetClusterByID retrieves a cluster by ID
func (s *Shard) GetClusterByID(id int64) (*LogCluster, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, exists := s.idToClusters[id]
	return cluster, exists
}

// GetClusterCount returns the number of clusters in this shard
func (s *Shard) GetClusterCount() int64 {
	return atomic.LoadInt64(&s.clusterCount)
}

// getTokenCountKey generates a key based on token count
func getTokenCountKey(count int) string {
	// Group token counts into buckets to reduce tree branching
	if count <= 5 {
		return string(rune('0' + count))
	} else if count <= 10 {
		return "6-10"
	} else if count <= 20 {
		return "11-20"
	} else if count <= 50 {
		return "21-50"
	} else {
		return "50+"
	}
}

// hashString computes hash for a string
func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// evictClusterHybrid implements a hybrid eviction strategy combining LRU and LFU
// Strategy:
// 1. Look for clusters that are both stale (LRU) AND small (LFU) - these are best candidates
// 2. If none found, evict the smallest cluster (LFU)
// 3. If all clusters are large, evict least recently used (LRU)
func (s *Shard) evictClusterHybrid() *LogCluster {
	if len(s.idToClusters) == 0 {
		return nil
	}

	const (
		sizeThreshold    = 10   // Clusters with size < 10 are considered "small"
		staleThresholdMs = 3600000 // 1 hour in milliseconds
	)

	now := currentTimestamp()
	var evictCandidate *LogCluster
	var smallestSize int64 = int64(^uint64(0) >> 1) // MaxInt64
	var oldestAccess int64 = now

	// Phase 1: Find clusters that are both stale AND small (ideal candidates)
	for _, cluster := range s.idToClusters {
		size := cluster.Size()
		lastAccess := cluster.LastAccessed()
		ageMs := (now - lastAccess) * 1000 // Convert to milliseconds

		// Perfect candidate: small and stale
		if size < sizeThreshold && ageMs > staleThresholdMs {
			if lastAccess < oldestAccess || (lastAccess == oldestAccess && size < smallestSize) {
				oldestAccess = lastAccess
				smallestSize = size
				evictCandidate = cluster
			}
		}
	}

	// Phase 2: If no stale+small clusters, find smallest cluster regardless of age
	if evictCandidate == nil {
		for _, cluster := range s.idToClusters {
			size := cluster.Size()
			if size < smallestSize {
				smallestSize = size
				evictCandidate = cluster
			}
		}
	}

	// Phase 3: If all clusters are large, evict the least recently used
	if evictCandidate == nil || smallestSize >= sizeThreshold*2 {
		for _, cluster := range s.idToClusters {
			lastAccess := cluster.LastAccessed()
			if lastAccess < oldestAccess {
				oldestAccess = lastAccess
				evictCandidate = cluster
			}
		}
	}

	return evictCandidate
}

// removeClusterFromTree removes a cluster from the parse tree
func (s *Shard) removeClusterFromTree(cluster *LogCluster) {
	if cluster == nil {
		return
	}

	// We need to traverse the tree to find and remove this cluster
	// This is a simplified version - in production you might want to track
	// which node contains each cluster for O(1) removal
	s.removeClusterFromNode(s.root, cluster)
}

// removeClusterFromNode recursively searches for and removes a cluster from nodes
func (s *Shard) removeClusterFromNode(node *Node, cluster *LogCluster) bool {
	if node == nil {
		return false
	}

	// If this is a leaf node, try to remove the cluster
	if node.IsLeaf() {
		return node.RemoveCluster(cluster)
	}

	// Recursively search children
	children := node.GetChildren()
	for _, child := range children {
		if s.removeClusterFromNode(child, cluster) {
			return true
		}
	}

	return false
}

// removeCluster removes a cluster from the shard (both tree and map)
func (s *Shard) removeCluster(cluster *LogCluster) {
	if cluster == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove from tree
	s.removeClusterFromTree(cluster)

	// Remove from map
	delete(s.idToClusters, cluster.ID())

	// Decrement local count
	atomic.AddInt64(&s.clusterCount, -1)
}
