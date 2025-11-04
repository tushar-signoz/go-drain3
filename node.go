package drain

import (
	"sync"
)

// NodeType represents the type of a tree node
type NodeType int

const (
	NodeTypeInternal NodeType = iota // Internal node with children
	NodeTypeLeaf                      // Leaf node containing clusters
)

// Node represents a node in the Drain parse tree
type Node struct {
	mu       sync.RWMutex
	nodeType NodeType
	depth    int

	// For internal nodes
	keyToChildNode map[string]*Node

	// For leaf nodes
	clusters []*LogCluster
}

// NewInternalNode creates a new internal node
func NewInternalNode(depth int) *Node {
	return &Node{
		nodeType:       NodeTypeInternal,
		depth:          depth,
		keyToChildNode: make(map[string]*Node),
		clusters:       nil,
	}
}

// NewLeafNode creates a new leaf node
func NewLeafNode(depth int) *Node {
	return &Node{
		nodeType:       NodeTypeLeaf,
		depth:          depth,
		keyToChildNode: nil,
		clusters:       make([]*LogCluster, 0),
	}
}

// IsLeaf returns true if this is a leaf node
func (n *Node) IsLeaf() bool {
	return n.nodeType == NodeTypeLeaf
}

// GetChild retrieves a child node by key
func (n *Node) GetChild(key string) (*Node, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	child, exists := n.keyToChildNode[key]
	return child, exists
}

// AddChild adds or updates a child node
func (n *Node) AddChild(key string, child *Node) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.keyToChildNode[key] = child
}

// GetOrCreateChild gets or creates a child node
// Enforces MaxChildren limit by mapping to wildcard "<*>" when limit is reached
func (n *Node) GetOrCreateChild(key string, createLeaf bool, maxDepth int, maxChildren int) *Node {
	// Try read lock first
	n.mu.RLock()
	if child, exists := n.keyToChildNode[key]; exists {
		n.mu.RUnlock()
		return child
	}
	n.mu.RUnlock()

	// Need to create, use write lock
	n.mu.Lock()
	defer n.mu.Unlock()

	// Double-check after acquiring write lock
	if child, exists := n.keyToChildNode[key]; exists {
		return child
	}

	// Check MaxChildren limit - if exceeded, use wildcard node
	if len(n.keyToChildNode) >= maxChildren {
		// Check if wildcard node already exists
		if wildcardChild, exists := n.keyToChildNode["<*>"]; exists {
			return wildcardChild
		}
		// Create wildcard node if it doesn't exist
		key = "<*>"
	}

	// Create new child
	var child *Node
	if createLeaf || n.depth+1 >= maxDepth {
		child = NewLeafNode(n.depth + 1)
	} else {
		child = NewInternalNode(n.depth + 1)
	}

	n.keyToChildNode[key] = child
	return child
}

// GetClusters returns clusters from a leaf node
func (n *Node) GetClusters() []*LogCluster {
	if !n.IsLeaf() {
		return nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make([]*LogCluster, len(n.clusters))
	copy(result, n.clusters)
	return result
}

// AddCluster adds a cluster to a leaf node
func (n *Node) AddCluster(cluster *LogCluster) {
	if !n.IsLeaf() {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.clusters = append(n.clusters, cluster)
}

// GetAllChildren returns all child nodes
func (n *Node) GetAllChildren() map[string]*Node {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make(map[string]*Node, len(n.keyToChildNode))
	for k, v := range n.keyToChildNode {
		result[k] = v
	}
	return result
}

// GetChildrenKeys returns all child keys
func (n *Node) GetChildrenKeys() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	keys := make([]string, 0, len(n.keyToChildNode))
	for k := range n.keyToChildNode {
		keys = append(keys, k)
	}
	return keys
}

// GetClusterCount returns the number of clusters in a leaf node
func (n *Node) GetClusterCount() int {
	if !n.IsLeaf() {
		return 0
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.clusters)
}

// GetChildCount returns the number of child nodes
func (n *Node) GetChildCount() int {
	if n.IsLeaf() {
		return 0
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.keyToChildNode)
}

// GetChildren returns a slice of all child nodes
func (n *Node) GetChildren() []*Node {
	if n.IsLeaf() {
		return nil
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	children := make([]*Node, 0, len(n.keyToChildNode))
	for _, child := range n.keyToChildNode {
		children = append(children, child)
	}
	return children
}

// RemoveCluster removes a specific cluster from a leaf node
func (n *Node) RemoveCluster(cluster *LogCluster) bool {
	if !n.IsLeaf() {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Find and remove the cluster
	for i, c := range n.clusters {
		if c.ID() == cluster.ID() {
			// Remove by swapping with last element and truncating
			n.clusters[i] = n.clusters[len(n.clusters)-1]
			n.clusters = n.clusters[:len(n.clusters)-1]
			return true
		}
	}

	return false
}
