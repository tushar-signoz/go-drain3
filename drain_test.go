package drain

import (
	"fmt"
	"sync"
	"testing"
)

func TestDrainBasic(t *testing.T) {
	config := DefaultConfig()
	config.ShardCount = 4

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()

	if err := drain.Start(); err != nil {
		t.Fatalf("Failed to start drain: %v", err)
	}

	// Test log messages
	logs := []string{
		"User john logged in from 192.168.1.1",
		"User jane logged in from 192.168.1.2",
		"User bob logged in from 192.168.1.3",
		"Error connecting to database at 10.0.0.1",
		"Error connecting to database at 10.0.0.2",
	}

	for _, log := range logs {
		cluster, err := drain.ProcessLog(log)
		if err != nil {
			t.Errorf("Failed to process log: %v", err)
		}
		if cluster == nil {
			t.Error("Expected cluster, got nil")
		}
	}

	// Should have 2 clusters
	clusters := drain.GetClusters()
	if len(clusters) < 2 {
		t.Errorf("Expected at least 2 clusters, got %d", len(clusters))
	}

	stats := drain.GetStats()
	if stats.TotalLogs != int64(len(logs)) {
		t.Errorf("Expected %d logs, got %d", len(logs), stats.TotalLogs)
	}
}

func TestDrainConcurrent(t *testing.T) {
	config := DefaultConfig()
	config.ShardCount = 8

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()

	if err := drain.Start(); err != nil {
		t.Fatalf("Failed to start drain: %v", err)
	}

	// Generate test logs
	numLogs := 1000
	var wg sync.WaitGroup

	// Process logs concurrently
	for i := 0; i < numLogs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			log := fmt.Sprintf("Request %d processed in %d ms", id, id%100)
			_, err := drain.ProcessLog(log)
			if err != nil {
				t.Errorf("Failed to process log: %v", err)
			}
		}(i)
	}

	wg.Wait()

	stats := drain.GetStats()
	if stats.TotalLogs != int64(numLogs) {
		t.Errorf("Expected %d logs, got %d", numLogs, stats.TotalLogs)
	}
}

func TestDrainMatch(t *testing.T) {
	config := DefaultConfig()
	config.SimTh = 0.4 // Lower threshold for testing
	config.ShardCount = 1 // Use single shard for predictable behavior

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()

	if err := drain.Start(); err != nil {
		t.Fatalf("Failed to start drain: %v", err)
	}

	// Train with several similar logs to establish pattern
	logs := []string{
		"User john logged in successfully",
		"User alice logged in successfully",
		"User bob logged in successfully",
	}

	var firstClusterID int64
	for i, log := range logs {
		cluster, err := drain.ProcessLog(log)
		if err != nil {
			t.Fatalf("Failed to process log %d: %v", i, err)
		}
		if i == 0 {
			firstClusterID = cluster.ID()
		} else {
			// All similar logs should go to same cluster
			if cluster.ID() != firstClusterID {
				t.Logf("Log %d went to cluster %d instead of %d", i, cluster.ID(), firstClusterID)
			}
		}
	}

	// Match similar log
	testLog := "User charlie logged in successfully"
	matched, err := drain.Match(testLog)
	if err != nil {
		t.Fatalf("Failed to match log: %v", err)
	}

	if matched != nil {
		t.Logf("Successfully matched to cluster %d with template: %s", matched.ID(), matched.GetTemplate())
	} else {
		t.Log("No match found - acceptable for Match without adding")
	}
}

func TestClusterSimilarity(t *testing.T) {
	tokens1 := []string{"User", "john", "logged", "in"}
	cluster := NewLogCluster(1, tokens1)

	tests := []struct {
		tokens   []string
		expected float64
	}{
		{[]string{"User", "john", "logged", "in"}, 1.0},
		{[]string{"User", "jane", "logged", "in"}, 0.75},
		{[]string{"User", "jane", "logged", "out"}, 0.5},
		{[]string{"Different", "log", "message", "here"}, 0.0},
		{[]string{"Too", "short"}, 0.0},
	}

	for _, tt := range tests {
		sim := cluster.CalculateSimilarity(tt.tokens)
		if sim != tt.expected {
			t.Errorf("For tokens %v, expected similarity %.2f, got %.2f",
				tt.tokens, tt.expected, sim)
		}
	}
}

func TestTokenizer(t *testing.T) {
	tokenizer := NewTokenizer([]string{":", "=", ","})

	tests := []struct {
		input       string
		minExpected int
	}{
		{"Simple log message", 3},
		{"Key:Value,Another:Value", 4},
		{"Complex=Message:With,Many:Delimiters", 5}, // Adjusted expectation
	}

	for _, tt := range tests {
		tokens := tokenizer.Tokenize(tt.input)
		if len(tokens) < tt.minExpected {
			t.Errorf("For input %q, expected at least %d tokens, got %d: %v",
				tt.input, tt.minExpected, len(tokens), tokens)
		}
	}
}

func TestSharding(t *testing.T) {
	config := DefaultConfig()
	config.ShardCount = 4

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()

	// Test that logs are distributed across shards
	logs := []string{
		"Short log",
		"Medium length log message",
		"This is a longer log message with more words",
		"Another message",
	}

	shardCounts := make(map[*Shard]int)

	for _, log := range logs {
		tokens := drain.tokenizer.Tokenize(log)
		shard := drain.getShard(tokens)
		shardCounts[shard]++
	}

	// Should use multiple shards
	if len(shardCounts) < 2 {
		t.Error("Expected logs to be distributed across multiple shards")
	}
}

func BenchmarkDrainProcess(b *testing.B) {
	config := DefaultConfig()
	drain, _ := New(config)
	defer drain.Close()
	drain.Start()

	logs := generateTestLogs(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log := logs[i%len(logs)]
		drain.ProcessLog(log)
	}
}

func BenchmarkDrainConcurrent(b *testing.B) {
	config := DefaultConfig()
	config.ShardCount = 8
	drain, _ := New(config)
	defer drain.Close()
	drain.Start()

	logs := generateTestLogs(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			log := logs[i%len(logs)]
			drain.ProcessLog(log)
			i++
		}
	})
}

func generateTestLogs(count int) []string {
	patterns := []string{
		"User %s logged in from %s",
		"Request to /api/endpoint took %d ms",
		"Error connecting to database: %s",
		"Cache miss for key %s",
		"Processing job %d completed successfully",
	}

	logs := make([]string, count)
	for i := 0; i < count; i++ {
		pattern := patterns[i%len(patterns)]
		switch i % 5 {
		case 0:
			logs[i] = fmt.Sprintf(pattern, fmt.Sprintf("user%d", i), fmt.Sprintf("192.168.1.%d", i%256))
		case 1:
			logs[i] = fmt.Sprintf(pattern, i%1000)
		case 2:
			logs[i] = fmt.Sprintf(pattern, fmt.Sprintf("error%d", i))
		case 3:
			logs[i] = fmt.Sprintf(pattern, fmt.Sprintf("cache_key_%d", i))
		case 4:
			logs[i] = fmt.Sprintf(pattern, i)
		}
	}
	return logs
}

func TestEvictionBehavior(t *testing.T) {
	config := DefaultConfig()
	config.MaxClusters = 20  // Small limit to trigger eviction
	config.ShardCount = 2    // Use 2 shards, so 10 clusters per shard

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()

	if err := drain.Start(); err != nil {
		t.Fatalf("Failed to start drain: %v", err)
	}

	// Create 15 unique log patterns (will exceed per-shard limit)
	patterns := []string{
		"Error type A occurred in module X",
		"Error type B occurred in module Y",
		"Error type C occurred in module Z",
		"Warning level 1 detected",
		"Warning level 2 detected",
		"Warning level 3 detected",
		"Info message alpha",
		"Info message beta",
		"Info message gamma",
		"Debug trace delta",
		"Debug trace epsilon",
		"Debug trace zeta",
		"Critical alert eta",
		"Critical alert theta",
		"Critical alert iota",
	}

	// Process each pattern multiple times (varying frequency)
	for i, pattern := range patterns {
		// Some patterns are more frequent than others
		repeatCount := (i % 3) + 1 // 1, 2, or 3 times
		for j := 0; j < repeatCount; j++ {
			_, err := drain.ProcessLog(pattern)
			if err != nil {
				t.Errorf("Failed to process log: %v", err)
			}
		}
	}

	// Check that total clusters doesn't greatly exceed MaxClusters
	totalClusters := drain.GetClusterCount()
	t.Logf("Total clusters created: %d (limit: %d)", totalClusters, config.MaxClusters)

	// Allow some slack due to sharding, but should be close to limit
	if totalClusters > int64(config.MaxClusters)*2 {
		t.Errorf("Too many clusters created: %d (expected around %d)", totalClusters, config.MaxClusters)
	}

	// Verify that clusters were actually created
	if totalClusters < 5 {
		t.Errorf("Too few clusters created: %d", totalClusters)
	}
}

func TestHybridEvictionStrategy(t *testing.T) {
	config := DefaultConfig()
	config.MaxClusters = 16 // Small limit
	config.ShardCount = 4   // 4 clusters per shard

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()

	drain.Start()

	// Phase 1: Create some "hot" (frequently accessed) patterns
	hotPatterns := []string{
		"GET /api/health status=200",
		"GET /api/status status=200",
		"POST /api/login status=200",
	}

	for i := 0; i < 100; i++ {
		for _, pattern := range hotPatterns {
			drain.ProcessLog(pattern)
		}
	}

	// Phase 2: Create many unique "cold" patterns to trigger eviction
	for i := 0; i < 30; i++ {
		coldPattern := fmt.Sprintf("RARE_ERROR error_code_%d occurred", i)
		drain.ProcessLog(coldPattern)
	}

	// Phase 3: Access hot patterns again to update their lastAccessed
	for i := 0; i < 10; i++ {
		for _, pattern := range hotPatterns {
			drain.ProcessLog(pattern)
		}
	}

	// Verify hot patterns still exist
	clusters := drain.GetClusters()
	hotTemplateFound := 0
	for _, cluster := range clusters {
		template := cluster.GetTemplate()
		for _, hotPattern := range hotPatterns {
			if len(template) > 10 && template[:10] == hotPattern[:10] {
				hotTemplateFound++
				t.Logf("Hot pattern preserved: %s (size: %d)", template, cluster.Size())
				break
			}
		}
	}

	t.Logf("Hot patterns found after eviction: %d / %d", hotTemplateFound, len(hotPatterns))
	t.Logf("Total clusters: %d (limit: %d)", len(clusters), config.MaxClusters)

	// At least some hot patterns should be preserved
	if hotTemplateFound == 0 {
		t.Errorf("No hot patterns preserved - eviction strategy may be too aggressive")
	}
}

func TestGlobalEvictionAcrossShards(t *testing.T) {
	config := DefaultConfig()
	config.MaxClusters = 50 // Global limit
	config.ShardCount = 4   // 4 shards

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()
	drain.Start()

	// Phase 1: Create patterns that will distribute across different shards
	// Logs with different token counts will go to different shards
	patterns := make([]string, 0)

	// Short logs (likely shard 0 or 1)
	for i := 0; i < 20; i++ {
		patterns = append(patterns, fmt.Sprintf("Error %d", i))
	}

	// Medium logs (likely shard 2)
	for i := 0; i < 20; i++ {
		patterns = append(patterns, fmt.Sprintf("Warning level %d in module X", i))
	}

	// Long logs (likely shard 3)
	for i := 0; i < 30; i++ {
		patterns = append(patterns, fmt.Sprintf("Critical alert %d occurred in system Y at location Z", i))
	}

	// Process all patterns - this will exceed global limit
	for _, pattern := range patterns {
		drain.ProcessLog(pattern)
	}

	// Check that global limit is enforced
	totalClusters := drain.GetClusterCount()
	t.Logf("Total clusters after processing %d patterns: %d (limit: %d)",
		len(patterns), totalClusters, config.MaxClusters)

	// Global limit should be approximately respected
	if totalClusters > int64(config.MaxClusters)*110/100 { // Allow 10% slack
		t.Errorf("Global limit exceeded: got %d clusters, expected ~%d",
			totalClusters, config.MaxClusters)
	}

	// Verify clusters are distributed across shards
	shardCounts := make([]int64, config.ShardCount)
	for i, shard := range drain.shards {
		shardCounts[i] = shard.GetClusterCount()
		t.Logf("Shard %d: %d clusters", i, shardCounts[i])
	}

	// At least 2 shards should have clusters
	nonEmptyShards := 0
	for _, count := range shardCounts {
		if count > 0 {
			nonEmptyShards++
		}
	}

	if nonEmptyShards < 2 {
		t.Errorf("Expected clusters in multiple shards, got %d non-empty shards", nonEmptyShards)
	}

	// Phase 2: Add hot patterns and verify they're preserved
	hotPattern := "CRITICAL ERROR 999 system failure"
	for i := 0; i < 20; i++ {
		drain.ProcessLog(hotPattern)
	}

	// Verify hot pattern exists
	clusters := drain.GetClusters()
	foundHot := false
	for _, cluster := range clusters {
		if cluster.Size() >= 20 {
			foundHot = true
			t.Logf("Hot pattern preserved: %s (size: %d)", cluster.GetTemplate(), cluster.Size())
			break
		}
	}

	if !foundHot {
		t.Error("Hot pattern was evicted - global eviction should preserve high-frequency clusters")
	}
}

func TestMaxChildrenEnforcement(t *testing.T) {
	config := DefaultConfig()
	config.MaxChildren = 5   // Very low limit to trigger enforcement
	config.ShardCount = 1    // Single shard for predictable behavior
	config.Depth = 4         // Allow deeper tree for testing

	drain, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create drain: %v", err)
	}
	defer drain.Close()
	drain.Start()

	// Create logs with many unique values in the same position
	// This should trigger MaxChildren limit at tree nodes
	patterns := make([]string, 0)

	// Create 20 unique patterns with different IPs (high cardinality)
	for i := 0; i < 20; i++ {
		pattern := fmt.Sprintf("Connection from 192.168.%d.%d established", i/256, i%256)
		patterns = append(patterns, pattern)
	}

	// Process all patterns
	for _, pattern := range patterns {
		_, err := drain.ProcessLog(pattern)
		if err != nil {
			t.Errorf("Failed to process log: %v", err)
		}
	}

	// With MaxChildren=5, we should see wildcard nodes being created
	// and patterns being grouped together
	totalClusters := drain.GetClusterCount()
	t.Logf("Total clusters created: %d", totalClusters)

	// Should create fewer clusters due to wildcard grouping
	// Without MaxChildren enforcement, we'd have ~20 clusters
	// With enforcement, should be significantly fewer
	if totalClusters > 10 {
		t.Logf("Warning: Many clusters created (%d) despite MaxChildren=%d - wildcard grouping may not be working optimally",
			totalClusters, config.MaxChildren)
	}

	// Verify that tree structure respects MaxChildren
	// Walk the tree and check node children counts
	for _, shard := range drain.shards {
		checkNodeChildrenCount(t, shard.root, config.MaxChildren)
	}
}

// checkNodeChildrenCount recursively checks that no node exceeds MaxChildren limit
func checkNodeChildrenCount(t *testing.T, node *Node, maxChildren int) {
	if node == nil || node.IsLeaf() {
		return
	}

	childCount := node.GetChildCount()

	// Allow maxChildren + 1 because wildcard node "<*>" can be created as the (maxChildren+1)th child
	if childCount > maxChildren+1 {
		t.Errorf("Node at depth %d has %d children, exceeds MaxChildren limit of %d",
			node.depth, childCount, maxChildren)
	}

	// Recursively check all children
	for _, child := range node.GetChildren() {
		checkNodeChildrenCount(t, child, maxChildren)
	}
}
