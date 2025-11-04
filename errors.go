package drain

import "errors"

var (
	ErrInvalidDepth        = errors.New("depth must be at least 2")
	ErrInvalidSimThreshold = errors.New("similarity threshold must be between 0 and 1")
	ErrInvalidMaxChildren  = errors.New("max children must be at least 1")
	ErrInvalidMaxClusters  = errors.New("max clusters must be at least 1")
	ErrInvalidShardCount   = errors.New("shard count must be a power of 2")
	ErrDrainClosed         = errors.New("drain instance is closed")
	ErrProcessingFailed    = errors.New("log processing failed")
)
