package models

import (
	"sync"
	"sync/atomic"
	"time"
)

type Session struct {
	ConnMu            sync.Mutex
	Username          string
	Authenticated     atomic.Bool
	LatestJobID       int
	LatestServerNonce string
	// SeenClientNonces tracks all client nonces across jobs to detect duplicates.
	// Note: this grows unbounded for long-lived sessions. For production use,
	// consider LRU eviction or periodic pruning.
	SeenClientNonces   map[string]struct{}
	JobHistory         map[int]string
	LastSubmissionTime time.Time
}
