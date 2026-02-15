package models

import (
	"sync"
	"sync/atomic"
	"time"
)

type Session struct {
	ConnMu             sync.Mutex
	Username           string
	Authenticated      atomic.Bool
	LatestJobID        int
	LatestServerNonce  string
	SeenClientNonces   map[string]struct{}
	JobHistory         map[int]string
	LastSubmissionTime time.Time
}
