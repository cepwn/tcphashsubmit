package models

import (
	"sync"
	"time"
)

type Session struct {
	ConnMu             sync.Mutex
	Username           string
	Authenticated      bool
	LatestJobID        int
	LatestServerNonce  string
	SeenClientNonces   map[string]struct{}
	JobHistory         map[int]string
	LastSubmissionTime time.Time
}
