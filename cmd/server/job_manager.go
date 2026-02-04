package main

import (
	"sync"

	"github.com/cepwn/tcphashsubmit/internal/util"
)

type jobManager struct {
	mu                 sync.RWMutex
	currentJobID       int
	currentServerNonce string
}

func newJobManager() *jobManager {
	return &jobManager{}
}

func (jm *jobManager) getCurrentJobInfo() (int, string) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()
	return jm.currentJobID, jm.currentServerNonce
}

func (jm *jobManager) generateJob() error {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	newServerNonce, err := util.GenerateNonce()
	if err != nil {
		return err
	}
	jm.currentJobID++
	jm.currentServerNonce = newServerNonce
	return nil
}
