package main

import "sync"

type clientState struct {
	mu                 sync.RWMutex
	currentJobID       int
	currentServerNonce string
	nextRequestID      int
}

func newClientState() *clientState {
	return &clientState{
		nextRequestID: 1,
	}
}

func (cs *clientState) getState() (int, string) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.currentJobID, cs.currentServerNonce
}

func (cs *clientState) setState(jobID int, nonce string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.currentJobID = jobID
	cs.currentServerNonce = nonce
}

func (cs *clientState) getNextRequestID() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	id := cs.nextRequestID
	cs.nextRequestID++
	return id
}
