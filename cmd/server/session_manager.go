package main

import (
	"encoding/json"
	"net"
	"sync"

	"github.com/cepwn/tcphashsubmit/internal/models"
)

type sessionManager struct {
	mu       sync.RWMutex
	sessions map[net.Conn]*models.Session
}

func newSessionManager() *sessionManager {
	return &sessionManager{
		sessions: make(map[net.Conn]*models.Session),
	}
}

func (sm *sessionManager) getSession(conn net.Conn) *models.Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[conn]
}

func (sm *sessionManager) setSession(conn net.Conn, session *models.Session) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sessions[conn] = session
}

func (sm *sessionManager) deleteSession(conn net.Conn) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, conn)
}

func (sm *sessionManager) broadcastTasks(jobID int, nonce string) []error {
	sm.mu.RLock()
	var targets []net.Conn
	for conn, session := range sm.sessions {
		if session.Authenticated {
			targets = append(targets, conn)
		}
	}
	sm.mu.RUnlock()

	var errs []error
	for _, conn := range targets {
		encoder := json.NewEncoder(conn)
		taskAssignment := &models.TaskAssignmentRequest{
			BaseRequest: models.BaseRequest{
				BasePayload: models.BasePayload{
					ID: nil,
				},
				Method: "job",
			},
			Params: models.TaskAssignmentRequestParams{
				JobID:       jobID,
				ServerNonce: nonce,
			},
		}
		err := encoder.Encode(taskAssignment)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
