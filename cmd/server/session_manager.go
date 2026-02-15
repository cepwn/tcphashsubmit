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

type target struct {
	conn    net.Conn
	session *models.Session
}

func (sm *sessionManager) broadcastTasks(jobID int, nonce string) []error {
	sm.mu.RLock()
	var targets []target
	for conn, session := range sm.sessions {
		if session.Authenticated.Load() {
			targets = append(targets, target{conn, session})
		}
	}
	sm.mu.RUnlock()

	var errs []error
	for _, target := range targets {
		encoder := json.NewEncoder(target.conn)
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
		target.session.ConnMu.Lock()
		err := encoder.Encode(taskAssignment)
		if err != nil {
			errs = append(errs, err)
		}
		target.session.ConnMu.Unlock()
		if err == nil {
			target.session.DataMu.Lock()
			target.session.JobHistory[jobID] = nonce
			target.session.DataMu.Unlock()
		}
	}
	return errs
}
