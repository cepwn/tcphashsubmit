package main

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/cepwn/tcphashsubmit/internal/models"
)

type mockConn struct {
	net.Conn
	id int
}

func newMockConn(id int) *mockConn {
	return &mockConn{id: id}
}

type writableMockConn struct {
	mockConn
	buf      bytes.Buffer
	writeErr error
}

func (c *writableMockConn) Write(p []byte) (n int, err error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return c.buf.Write(p)
}

func TestSessionManager_SetAndGet(t *testing.T) {
	sm := newSessionManager()
	conn := newMockConn(1)
	session := &models.Session{
		Username:         "alice",
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	session.Authenticated.Store(true)

	sm.setSession(conn, session)

	got := sm.getSession(conn)
	if got == nil {
		t.Fatal("expected session, got nil")
	}
	if got.Username != "alice" {
		t.Errorf("expected username 'alice', got %q", got.Username)
	}
}

func TestSessionManager_Delete(t *testing.T) {
	sm := newSessionManager()
	conn := newMockConn(1)
	session := &models.Session{
		Username:         "alice",
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}

	sm.setSession(conn, session)
	sm.deleteSession(conn)

	got := sm.getSession(conn)
	if got != nil {
		t.Error("expected nil session after delete")
	}
}

func TestSessionManager_ConcurrentAccess(t *testing.T) {
	sm := newSessionManager()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := newMockConn(id)
			session := &models.Session{
				Username:         "user",
				SeenClientNonces: make(map[string]struct{}),
				JobHistory:       make(map[int]string),
			}
			session.Authenticated.Store(true)
			sm.setSession(conn, session)
			sm.getSession(conn)
			sm.deleteSession(conn)
		}(i)
	}

	wg.Wait()
}

func TestBroadcastTasks_OnlyAuthenticated(t *testing.T) {
	sm := newSessionManager()

	authConn := &writableMockConn{mockConn: mockConn{id: 1}}
	authSession := &models.Session{
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	authSession.Authenticated.Store(true)
	sm.setSession(authConn, authSession)

	unauthConn := &writableMockConn{mockConn: mockConn{id: 2}}
	unauthSession := &models.Session{
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	sm.setSession(unauthConn, unauthSession)

	errs := sm.broadcastTasks(1, "testnonce")
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if authConn.buf.Len() == 0 {
		t.Error("expected authenticated session to receive broadcast")
	}
	if unauthConn.buf.Len() != 0 {
		t.Error("expected unauthenticated session to NOT receive broadcast")
	}
}

func TestBroadcastTasks_RecordsJobHistory(t *testing.T) {
	sm := newSessionManager()

	conn := &writableMockConn{mockConn: mockConn{id: 1}}
	session := &models.Session{
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	session.Authenticated.Store(true)
	sm.setSession(conn, session)

	errs := sm.broadcastTasks(42, "testnonce")
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	nonce, ok := session.JobHistory[42]
	if !ok {
		t.Fatal("expected job history entry for job ID 42")
	}
	if nonce != "testnonce" {
		t.Errorf("expected nonce 'testnonce', got %q", nonce)
	}
}

func TestBroadcastTasks_WriteErrors(t *testing.T) {
	sm := newSessionManager()

	conn := &writableMockConn{
		mockConn: mockConn{id: 1},
		writeErr: errors.New("write failed"),
	}
	session := &models.Session{
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	session.Authenticated.Store(true)
	sm.setSession(conn, session)

	errs := sm.broadcastTasks(1, "nonce")
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if _, ok := session.JobHistory[1]; ok {
		t.Error("job history should not be updated on write error")
	}
}
