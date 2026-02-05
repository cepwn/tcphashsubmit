package main

import (
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

func TestSessionManager_SetAndGet(t *testing.T) {
	sm := newSessionManager()
	conn := newMockConn(1)
	session := &models.Session{
		Username:         "alice",
		Authenticated:    true,
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}

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
				Authenticated:    true,
				SeenClientNonces: make(map[string]struct{}),
				JobHistory:       make(map[int]string),
			}
			sm.setSession(conn, session)
			sm.getSession(conn)
			sm.deleteSession(conn)
		}(i)
	}

	wg.Wait()
}
