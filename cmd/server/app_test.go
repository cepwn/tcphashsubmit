package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/models"
)

func TestGenerateAndBroadcastJob(t *testing.T) {
	app, _ := newTestApp()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	session := &models.Session{
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	session.Authenticated.Store(true)
	app.sessionManager.setSession(serverConn, session)

	var received bytes.Buffer
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		io.Copy(&received, clientConn)
	}()

	app.generateAndBroadcastJob()

	jobID, nonce := app.jobManager.getCurrentJobInfo()
	if jobID != 1 {
		t.Errorf("expected job ID 1, got %d", jobID)
	}
	if nonce == "" {
		t.Error("expected non-empty nonce")
	}

	// Close server conn to signal EOF to the reader goroutine
	serverConn.Close()
	<-readDone

	if received.Len() == 0 {
		t.Fatal("expected task broadcast to authenticated session")
	}

	var task models.TaskAssignmentRequest
	if err := json.NewDecoder(&received).Decode(&task); err != nil {
		t.Fatalf("failed to decode broadcast: %v", err)
	}
	if task.Params.JobID != jobID {
		t.Errorf("expected job ID %d, got %d", jobID, task.Params.JobID)
	}
	if task.Params.ServerNonce != nonce {
		t.Errorf("expected nonce %q, got %q", nonce, task.Params.ServerNonce)
	}
}

func TestRunJobTicker_StopsOnCancel(t *testing.T) {
	app, _ := newTestApp()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.runJobTicker(ctx)
	}()

	// Give time for initial job generation
	time.Sleep(50 * time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runJobTicker did not stop on context cancel")
	}

	jobID, _ := app.jobManager.getCurrentJobInfo()
	if jobID < 1 {
		t.Error("expected at least one job to be generated")
	}
}
