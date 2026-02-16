package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/queue"
	"github.com/cepwn/tcphashsubmit/internal/store"
)

type mockSubmissionStore struct {
	mu        sync.Mutex
	recorded  []recordedSubmission
	recordErr error
}

type recordedSubmission struct {
	Username  string
	Timestamp time.Time
}

func (m *mockSubmissionStore) RecordSubmission(username string, timestamp time.Time) error {
	if m.recordErr != nil {
		return m.recordErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recorded = append(m.recorded, recordedSubmission{Username: username, Timestamp: timestamp})
	return nil
}

func (m *mockSubmissionStore) GetSubmissions(username string) ([]store.Submission, error) {
	return nil, nil
}

type mockSubmissionConsumer struct {
	deliveries chan queue.Delivery
	closeErr   error
	closed     bool
}

func (m *mockSubmissionConsumer) Subscribe() (<-chan queue.Delivery, error) {
	return m.deliveries, nil
}

func (m *mockSubmissionConsumer) Close() error {
	m.closed = true
	return m.closeErr
}

func newTestConsumerApp(s *mockSubmissionStore, c *mockSubmissionConsumer) *application {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return &application{
		logger:             logger,
		submissionStore:    s,
		submissionConsumer: c,
	}
}

func TestProcessSubmissions_HappyPath(t *testing.T) {
	mockStore := &mockSubmissionStore{}
	deliveries := make(chan queue.Delivery, 1)
	mockConsumer := &mockSubmissionConsumer{deliveries: deliveries}
	app := newTestConsumerApp(mockStore, mockConsumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.processSubmissions(ctx)
	}()

	timestamp := time.Now()
	event := queue.SubmissionEvent{Username: "alice", Timestamp: timestamp}
	body, _ := json.Marshal(event)

	acked := make(chan bool, 1)
	deliveries <- queue.Delivery{
		Body: body,
		Ack: func(multiple bool) error {
			acked <- multiple
			return nil
		},
	}

	select {
	case multiple := <-acked:
		if multiple {
			t.Error("expected ack with multiple=false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delivery was not acked")
	}

	mockStore.mu.Lock()
	if len(mockStore.recorded) != 1 {
		t.Fatalf("expected 1 recorded submission, got %d", len(mockStore.recorded))
	}
	if mockStore.recorded[0].Username != "alice" {
		t.Errorf("expected username 'alice', got %q", mockStore.recorded[0].Username)
	}
	mockStore.mu.Unlock()

	cancel()
	<-done
}

func TestProcessSubmissions_BadJSON(t *testing.T) {
	mockStore := &mockSubmissionStore{}
	deliveries := make(chan queue.Delivery, 1)
	mockConsumer := &mockSubmissionConsumer{deliveries: deliveries}
	app := newTestConsumerApp(mockStore, mockConsumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.processSubmissions(ctx)
	}()

	acked := make(chan bool, 1)
	deliveries <- queue.Delivery{
		Body: []byte(`{bad json`),
		Ack: func(multiple bool) error {
			acked <- multiple
			return nil
		},
	}

	select {
	case <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery was not acked")
	}

	mockStore.mu.Lock()
	if len(mockStore.recorded) != 0 {
		t.Errorf("expected 0 recorded submissions, got %d", len(mockStore.recorded))
	}
	mockStore.mu.Unlock()

	cancel()
	<-done
}

func TestProcessSubmissions_StoreError(t *testing.T) {
	mockStore := &mockSubmissionStore{recordErr: errors.New("db error")}
	deliveries := make(chan queue.Delivery, 1)
	mockConsumer := &mockSubmissionConsumer{deliveries: deliveries}
	app := newTestConsumerApp(mockStore, mockConsumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.processSubmissions(ctx)
	}()

	event := queue.SubmissionEvent{Username: "alice", Timestamp: time.Now()}
	body, _ := json.Marshal(event)

	acked := make(chan bool, 1)
	deliveries <- queue.Delivery{
		Body: body,
		Ack: func(multiple bool) error {
			acked <- multiple
			return nil
		},
	}

	select {
	case <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery was not acked")
	}

	cancel()
	<-done
}

func TestProcessSubmissions_ContextCancel(t *testing.T) {
	deliveries := make(chan queue.Delivery)
	mockConsumer := &mockSubmissionConsumer{deliveries: deliveries}
	app := newTestConsumerApp(&mockSubmissionStore{}, mockConsumer)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.processSubmissions(ctx)
	}()

	cancel()

	select {
	case <-done:
		if !mockConsumer.closed {
			t.Error("expected consumer to be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("processSubmissions did not exit on context cancel")
	}
}
