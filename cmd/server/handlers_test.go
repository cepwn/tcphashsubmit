package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/models"
	"github.com/cepwn/tcphashsubmit/internal/queue"
	"github.com/cepwn/tcphashsubmit/internal/util"
)

type mockSubmissionPublisher struct {
	mu         sync.Mutex
	published  []queue.SubmissionEvent
	publishErr error
}

func (m *mockSubmissionPublisher) Publish(event queue.SubmissionEvent) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, event)
	return nil
}

func (m *mockSubmissionPublisher) Close() error {
	return nil
}

func newTestApp() (*application, *mockSubmissionPublisher) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	sm := newSessionManager()
	jm := newJobManager()
	mock := &mockSubmissionPublisher{}

	return &application{
		logger:              logger,
		sessionManager:      sm,
		jobManager:          jm,
		submissionPublisher: mock,
	}, mock
}

func newTestSession(username string) *models.Session {
	s := &models.Session{
		Username:         username,
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	s.Authenticated.Store(true)
	return s
}

func makeRawRequest(id int, method string, params interface{}) *models.RawRequest {
	paramsBytes, _ := json.Marshal(params)
	return &models.RawRequest{
		BaseRequest: models.BaseRequest{
			BasePayload: models.BasePayload{
				ID: &id,
			},
			Method: method,
		},
		Params: paramsBytes,
	}
}

func decodeResponse(t *testing.T, buf *bytes.Buffer) *models.Response {
	t.Helper()
	var resp models.Response
	err := json.NewDecoder(buf).Decode(&resp)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	return &resp
}

func TestHandleAuthorizationRequest(t *testing.T) {
	tests := []struct {
		name           string
		username       string
		authenticated  bool
		expectedResult bool
		expectedError  string
	}{
		{
			name:           "successful authentication",
			username:       "alice",
			authenticated:  false,
			expectedResult: true,
			expectedError:  "",
		},
		{
			name:           "empty username",
			username:       "",
			authenticated:  false,
			expectedResult: false,
			expectedError:  "Bad request",
		},
		{
			name:           "already authenticated",
			username:       "alice",
			authenticated:  true,
			expectedResult: false,
			expectedError:  "Already authenticated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, _ := newTestApp()
			session := &models.Session{
				SeenClientNonces: make(map[string]struct{}),
				JobHistory:       make(map[int]string),
			}
			session.Authenticated.Store(tt.authenticated)

			rawRequest := makeRawRequest(1, "authorize", models.AuthenticationRequestParams{
				Username: tt.username,
			})

			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)

			app.handleAuthorizationRequest(rawRequest, session, encoder)

			resp := decodeResponse(t, &buf)

			if resp.Result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, resp.Result)
			}
			if tt.expectedError != "" {
				if resp.Error == nil || *resp.Error != tt.expectedError {
					t.Errorf("expected error %q, got %v", tt.expectedError, resp.Error)
				}
			}
			if tt.expectedResult && !session.Authenticated.Load() {
				t.Error("expected session to be authenticated")
			}
			if tt.expectedResult && session.Username != tt.username {
				t.Errorf("expected username %q, got %q", tt.username, session.Username)
			}
		})
	}
}

func TestHandleAuthorizationRequest_BadJSON(t *testing.T) {
	app, _ := newTestApp()
	session := &models.Session{
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}

	id := 1
	rawRequest := &models.RawRequest{
		BaseRequest: models.BaseRequest{
			BasePayload: models.BasePayload{ID: &id},
			Method:      "authorize",
		},
		Params: json.RawMessage(`{invalid`),
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	app.handleAuthorizationRequest(rawRequest, session, encoder)

	resp := decodeResponse(t, &buf)
	if resp.Result {
		t.Error("expected failure on bad JSON params")
	}
	if resp.Error == nil || *resp.Error != "Internal server error" {
		t.Errorf("expected 'Internal server error', got %v", resp.Error)
	}
}

func TestHandleResultSubmissionRequest(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(app *application, session *models.Session) (int, string, string)
		expectedResult bool
		expectedError  string
	}{
		{
			name: "successful submission",
			setup: func(app *application, session *models.Session) (int, string, string) {
				app.jobManager.mu.Lock()
				app.jobManager.currentJobID = 1
				app.jobManager.currentServerNonce = "servernonce123"
				app.jobManager.mu.Unlock()

				clientNonce := "clientnonce456"
				hash := util.ComputeSHA256("servernonce123" + clientNonce)
				return 1, clientNonce, hash
			},
			expectedResult: true,
			expectedError:  "",
		},
		{
			name: "not authenticated",
			setup: func(app *application, session *models.Session) (int, string, string) {
				session.Authenticated.Store(false)
				return 1, "nonce", "hash"
			},
			expectedResult: false,
			expectedError:  "Not authenticated",
		},
		{
			name: "invalid job id - does not exist",
			setup: func(app *application, session *models.Session) (int, string, string) {
				app.jobManager.mu.Lock()
				app.jobManager.currentJobID = 1
				app.jobManager.currentServerNonce = "nonce"
				app.jobManager.mu.Unlock()
				return 99, "clientnonce", "hash"
			},
			expectedResult: false,
			expectedError:  "Task does not exist",
		},
		{
			name: "expired job id",
			setup: func(app *application, session *models.Session) (int, string, string) {
				app.jobManager.mu.Lock()
				app.jobManager.currentJobID = 5
				app.jobManager.currentServerNonce = "nonce"
				app.jobManager.mu.Unlock()
				session.JobHistory[3] = "oldnonce"
				return 3, "clientnonce", "hash"
			},
			expectedResult: false,
			expectedError:  "Task expired",
		},
		{
			name: "duplicate nonce",
			setup: func(app *application, session *models.Session) (int, string, string) {
				app.jobManager.mu.Lock()
				app.jobManager.currentJobID = 1
				app.jobManager.currentServerNonce = "servernonce"
				app.jobManager.mu.Unlock()

				session.SeenClientNonces["duplicate"] = struct{}{}
				clientNonce := "duplicate"
				hash := util.ComputeSHA256("servernonce" + clientNonce)
				return 1, clientNonce, hash
			},
			expectedResult: false,
			expectedError:  "Duplicate submission",
		},
		{
			name: "invalid hash",
			setup: func(app *application, session *models.Session) (int, string, string) {
				app.jobManager.mu.Lock()
				app.jobManager.currentJobID = 1
				app.jobManager.currentServerNonce = "servernonce"
				app.jobManager.mu.Unlock()
				return 1, "clientnonce", "badhash"
			},
			expectedResult: false,
			expectedError:  "Invalid result",
		},
		{
			name: "rate limit exceeded",
			setup: func(app *application, session *models.Session) (int, string, string) {
				app.jobManager.mu.Lock()
				app.jobManager.currentJobID = 1
				app.jobManager.currentServerNonce = "servernonce"
				app.jobManager.mu.Unlock()

				session.LastSubmissionTime = time.Now() // Just submitted

				clientNonce := "clientnonce"
				hash := util.ComputeSHA256("servernonce" + clientNonce)
				return 1, clientNonce, hash
			},
			expectedResult: false,
			expectedError:  "Submission too frequent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, _ := newTestApp()
			session := newTestSession("testuser")

			jobID, clientNonce, hash := tt.setup(app, session)

			rawRequest := makeRawRequest(1, "submit", models.ResultSubmissionRequestParams{
				JobID:       jobID,
				ClientNonce: clientNonce,
				Result:      hash,
			})

			var buf bytes.Buffer
			encoder := json.NewEncoder(&buf)

			app.handleResultSubmissionRequest(rawRequest, session, encoder)

			resp := decodeResponse(t, &buf)

			if resp.Result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, resp.Result)
			}
			if tt.expectedError != "" {
				if resp.Error == nil || *resp.Error != tt.expectedError {
					t.Errorf("expected error %q, got %v", tt.expectedError, resp.Error)
				}
			}
		})
	}
}

func TestHandleResultSubmissionRequest_RecordsEvent(t *testing.T) {
	app, mock := newTestApp()
	session := newTestSession("alice")

	app.jobManager.mu.Lock()
	app.jobManager.currentJobID = 1
	app.jobManager.currentServerNonce = "testnonce"
	app.jobManager.mu.Unlock()

	clientNonce := "uniquenonce123"
	hash := util.ComputeSHA256("testnonce" + clientNonce)

	rawRequest := makeRawRequest(1, "submit", models.ResultSubmissionRequestParams{
		JobID:       1,
		ClientNonce: clientNonce,
		Result:      hash,
	})

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	app.handleResultSubmissionRequest(rawRequest, session, encoder)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(mock.published))
	}
	if mock.published[0].Username != "alice" {
		t.Errorf("expected username 'alice', got %q", mock.published[0].Username)
	}
}

func TestHandleResultSubmissionRequest_PublishFailure(t *testing.T) {
	app, mock := newTestApp()
	mock.publishErr = errors.New("publish failed")

	session := newTestSession("testuser")

	app.jobManager.mu.Lock()
	app.jobManager.currentJobID = 1
	app.jobManager.currentServerNonce = "servernonce"
	app.jobManager.mu.Unlock()

	clientNonce := "clientnonce"
	hash := util.ComputeSHA256("servernonce" + clientNonce)

	rawRequest := makeRawRequest(1, "submit", models.ResultSubmissionRequestParams{
		JobID:       1,
		ClientNonce: clientNonce,
		Result:      hash,
	})

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	app.handleResultSubmissionRequest(rawRequest, session, encoder)

	resp := decodeResponse(t, &buf)
	if resp.Result {
		t.Error("expected failure on publish error")
	}
	if resp.Error == nil || *resp.Error != "Internal server error" {
		t.Errorf("expected 'Internal server error', got %v", resp.Error)
	}
}

func TestHandleResultSubmissionRequest_RateLimitUpdatesTime(t *testing.T) {
	app, _ := newTestApp()
	session := newTestSession("testuser")

	app.jobManager.mu.Lock()
	app.jobManager.currentJobID = 1
	app.jobManager.currentServerNonce = "servernonce"
	app.jobManager.mu.Unlock()

	before := time.Now()

	rawRequest := makeRawRequest(1, "submit", models.ResultSubmissionRequestParams{
		JobID:       1,
		ClientNonce: "clientnonce",
		Result:      "badhash",
	})

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	app.handleResultSubmissionRequest(rawRequest, session, encoder)

	resp := decodeResponse(t, &buf)
	if resp.Result {
		t.Error("expected failure for bad hash")
	}

	if session.LastSubmissionTime.Before(before) {
		t.Error("expected LastSubmissionTime to be updated even on failed validation")
	}
}

func TestHandleConnection_UnknownMethod(t *testing.T) {
	app, _ := newTestApp()

	serverConn, clientConn := net.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		app.handleConnection(ctx, serverConn)
	}()

	encoder := json.NewEncoder(clientConn)
	id := 1
	err := encoder.Encode(&models.RawRequest{
		BaseRequest: models.BaseRequest{
			BasePayload: models.BasePayload{ID: &id},
			Method:      "unknown",
		},
	})
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}

	clientConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConnection did not exit after unknown method and connection close")
	}
}
