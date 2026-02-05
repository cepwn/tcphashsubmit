package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/models"
	"github.com/cepwn/tcphashsubmit/internal/util"
)

func newTestApp() *application {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	sm := newSessionManager()
	jm := newJobManager()
	ch := make(chan submissionEvent, 100)

	return &application{
		logger:         logger,
		sessionManager: sm,
		jobManager:     jm,
		submissionCh:   ch,
	}
}

func newTestSession(username string) *models.Session {
	return &models.Session{
		Username:         username,
		Authenticated:    true,
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
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
			expectedError:  "bad request",
		},
		{
			name:           "already authenticated",
			username:       "alice",
			authenticated:  true,
			expectedResult: false,
			expectedError:  "already authenticated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newTestApp()
			session := &models.Session{
				Authenticated:    tt.authenticated,
				SeenClientNonces: make(map[string]struct{}),
				JobHistory:       make(map[int]string),
			}

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
			if tt.expectedResult && !session.Authenticated {
				t.Error("expected session to be authenticated")
			}
			if tt.expectedResult && session.Username != tt.username {
				t.Errorf("expected username %q, got %q", tt.username, session.Username)
			}
		})
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
				session.Authenticated = false
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
			app := newTestApp()
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
	app := newTestApp()
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

	// Check that a submission event was sent to the channel
	select {
	case event := <-app.submissionCh:
		if event.Username != "alice" {
			t.Errorf("expected username 'alice', got %q", event.Username)
		}
	default:
		t.Error("expected submission event on channel, got none")
	}
}
