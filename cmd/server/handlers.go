package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/models"
	"github.com/cepwn/tcphashsubmit/internal/util"
)

func (app *application) handleConnection(serverCtx context.Context, conn net.Conn) {
	defer conn.Close()

	connCtx, connCancel := context.WithCancel(serverCtx)
	defer connCancel()

	go func() {
		<-connCtx.Done()
		conn.SetDeadline(time.Now())
	}()

	remote := conn.RemoteAddr().String()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	session := &models.Session{
		SeenClientNonces: make(map[string]struct{}),
		JobHistory:       make(map[int]string),
	}
	app.sessionManager.setSession(conn, session)
	defer app.sessionManager.deleteSession(conn)
	for {
		rawRequest := &models.RawRequest{}
		err := decoder.Decode(rawRequest)
		if err != nil {
			if serverCtx.Err() != nil {
				app.logger.Debug("connection closed due to server shutdown", "remote", remote)
				return
			}
			if !errors.Is(err, io.EOF) {
				app.logger.Error("decode error", "remote", remote, "error", err)
			}
			app.logger.Debug("connection closed", "remote", remote)
			return
		}

		switch rawRequest.Method {
		case "authorize":
			app.handleAuthorizationRequest(rawRequest, session, encoder)
		case "submit":
			app.handleResultSubmissionRequest(rawRequest, session, encoder)
		default:
			app.logger.Error("unknown method", "remote", remote, "method", rawRequest.Method)
		}
	}
}

func (app *application) handleAuthorizationRequest(rawRequest *models.RawRequest, session *models.Session, encoder *json.Encoder) {
	params := &models.AuthenticationRequestParams{}
	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		app.logger.Error("invalid authorize params", "error", err)

		app.sendResponse(encoder, session, rawRequest.ID, false, "Internal server error")
		return
	}
	if params.Username == "" {
		app.logger.Error("authorize without username")
		app.sendResponse(encoder, session, rawRequest.ID, false, "Bad request")
		return
	}
	if session.Authenticated.Load() {
		app.logger.Error("authorize after already authenticated")
		app.sendResponse(encoder, session, rawRequest.ID, false, "Already authenticated")
		return
	}
	app.logger.Debug("authorize", "username", params.Username)

	session.Username = params.Username
	session.Authenticated.Store(true)

	app.sendResponse(encoder, session, rawRequest.ID, true, "")
}

func (app *application) handleResultSubmissionRequest(rawRequest *models.RawRequest, session *models.Session, encoder *json.Encoder) {
	if !session.Authenticated.Load() {
		app.logger.Error("submit without authentication")
		app.sendResponse(encoder, session, rawRequest.ID, false, "Not authenticated")
		return
	}

	if time.Since(session.LastSubmissionTime) < time.Second {
		app.logger.Error("rate limit exceeded", "username", session.Username)
		app.sendResponse(encoder, session, rawRequest.ID, false, "Submission too frequent")
		return
	}

	defer func() {
		session.LastSubmissionTime = time.Now()
	}()

	params := &models.ResultSubmissionRequestParams{}

	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		app.logger.Error("invalid submit params", "error", err)
		return
	}
	currentJobID, currentNonce := app.jobManager.getCurrentJobInfo()

	if params.JobID < currentJobID {
		app.logger.Error("expired job_id", "job_id", params.JobID, "current_job_id", currentJobID)
		app.sendResponse(encoder, session, rawRequest.ID, false, "Task expired")
		return
	}

	if params.JobID != currentJobID {
		app.logger.Error("invalid job_id", "job_id", params.JobID, "current_job_id", currentJobID)
		app.sendResponse(encoder, session, rawRequest.ID, false, "Task does not exist")
		return
	}

	if _, ok := session.SeenClientNonces[params.ClientNonce]; ok {
		app.logger.Error("duplicate client_nonce", "username", session.Username, "job_id", params.JobID)
		app.sendResponse(encoder, session, rawRequest.ID, false, "Duplicate submission")
		return
	}

	hash := util.ComputeSHA256(currentNonce + params.ClientNonce)

	if hash != params.Result {
		app.logger.Error("invalid result hash", "username", session.Username, "job_id", params.JobID)
		app.sendResponse(encoder, session, rawRequest.ID, false, "Invalid result")
		return
	}

	session.SeenClientNonces[params.ClientNonce] = struct{}{}
	app.submissionCh <- submissionEvent{
		Username:  session.Username,
		Timestamp: time.Now(),
	}
	app.logger.Debug("submit accepted", "username", session.Username, "job_id", params.JobID)
	app.sendResponse(encoder, session, rawRequest.ID, true, "")
}

func (app *application) sendResponse(encoder *json.Encoder, session *models.Session, id *int, success bool, errMsg string) {
	response := &models.Response{
		BasePayload: models.BasePayload{ID: id},
		Result:      success,
	}
	if errMsg != "" {
		response.Error = util.StrPtr(errMsg)
	}
	session.ConnMu.Lock()
	defer session.ConnMu.Unlock()
	if err := encoder.Encode(response); err != nil {
		app.logger.Error("failed to encode submit response", "error", err)
	}
}
