package main

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/models"
	"github.com/cepwn/tcphashsubmit/internal/util"
)

func (app *application) handleConnection(conn net.Conn) {
	defer conn.Close()
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

		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("internal server error"),
		})
		if err != nil {
			app.logger.Error("failed to encode response", "error", err)
		}
		return
	}
	if params.Username == "" {
		app.logger.Error("authorize without username")
		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("bad request"),
		})
		if err != nil {
			app.logger.Error("failed to encode response", "error", err)
		}
		return
	}
	if session.Authenticated {
		app.logger.Error("authorize after already authenticated")
		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("already authenticated"),
		})
		if err != nil {
			app.logger.Error("failed to encode response", "error", err)
		}
		return
	}
	app.logger.Debug("authorize", "username", params.Username)

	session.Username = params.Username
	session.Authenticated = true

	authorizationResponse := &models.Response{
		BasePayload: models.BasePayload{
			ID: rawRequest.ID,
		},
		Result: true,
	}
	err = encoder.Encode(authorizationResponse)
	if err != nil {
		app.logger.Error("failed to encode authorize response", "error", err)
	}
}

func (app *application) handleResultSubmissionRequest(rawRequest *models.RawRequest, session *models.Session, encoder *json.Encoder) {
	if !session.Authenticated {
		app.logger.Error("submit without authentication")
		err := encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("Not authenticated"),
		})
		if err != nil {
			app.logger.Error("failed to encode response", "error", err)
		}
		return
	}

	defer func() {
		session.LastSubmissionTime = time.Now()
	}()

	if time.Since(session.LastSubmissionTime) < time.Second {
		app.logger.Error("rate limit exceeded", "username", session.Username)
		app.sendResponse(encoder, rawRequest.ID, false, "Submission too frequent")
		return
	}

	params := &models.ResultSubmissionRequestParams{}

	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		app.logger.Error("invalid submit params", "error", err)
		return
	}
	currentJobID, currentNonce := app.jobManager.getCurrentJobInfo()

	if params.JobID < currentJobID {
		app.logger.Error("expired job_id", "job_id", params.JobID, "current_job_id", currentJobID)
		app.sendResponse(encoder, rawRequest.ID, false, "Task expired")
		return
	}

	if params.JobID != currentJobID {
		app.logger.Error("invalid job_id", "job_id", params.JobID, "current_job_id", currentJobID)
		app.sendResponse(encoder, rawRequest.ID, false, "Task does not exist")
		return
	}

	if _, ok := session.SeenClientNonces[params.ClientNonce]; ok {
		app.logger.Error("duplicate client_nonce", "username", session.Username, "job_id", params.JobID)
		app.sendResponse(encoder, rawRequest.ID, false, "Duplicate submission")
		return
	}

	hash := util.ComputeSHA256(currentNonce + params.ClientNonce)

	if hash != params.Result {
		app.logger.Error("invalid result hash", "username", session.Username, "job_id", params.JobID)
		app.sendResponse(encoder, rawRequest.ID, false, "Invalid result")
		return
	}

	session.SeenClientNonces[params.ClientNonce] = struct{}{}
	app.submissionCh <- submissionEvent{
		Username:  session.Username,
		Timestamp: time.Now(),
	}
	app.logger.Debug("submit accepted", "username", session.Username, "job_id", params.JobID)
	app.sendResponse(encoder, rawRequest.ID, true, "")
}

func (app *application) sendResponse(encoder *json.Encoder, id *int, success bool, errMsg string) {
	response := &models.Response{
		BasePayload: models.BasePayload{ID: id},
		Result:      success,
	}
	if errMsg != "" {
		response.Error = util.StrPtr(errMsg)
	}
	if err := encoder.Encode(response); err != nil {
		app.logger.Error("failed to encode submit response", "error", err)
	}
}
