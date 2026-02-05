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
				app.logger.Error(err.Error())
			}
			app.logger.Info("connection closed by client")
			return
		}

		switch rawRequest.Method {
		case "authorize":
			app.handleAuthorizationRequest(rawRequest, session, encoder)
		case "submit":
			app.handleResultSubmissionRequest(rawRequest, session, encoder)
		default:
			app.logger.Error("unknown method:", "method", rawRequest.Method)
		}
	}
}

func (app *application) handleAuthorizationRequest(rawRequest *models.RawRequest, session *models.Session, encoder *json.Encoder) {
	params := &models.AuthenticationRequestParams{}
	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		app.logger.Error(err.Error())

		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("internal server error"),
		})
		if err != nil {
			app.logger.Error(err.Error())
		}
		return
	}
	if params.Username == "" {
		app.logger.Error("received authentication request without username")
		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("bad request"),
		})
		if err != nil {
			app.logger.Error(err.Error())
		}
		return
	}
	if session.Authenticated {
		app.logger.Error("received authentication request after authentication")
		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("already authenticated"),
		})
		if err != nil {
			app.logger.Error(err.Error())
		}
		return
	}
	app.logger.Info("received authentication request:", "username", params.Username)

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
		app.logger.Error(err.Error())
	}
}

func (app *application) handleResultSubmissionRequest(rawRequest *models.RawRequest, session *models.Session, encoder *json.Encoder) {
	if !session.Authenticated {
		app.logger.Error("received result submission request without authentication")
		err := encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("Not authenticated"),
		})
		if err != nil {
			app.logger.Error(err.Error())
		}
		return
	}

	defer func() {
		session.LastSubmissionTime = time.Now()
	}()

	if time.Since(session.LastSubmissionTime) < time.Second {
		app.logger.Error("rate limit exceeded")
		app.sendResponse(encoder, rawRequest.ID, false, "Submission too frequent")
		return
	}

	params := &models.ResultSubmissionRequestParams{}

	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		app.logger.Error(err.Error())
		return
	}
	currentJobID, currentNonce := app.jobManager.getCurrentJobInfo()

	if params.JobID < currentJobID {
		app.logger.Error("received expired job id", "job_id", params.JobID)
		app.sendResponse(encoder, rawRequest.ID, false, "Task expired")
		return
	}

	if params.JobID != currentJobID {
		app.logger.Error("received invalid job id", "job_id", params.JobID)
		app.sendResponse(encoder, rawRequest.ID, false, "Task does not exist")
		return
	}

	if _, ok := session.SeenClientNonces[params.ClientNonce]; ok {
		app.logger.Error("received duplicate client nonce", "nonce", params.ClientNonce)
		app.sendResponse(encoder, rawRequest.ID, false, "Duplicate submission")
		return
	}

	hash := util.ComputeSHA256(currentNonce + params.ClientNonce)

	if hash != params.Result {
		app.logger.Error("received invalid result hash", "result", params.Result)
		app.sendResponse(encoder, rawRequest.ID, false, "Invalid result")
		return
	}

	session.SeenClientNonces[params.ClientNonce] = struct{}{}
	app.logger.Info("received result submission request:", "job_id", params.JobID)
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
		app.logger.Error("failed to encode response", "error", err)
	}
}
