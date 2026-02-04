package main

import (
	"encoding/json"
	"errors"
	"io"
	"net"

	"github.com/cepwn/tcphashsubmit/internal/models"
	"github.com/cepwn/tcphashsubmit/internal/util"
)

func (app *application) handleConnection(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	session := &models.Session{}
	app.sessionManager.setSession(conn, session)
	defer app.sessionManager.deleteSession(conn)
	for {
		rawRequest := &models.RawRequest{}
		err := decoder.Decode(rawRequest)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				app.logger.Error(err.Error())
			}
			app.logger.Info("Connection closed by client")
			return
		}

		switch rawRequest.Method {
		case "authorize":
			app.handleAuthorizationRequest(rawRequest, session, encoder)
		case "submit":
			app.handleResultSubmissionRequest(rawRequest, session, encoder)
		default:
			app.logger.Error("Unknown method:", "method", rawRequest.Method)
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
		app.logger.Error("Received authentication request without username")
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
		app.logger.Error("Received authentication request after authentication")
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
	app.logger.Info("Received authentication request:", "username", params.Username)

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
		app.logger.Error("Received result submission request without authentication")
		err := encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("not authenticated"),
		})
		if err != nil {
			app.logger.Error(err.Error())
		}
		return
	}
	params := &models.ResultSubmissionRequestParams{}

	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		app.logger.Error(err.Error())
		return
	}
	app.logger.Info("Received result submission request:", "job_id", params.JobID)
	err = encoder.Encode(&models.Response{
		BasePayload: models.BasePayload{
			ID: rawRequest.ID,
		},
		Result: true,
	})
	if err != nil {
		app.logger.Error(err.Error())
	}
}
