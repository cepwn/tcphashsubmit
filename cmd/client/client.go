package main

import (
	"encoding/json"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/models"
	"github.com/cepwn/tcphashsubmit/internal/util"
)

func (app *application) listenForMessages() {
	for {
		messageEnvelope := &models.MessageEnvelope{}
		err := app.decoder.Decode(messageEnvelope)
		if err != nil {
			app.logger.Error("decode error", "error", err)
			return
		}
		app.logger.Debug("message received", "id", messageEnvelope.ID)

		if messageEnvelope.ID == nil {
			app.handleTaskAssignmentRequest(messageEnvelope)
		} else {
			app.handleResponse(messageEnvelope)
		}
	}
}

func (app *application) submitResults() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		jobID, serverNonce := app.clientState.getState()

		if jobID == 0 {
			continue
		}

		clientNonce, err := util.GenerateNonce()
		if err != nil {
			app.logger.Error("failed to generate nonce", "error", err)
			continue
		}
		hash := util.ComputeSHA256(serverNonce + clientNonce)
		err = app.sendResultSubmissionRequest(jobID, clientNonce, hash)
		if err != nil {
			app.logger.Error("error sending result submission request:", "error", err)
			return
		}

	}
}

func (app *application) handleResponse(messageEnvelope *models.MessageEnvelope) {
	if messageEnvelope.Result != nil && *messageEnvelope.Result {
		app.logger.Debug("request succeeded", "id", messageEnvelope.ID)
	} else {
		errMsg := "unknown error"
		if messageEnvelope.Error != nil {
			errMsg = *messageEnvelope.Error
		}
		app.logger.Error("request failed", "id", messageEnvelope.ID, "error", errMsg)
	}
}

func (app *application) handleTaskAssignmentRequest(messageEnvelope *models.MessageEnvelope) {
	params := &models.TaskAssignmentRequestParams{}
	err := json.Unmarshal(messageEnvelope.Params, params)
	if err != nil {
		app.logger.Error("invalid job params", "error", err)
		return
	}
	app.clientState.setState(params.JobID, params.ServerNonce)
	app.logger.Debug("job received", "job_id", params.JobID)
}

func (app *application) sendAuthorizationRequest() {
	requestID := app.clientState.getNextRequestID()

	nonce, _ := util.GenerateNonce()
	username := "user-" + nonce[:8]

	authorizationRequest := &models.AuthenticationRequest{
		BaseRequest: models.BaseRequest{
			BasePayload: models.BasePayload{
				ID: &requestID,
			},
			Method: "authorize",
		},
		Params: models.AuthenticationRequestParams{
			Username: username,
		},
	}
	err := app.encoder.Encode(authorizationRequest)
	if err != nil {
		app.logger.Error("failed to send authorize", "error", err)
	}
	return
}

func (app *application) sendResultSubmissionRequest(jobID int, clientNonce string, hash string) error {
	requestID := app.clientState.getNextRequestID()
	resultSubmissionRequest := &models.ResultSubmissionRequest{
		BaseRequest: models.BaseRequest{
			BasePayload: models.BasePayload{
				ID: &requestID,
			},
			Method: "submit",
		},
		Params: models.ResultSubmissionRequestParams{
			JobID:       jobID,
			ClientNonce: clientNonce,
			Result:      hash,
		},
	}
	err := app.encoder.Encode(resultSubmissionRequest)
	if err != nil {
		return err
	}
	app.logger.Debug("submit sent", "job_id", jobID)
	return nil
}
