package main

import (
	"encoding/json"
	"log/slog"
	"net"
	"os"

	"github.com/cepwn/tcphashsubmit/internal/models"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	conn, err := net.Dial("tcp", ":1337")

	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	sendAuthorizationRequest(encoder, logger)
	listenForMessages(decoder, logger)
}

func listenForMessages(decoder *json.Decoder, logger *slog.Logger) {
	for {
		messageEnvelope := &models.MessageEnvelope{}
		err := decoder.Decode(messageEnvelope)
		if err != nil {
			logger.Error("unknown message type received", "message", messageEnvelope)
			return
		}
		logger.Info("received message envelope:", "id", messageEnvelope.ID)

		if messageEnvelope.ID == nil {
			handleTaskAssignmentRequest(messageEnvelope, logger)
		} else {
			handleResponse(messageEnvelope, logger)
		}
	}
}

func handleResponse(messageEnvelope *models.MessageEnvelope, logger *slog.Logger) {
	if messageEnvelope.Result != nil && *messageEnvelope.Result {
		logger.Info("request succeeded", "id", messageEnvelope.ID)
	} else {
		errMsg := "unknown error"
		if messageEnvelope.Error != nil {
			errMsg = *messageEnvelope.Error
		}
		logger.Error("request failed", "id", messageEnvelope.ID, "error", errMsg)
	}
}

func handleTaskAssignmentRequest(messageEnvelope *models.MessageEnvelope, logger *slog.Logger) {
	params := &models.TaskAssignmentRequestParams{}
	err := json.Unmarshal(messageEnvelope.Params, params)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	logger.Info("received task assignment request", "job_id", params.JobID)
}

func sendAuthorizationRequest(encoder *json.Encoder, logger *slog.Logger) {
	authRequestID := 1
	authorizationRequest := &models.AuthenticationRequest{
		BaseRequest: models.BaseRequest{
			BasePayload: models.BasePayload{
				ID: &authRequestID,
			},
			Method: "authorize",
		},
		Params: models.AuthenticationRequestParams{
			Username: "test",
		},
	}
	err := encoder.Encode(authorizationRequest)
	if err != nil {
		logger.Error(err.Error())
	}
	return
}

func sendResultSubmissionRequest(encoder *json.Encoder, logger *slog.Logger) {
	resultSubmissionRequestID := 2
	resultSubmissionRequest := &models.ResultSubmissionRequest{
		BaseRequest: models.BaseRequest{
			BasePayload: models.BasePayload{
				ID: &resultSubmissionRequestID,
			},
			Method: "submit",
		},
		Params: models.ResultSubmissionRequestParams{
			JobID:       1,
			ClientNonce: "1234567890",
		},
	}
	err := encoder.Encode(resultSubmissionRequest)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	logger.Info("sent result submission request")
}
