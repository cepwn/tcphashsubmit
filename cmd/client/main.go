package main

import (
	"encoding/json"
	"fmt"
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

	sendAuthorizationRequest(conn, logger)
	sendResultSubmissionRequest(conn, logger)
}

func sendAuthorizationRequest(conn net.Conn, logger *slog.Logger) {
	authRequestID := 1
	authenticationRequest := &models.AuthenticationRequest{
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

	authRequestJson, err := json.Marshal(authenticationRequest)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	authRequestJson = append(authRequestJson, '\n')

	n, err := conn.Write(authRequestJson)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("Wrote %d bytes for authorization request", n))

	decoder := json.NewDecoder(conn)
	response := &models.Response{}
	err = decoder.Decode(response)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	logger.Info("Received authorization response:", "result", response.Result)
}

func sendResultSubmissionRequest(conn net.Conn, logger *slog.Logger) {
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
	resultSubmissionRequestJson, err := json.Marshal(resultSubmissionRequest)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	resultSubmissionRequestJson = append(resultSubmissionRequestJson, '\n')

	n, err := conn.Write(resultSubmissionRequestJson)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("Wrote %d bytes for result submission request", n))

}
