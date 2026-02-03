package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/cepwn/tcphashsubmit/internal/models"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	listener, err := net.Listen("tcp", ":1337")
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, logger)
	}
}

func handleConnection(conn net.Conn, logger *slog.Logger) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		rawRequest := &models.RawRequest{}
		err := decoder.Decode(rawRequest)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logger.Error(err.Error())
			}
			return
		}

		switch rawRequest.Method {
		case "authorize":
			params := &models.AuthenticationRequestParams{}
			err = json.Unmarshal(rawRequest.Params, params)
			if err != nil {
				logger.Error(err.Error())
				return
			}
			logger.Info("Received authentication request:", "username", params.Username)
			authorizationResponse := &models.Response{
				BasePayload: models.BasePayload{
					ID: rawRequest.ID,
				},
				Result: true,
			}
			err := encoder.Encode(authorizationResponse)
			if err != nil {
				logger.Error(err.Error())
			}
		case "submit":
			params := &models.ResultSubmissionRequestParams{}
			err = json.Unmarshal(rawRequest.Params, params)
			if err != nil {
				logger.Error(err.Error())
				return
			}
			logger.Info("Received result submission request:", "job_id", params.JobID)
		default:
			logger.Error("Unknown method:", "method", rawRequest.Method)
		}
	}
}
