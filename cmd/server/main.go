package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/cepwn/tcphashsubmit/internal/models"
	"github.com/cepwn/tcphashsubmit/internal/util"
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
	session := &models.Session{}
	for {
		rawRequest := &models.RawRequest{}
		err := decoder.Decode(rawRequest)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logger.Error(err.Error())
			}
			logger.Info("Connection closed by client")
			return
		}

		switch rawRequest.Method {
		case "authorize":
			handleAuthorizationRequest(rawRequest, session, encoder, logger)
		case "submit":
			handleResultSubmissionRequest(rawRequest, session, encoder, logger)
		default:
			logger.Error("Unknown method:", "method", rawRequest.Method)
		}
	}
}

func handleAuthorizationRequest(rawRequest *models.RawRequest, session *models.Session, encoder *json.Encoder, logger *slog.Logger) {
	params := &models.AuthenticationRequestParams{}
	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		logger.Error(err.Error())

		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("internal server error"),
		})
		if err != nil {
			logger.Error(err.Error())
		}
		return
	}
	if params.Username == "" {
		logger.Error("Received authentication request without username")
		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("bad request"),
		})
		if err != nil {
			logger.Error(err.Error())
		}
		return
	}
	if session.Authenticated {
		logger.Error("Received authentication request after authentication")
		err = encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("already authenticated"),
		})
		if err != nil {
			logger.Error(err.Error())
		}
		return
	}
	logger.Info("Received authentication request:", "username", params.Username)

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
		logger.Error(err.Error())
	}
}

func handleResultSubmissionRequest(rawRequest *models.RawRequest, session *models.Session, encoder *json.Encoder, logger *slog.Logger) {
	if !session.Authenticated {
		logger.Error("Received result submission request without authentication")
		err := encoder.Encode(&models.Response{
			BasePayload: models.BasePayload{
				ID: rawRequest.ID,
			},
			Result: false,
			Error:  util.StrPtr("not authenticated"),
		})
		if err != nil {
			logger.Error(err.Error())
		}
		return
	}
	params := &models.ResultSubmissionRequestParams{}

	err := json.Unmarshal(rawRequest.Params, params)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	logger.Info("Received result submission request:", "job_id", params.JobID)
	err = encoder.Encode(&models.Response{
		BasePayload: models.BasePayload{
			ID: rawRequest.ID,
		},
		Result: true,
	})
	if err != nil {
		logger.Error(err.Error())
	}
}
