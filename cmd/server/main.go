package main

import (
	"log/slog"
	"net"
	"os"
	"time"
)

type application struct {
	logger         *slog.Logger
	sessionManager *sessionManager
	jobManager     *jobManager
}

func (app *application) runJobTicker() {
	app.generateAndBroadcastJob()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		app.generateAndBroadcastJob()
	}
}

func (app *application) generateAndBroadcastJob() {
	err := app.jobManager.generateJob()
	if err != nil {
		app.logger.Error("failed to generate job", "error", err)
		return
	}
	jobID, serverNonce := app.jobManager.getCurrentJobInfo()
	errs := app.sessionManager.broadcastTasks(jobID, serverNonce)
	for _, err := range errs {
		app.logger.Error("failed to broadcast task", "error", err)
	}
}

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

	sessionManager := newSessionManager()
	jobManager := newJobManager()

	app := &application{logger: logger, sessionManager: sessionManager, jobManager: jobManager}
	go app.runJobTicker()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		go app.handleConnection(conn)
	}
}
