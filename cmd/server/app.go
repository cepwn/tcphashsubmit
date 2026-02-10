package main

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/store"
)

type submissionEvent struct {
	Username  string
	Timestamp time.Time
}

type application struct {
	logger          *slog.Logger
	sessionManager  *sessionManager
	jobManager      *jobManager
	submissionStore store.SubmissionStore
	submissionCh    chan submissionEvent
}

func (app *application) processSubmissions() {
	for event := range app.submissionCh {
		err := app.submissionStore.RecordSubmission(event.Username, event.Timestamp)
		if err != nil {
			app.logger.Error("failed to record submission", "username", event.Username, "error", err)
		}
	}
}

func (app *application) runJobTicker(serverCtx context.Context) {
	app.generateAndBroadcastJob()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-serverCtx.Done():
			app.logger.Debug("stopping job ticker")
			return
		case <-ticker.C:
			app.generateAndBroadcastJob()
		}
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
		app.logger.Error("failed to broadcast task", "job_id", jobID, "error", err)
	}
	if len(errs) == 0 {
		app.logger.Debug("job broadcast", "job_id", jobID)
	}
}

func (app *application) listenAndServe(listener net.Listener, serverCtx context.Context, connWg *sync.WaitGroup) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if serverCtx.Err() != nil {
				app.logger.Info("stopped accepting new connections")
				return
			}
			app.logger.Error("accept failed", "error", err)
			continue
		}
		app.logger.Debug("connection accepted", "remote", conn.RemoteAddr().String())
		connWg.Add(1)
		go func() {
			defer connWg.Done()
			app.handleConnection(serverCtx, conn)
		}()
	}
}
