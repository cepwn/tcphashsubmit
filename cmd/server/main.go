package main

import (
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/store"
	"github.com/cepwn/tcphashsubmit/migrations"
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
		app.logger.Error("failed to broadcast task", "job_id", jobID, "error", err)
	}
	if len(errs) == 0 {
		app.logger.Debug("job broadcast", "job_id", jobID)
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	pgDB, err := store.Open("host=localhost user=luxor password=luxor dbname=luxor port=5432 sslmode=disable")
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pgDB.Close()
	logger.Info("database connected")

	err = store.MigrateFS(pgDB, migrations.FS, ".")
	if err != nil {
		logger.Error("migrations failed", "error", err)
		os.Exit(1)
	}
	logger.Info("migrations applied")

	submissionStore := store.NewPostgresSubmissionStore(pgDB)
	submissionCh := make(chan submissionEvent, 1000)

	listener, err := net.Listen("tcp", ":1337")
	if err != nil {
		logger.Error("listen failed", "error", err)
		os.Exit(1)
	}
	defer listener.Close()

	logger.Info("server started", "addr", listener.Addr().String())

	sessionManager := newSessionManager()
	jobManager := newJobManager()

	app := &application{
		logger:          logger,
		sessionManager:  sessionManager,
		jobManager:      jobManager,
		submissionStore: submissionStore,
		submissionCh:    submissionCh,
	}

	go app.runJobTicker()
	go app.processSubmissions()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("accept failed", "error", err)
			os.Exit(1)
		}
		logger.Debug("connection accepted", "remote", conn.RemoteAddr().String())
		go app.handleConnection(conn)
	}
}
