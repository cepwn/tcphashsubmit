package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/queue"
	"github.com/cepwn/tcphashsubmit/internal/store"
	"github.com/cepwn/tcphashsubmit/migrations"
)

type application struct {
	logger             *slog.Logger
	submissionStore    store.SubmissionStore
	submissionConsumer queue.SubmissionConsumer
}

func (app *application) processSubmissions(consumerCtx context.Context) {
	deliveries, err := app.submissionConsumer.Subscribe()
	if err != nil {
		app.logger.Error("failed to subscribe to queue", "error", err)
		return
	}

	for {
		select {
		case <-consumerCtx.Done():
			err := app.submissionConsumer.Close()
			if err != nil {
				app.logger.Error("failed to close queue", "error", err)
			}
			return
		case delivery := <-deliveries:
			var se queue.SubmissionEvent
			if err := json.Unmarshal(delivery.Body, &se); err != nil {
				app.logger.Error("failed to unmarshal submission event, discarding message", "error", err)
			} else {
				app.logger.Debug("received message", "user", se.Username, "timestamp", se.Timestamp)
				if err := app.submissionStore.RecordSubmission(se.Username, se.Timestamp); err != nil {
					app.logger.Error("failed to record submission, discarding message", "username", se.Username, "error", err)
				}
			}
			if err := delivery.Ack(false); err != nil {
				app.logger.Error("failed to acknowledge message", "error", err)
			}
		}
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

	amqpConn, err := queue.Open("amqp://guest:guest@localhost:5672/")
	if err != nil {
		logger.Error("amqp connection failed", "error", err)
		os.Exit(1)
	}

	submissionConsumer, err := queue.NewRabbitMQSubmissionQueue(amqpConn)
	if err != nil {
		logger.Error("submission queue init failed", "error", err)
		os.Exit(1)
	}

	app := &application{
		logger:             logger,
		submissionStore:    submissionStore,
		submissionConsumer: submissionConsumer,
	}

	consumerCtx, consumerCtxCancel := context.WithCancel(context.Background())
	defer consumerCtxCancel()

	var submissionWg sync.WaitGroup
	submissionWg.Add(1)
	go func() {
		defer submissionWg.Done()
		app.processSubmissions(consumerCtx)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("shutting down...")

	shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCtxCancel()
	consumerCtxCancel()

	drainDone := make(chan struct{})
	go func() {
		submissionWg.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		logger.Info("all pending submissions processed")
	case <-shutdownCtx.Done():
		logger.Warn("timed out waiting for connections to close")
	}
}
