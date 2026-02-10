package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/store"
	"github.com/cepwn/tcphashsubmit/migrations"
)

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

	serverCtx, serverCtxCancel := context.WithCancel(context.Background())
	defer serverCtxCancel()

	go app.runJobTicker(serverCtx)
	var submissionWg sync.WaitGroup
	submissionWg.Add(1)
	go func() {
		defer submissionWg.Done()
		app.processSubmissions()
	}()

	var connWg sync.WaitGroup
	go app.listenAndServe(listener, serverCtx, &connWg)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	// unblock main routine by writing to stop channel
	<-stop

	logger.Info("shutting down...")
	// start timer
	shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCtxCancel()
	// shutdown server, any checks to errors can tell if server is being shut down
	serverCtxCancel()
	// close listener, stop accepting new connections
	listener.Close()

	// wait until all handle connection functions exit
	done := make(chan struct{})
	go func() {
		connWg.Wait()
		close(done)
	}()

	// block until grace period timeout or all connection functions exit
	select {
	case <-done:
		logger.Info("all pending connection requests processed")
	case <-shutdownCtx.Done():
		logger.Warn("timed out waiting for connections to close")
		return
	}

	// close submissions channel once no one else will write to it
	close(submissionCh)
	drainDone := make(chan struct{})
	go func() {
		submissionWg.Wait()
		close(drainDone)
	}()

	// block until grace period timeout or all connection functions exit
	select {
	case <-drainDone:
		logger.Info("all pending submissions processed")
	case <-shutdownCtx.Done():
		logger.Warn("timed out waiting for connections to close")
	}

}
