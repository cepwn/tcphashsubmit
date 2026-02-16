package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cepwn/tcphashsubmit/internal/queue"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	amqpConn, err := queue.Open("amqp://guest:guest@localhost:5672/")
	if err != nil {
		logger.Error("amqp connection failed", "error", err)
		os.Exit(1)
	}

	submissionPublisher, err := queue.NewRabbitMQSubmissionQueue(amqpConn)
	if err != nil {
		logger.Error("submission queue init failed", "error", err)
		os.Exit(1)
	}
	defer submissionPublisher.Close()

	addr := flag.String("addr", ":1337", "TCP network address")
	flag.Parse()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("listen failed", "error", err)
		os.Exit(1)
	}
	defer listener.Close()

	logger.Info("server started", "addr", listener.Addr().String())

	sessionManager := newSessionManager()
	jobManager := newJobManager()

	app := &application{
		logger:              logger,
		sessionManager:      sessionManager,
		jobManager:          jobManager,
		submissionPublisher: submissionPublisher,
	}

	serverCtx, serverCtxCancel := context.WithCancel(context.Background())
	defer serverCtxCancel()

	go app.runJobTicker(serverCtx)

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
}
