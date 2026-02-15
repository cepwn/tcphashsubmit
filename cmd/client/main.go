package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type application struct {
	logger      *slog.Logger
	clientState *clientState
	encoder     *json.Encoder
	decoder     *json.Decoder
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	clientCtx, clientCtxCancel := context.WithCancel(context.Background())
	defer clientCtxCancel()

	serverAddr := flag.String("addr", ":1337", "server address to dial into")
	flag.Parse()

	conn, err := net.Dial("tcp", *serverAddr)

	if err != nil {
		logger.Error("connection failed", "server", *serverAddr, "error", err)
		os.Exit(1)
	}

	go func() {
		<-clientCtx.Done()
		conn.SetDeadline(time.Now())
	}()

	defer conn.Close()

	logger.Info("client started", "server", *serverAddr)

	clientState := newClientState()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	app := &application{
		logger:      logger,
		clientState: clientState,
		decoder:     decoder,
		encoder:     encoder,
	}

	app.sendAuthorizationRequest()

	disconnected := make(chan struct{})
	var connWg sync.WaitGroup
	connWg.Add(1)
	go func() {
		defer connWg.Done()
		app.listenForMessages(clientCtx, disconnected)
	}()

	go app.submitResults(clientCtx)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case <-disconnected:
		logger.Warn("server closed connection")
	case <-stop:
		logger.Warn("received shutdown signal")
	}

	logger.Info("shutting down...")

	shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCtxCancel()

	clientCtxCancel()
	conn.Close()

	done := make(chan struct{})
	go func() {
		connWg.Wait()
		close(done)
	}()
	select {
	case <-shutdownCtx.Done():
		logger.Error("shutdown timed out")
	case <-done:
		logger.Info("shutdown complete")
	}
}
