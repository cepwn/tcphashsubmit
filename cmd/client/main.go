package main

import (
	"encoding/json"
	"log/slog"
	"net"
	"os"
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

	conn, err := net.Dial("tcp", ":1337")

	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	defer conn.Close()

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
	go app.listenForMessages()
	go app.submitResults()

	select {}
}
