package main

import (
	"log/slog"
	"net"
	"os"
)

type application struct {
	logger *slog.Logger
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

	app := &application{logger: logger}

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		go app.handleConnection(conn)
	}
}
