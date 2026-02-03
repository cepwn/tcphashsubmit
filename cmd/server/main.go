package main

import (
	"log/slog"
	"net"
	"os"
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

	conn, err := listener.Accept()

	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	logger.Info(string(buf[:n]))
}
