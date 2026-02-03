package main

import (
	"errors"
	"io"
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
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, logger)

	}
}

func handleConnection(conn net.Conn, logger *slog.Logger) {
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logger.Error(err.Error())
			}
			return
		}
		logger.Info(string(buf[:n]), "remote_addr", conn.RemoteAddr())
	}
}
