package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
)

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

	message := "Hello World!\n"
	messageBytes := []byte(message)

	n, err := conn.Write(messageBytes)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("Wrote %d bytes", n))
}
