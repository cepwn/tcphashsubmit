package util

import (
	"crypto/rand"
	"encoding/hex"
)

func GenerateNonce() (string, error) {
	bytes := make([]byte, 16) // 16 bytes = 32 hex chars
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
