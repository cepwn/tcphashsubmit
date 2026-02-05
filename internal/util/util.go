package util

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

func StrPtr(s string) *string { return &s }

func GenerateNonce() (string, error) {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func ComputeSHA256(data string) string {
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}
