package util_test

import (
	"testing"

	"github.com/cepwn/tcphashsubmit/internal/util"
)

func TestComputeSHA256(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "known hash for 123456",
			input:    "123456",
			expected: "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:     "server_nonce + client_nonce concatenation",
			input:    "abc" + "def",
			expected: "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := util.ComputeSHA256(tt.input)
			if result != tt.expected {
				t.Errorf("ComputeSHA256(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGenerateNonce(t *testing.T) {
	t.Run("returns 32 character hex string", func(t *testing.T) {
		nonce, err := util.GenerateNonce()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(nonce) != 32 {
			t.Errorf("expected nonce length 32, got %d", len(nonce))
		}
	})

	t.Run("generates unique nonces", func(t *testing.T) {
		nonce1, err := util.GenerateNonce()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		nonce2, err := util.GenerateNonce()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nonce1 == nonce2 {
			t.Errorf("expected unique nonces, got %q twice", nonce1)
		}
	})
}

func TestStrPtr(t *testing.T) {
	s := util.StrPtr("hello")
	if s == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *s != "hello" {
		t.Errorf("expected 'hello', got %q", *s)
	}
}
