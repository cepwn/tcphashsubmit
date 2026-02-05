package main

import (
	"sync"
	"testing"
)

func TestJobManager_GenerateJob(t *testing.T) {
	jm := newJobManager()

	err := jm.generateJob()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jobID, nonce := jm.getCurrentJobInfo()
	if jobID != 1 {
		t.Errorf("expected job ID 1, got %d", jobID)
	}
	if nonce == "" {
		t.Error("expected non-empty nonce")
	}
}

func TestJobManager_IncrementingJobID(t *testing.T) {
	jm := newJobManager()

	for i := 1; i <= 5; i++ {
		err := jm.generateJob()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		jobID, _ := jm.getCurrentJobInfo()
		if jobID != i {
			t.Errorf("expected job ID %d, got %d", i, jobID)
		}
	}
}

func TestJobManager_UniqueNonces(t *testing.T) {
	jm := newJobManager()
	seen := make(map[string]bool)

	for i := 0; i < 10; i++ {
		err := jm.generateJob()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, nonce := jm.getCurrentJobInfo()
		if seen[nonce] {
			t.Errorf("duplicate nonce generated: %s", nonce)
		}
		seen[nonce] = true
	}
}

func TestJobManager_ConcurrentAccess(t *testing.T) {
	jm := newJobManager()
	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = jm.generateJob()
		}
	}()

	// Reader goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				jm.getCurrentJobInfo()
			}
		}()
	}

	wg.Wait()
}
