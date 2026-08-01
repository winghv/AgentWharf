package hub

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestContextWriteGateDoesNotBlockPastContextDeadline(t *testing.T) {
	gate := newContextWriteGate()
	release, err := gate.lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = gate.lock(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended write gate error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("contended write gate waited %s, want bounded wait", elapsed)
	}
}
