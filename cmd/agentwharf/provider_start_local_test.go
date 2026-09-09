package main

import (
	"context"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func TestProviderStartRevalidatesLocalAuthorityOnEveryAttempt(t *testing.T) {
	calls, writes := 0, 0
	admission := newProviderStartAdmission(2, func(context.Context) (protocol.Frame, error) { return &protocol.ProviderStartPrepare{Attempt: 1}, nil }, func(protocol.Frame) error { writes++; return nil }, nil)
	admission.verifyLocal = func(context.Context) error {
		calls++
		if calls > 1 {
			return errors.New("revoked")
		}
		return nil
	}
	if err := admission.PrepareProcessStart(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := admission.PrepareProcessStart(context.Background(), 2); err == nil {
		t.Fatal("revoked local authority allowed restart")
	}
	if calls != 2 || writes != 1 {
		t.Fatalf("checks=%d network writes=%d", calls, writes)
	}
}
