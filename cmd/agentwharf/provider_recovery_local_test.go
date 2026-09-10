package main

import (
	"context"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func TestRecoveryReferenceDoesNotOverrideLocalRevocation(t *testing.T) {
	revoked := false
	checks := 0
	admission := &providerStartAdmission{
		read: func(context.Context) (protocol.Frame, error) {
			t.Fatal("local recovery check attempted network authorization")
			return nil, nil
		},
		recoveryHandle: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
		verifyLocal: func(context.Context) error {
			checks++
			if revoked {
				return errors.New("revoked")
			}
			return nil
		},
	}
	if err := admission.VerifyRecoveryStart(context.Background()); err != nil {
		t.Fatal(err)
	}
	revoked = true
	if err := admission.VerifyRecoveryStart(context.Background()); err == nil {
		t.Fatal("Hub recovery handle bypassed local rejection")
	}
	if checks != 2 {
		t.Fatal("local authority was cached")
	}
}
