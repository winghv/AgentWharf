package auth

import (
	"context"
	"testing"
	"time"
)

func TestLocalSessionCredentialIssuerResolvesActiveEvidenceWithoutBearer(t *testing.T) {
	issuer, err := NewLocalSessionCredentialIssuer([]byte("test-only-session-credential-evidence"), 1)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := issuer.PrepareSessionCredential(context.Background(), SessionCredentialRequest{
		SessionID: "ses_target", Lineage: SessionCredentialLineage{Kind: SessionCredentialTargetRotation, AttachID: "att_target"},
		Generation: 2, RotationID: "rot_target", RevocationID: "rev_target", ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.SessionCredentialEvidence(context.Background(), prepared.Bearer); err == nil {
		t.Fatal("pending credential unexpectedly resolved evidence")
	}
	if err := issuer.ActivateSessionCredential(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	evidence, err := issuer.SessionCredentialEvidence(context.Background(), prepared.Bearer)
	if err != nil || evidence.SessionID != prepared.SessionID || evidence.Lineage != prepared.Lineage || evidence.Generation != prepared.Generation ||
		evidence.RotationID != prepared.RotationID || evidence.RevocationID != prepared.RevocationID || !evidence.ExpiresAt.Equal(prepared.ExpiresAt) {
		t.Fatalf("evidence = %+v, %v", evidence, err)
	}
}
