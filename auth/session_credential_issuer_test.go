package auth

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLocalSessionCredentialIssuerContract(t *testing.T) {
	SessionCredentialIssuerContract(t, SessionCredentialIssuerHarness{
		Open: func(t *testing.T) SessionCredentialIssuer {
			t.Helper()
			issuer, err := NewLocalSessionCredentialIssuer([]byte("test-only-local-session-credential-signer"), 3)
			if err != nil {
				t.Fatalf("NewLocalSessionCredentialIssuer() error = %v", err)
			}
			return issuer
		},
	})
}

func TestLocalSessionCredentialIssuerFailsClosedAndKeepsBearerOpaque(t *testing.T) {
	for _, config := range []struct {
		key     []byte
		version int64
	}{
		{key: nil, version: 1},
		{key: []byte("signer"), version: 0},
	} {
		if _, err := NewLocalSessionCredentialIssuer(config.key, config.version); err == nil {
			t.Fatalf("NewLocalSessionCredentialIssuer(%q, %d) unexpectedly succeeded", config.key, config.version)
		}
	}

	issuer, err := NewLocalSessionCredentialIssuer([]byte("test-only-local-session-credential-signer"), 3)
	if err != nil {
		t.Fatalf("NewLocalSessionCredentialIssuer() error = %v", err)
	}
	request := SessionCredentialRequest{
		SessionID:  "ses_target",
		Lineage:    SessionCredentialLineage{Kind: SessionCredentialTargetAttach, AttachID: "attach_1", JTI: "jti_1"},
		Generation: 2, RotationID: "rotation_1", RevocationID: "revocation_1", ExpiresAt: time.Now().Add(time.Minute),
	}
	prepared, err := issuer.PrepareSessionCredential(context.Background(), request)
	if err != nil {
		t.Fatalf("PrepareSessionCredential() error = %v", err)
	}
	if prepared.Scope != SessionAdapter(request.SessionID) {
		t.Fatalf("prepared scope = %s, want only %s", prepared.Scope, SessionAdapter(request.SessionID))
	}
	for _, sensitive := range []string{request.SessionID, request.Lineage.AttachID, request.Lineage.JTI, request.RotationID, request.RevocationID} {
		if strings.Contains(prepared.Bearer, sensitive) {
			t.Fatalf("bearer leaks request value %q", sensitive)
		}
	}
	if _, err := issuer.AuthenticateSessionCredential(context.Background(), prepared.Bearer); err == nil {
		t.Fatal("pending credential unexpectedly authenticated")
	}
	if err := issuer.ActivateSessionCredential(context.Background(), prepared); err != nil {
		t.Fatalf("ActivateSessionCredential() error = %v", err)
	}
	principal, err := issuer.AuthenticateSessionCredential(context.Background(), prepared.Bearer)
	if err != nil || principal.Subject != "local-session-credential" || len(principal.Scopes) != 1 || principal.Scopes[0] != SessionAdapter(request.SessionID) {
		t.Fatalf("AuthenticateSessionCredential() = %+v, %v", principal, err)
	}
	for _, mutate := range []func(*SessionCredentialRequest){
		func(request *SessionCredentialRequest) { request.RotationID = "rotation_2" },
		func(request *SessionCredentialRequest) { request.RevocationID = "revocation_2" },
	} {
		variant := request
		mutate(&variant)
		got, err := issuer.PrepareSessionCredential(context.Background(), variant)
		if err != nil {
			t.Fatalf("PrepareSessionCredential() error = %v", err)
		}
		if got.Bearer == prepared.Bearer {
			t.Fatalf("mutated credential request reused bearer: %+v", variant)
		}
		if err := issuer.ActivateSessionCredential(context.Background(), got); err != nil {
			t.Fatalf("ActivateSessionCredential() error = %v", err)
		}
	}
	if err := issuer.ActivateSessionCredential(context.Background(), PreparedSessionCredential{}); err == nil {
		t.Fatal("invalid activation unexpectedly succeeded")
	}
	if _, err := issuer.AuthenticateSessionCredential(context.Background(), prepared.Bearer); err == nil {
		t.Fatal("replaced credential unexpectedly authenticated")
	}
	rotated, err := NewLocalSessionCredentialIssuer([]byte("test-only-local-session-credential-signer"), 4)
	if err != nil {
		t.Fatalf("NewLocalSessionCredentialIssuer(rotated) error = %v", err)
	}
	rotatedPrepared, err := rotated.PrepareSessionCredential(context.Background(), request)
	if err != nil || rotatedPrepared.Bearer == prepared.Bearer {
		t.Fatalf("rotated signer did not issue a distinct bearer: %v", err)
	}
	if _, err := issuer.AuthenticateSessionCredential(context.Background(), rotatedPrepared.Bearer); err == nil {
		t.Fatal("credential from another issuer unexpectedly authenticated")
	}
	if _, err := issuer.AuthenticateSessionCredential(context.Background(), "unknown"); err == nil {
		t.Fatal("unknown credential unexpectedly authenticated")
	}
	for index := 0; index < maxLocalSessionCredentials+1; index++ {
		pendingRequest := request
		pendingRequest.SessionID = "ses_pending_" + strconv.Itoa(index)
		pending, err := issuer.PrepareSessionCredential(context.Background(), pendingRequest)
		if err != nil {
			t.Fatalf("prepare pending %d: %v", index, err)
		}
		issuer.DiscardSessionCredential(context.Background(), pending)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := issuer.PrepareSessionCredential(canceled, request); err == nil {
		t.Fatal("canceled prepare unexpectedly succeeded")
	}
}

func TestLocalSessionCredentialIssuerRecoversSealedCredentialAfterRestart(t *testing.T) {
	key := []byte("test-only-session-credential-restart-key")
	issuer, err := NewLocalSessionCredentialIssuer(key, 7)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := issuer.PrepareSessionCredential(context.Background(), SessionCredentialRequest{
		SessionID: "ses_target", Lineage: SessionCredentialLineage{Kind: SessionCredentialTargetRotation, AttachID: "att_target"},
		Generation: 2, RotationID: "rot_2", RevocationID: "rev_2", ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := issuer.ActivateSessionCredential(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewLocalSessionCredentialIssuer(key, 7)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := restarted.ActiveSessionCredential(context.Background(), prepared.Bearer)
	if err != nil || actual.Bearer != prepared.Bearer || actual.SessionID != prepared.SessionID || actual.Lineage != prepared.Lineage || actual.Generation != prepared.Generation || actual.RotationID != prepared.RotationID || actual.RevocationID != prepared.RevocationID || !actual.ExpiresAt.Equal(prepared.ExpiresAt) || actual.Scope != prepared.Scope {
		t.Fatalf("restart credential = %+v, %v; want %+v", actual, err, prepared)
	}
}
