package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSessionCredentialIssuerContract(t *testing.T) {
	SessionCredentialIssuerContract(t, SessionCredentialIssuerHarness{
		Open: func(t *testing.T) SessionCredentialIssuer {
			t.Helper()
			return &memorySessionCredentialIssuer{prepared: make(map[SessionCredentialRequest]PreparedSessionCredential)}
		},
	})
}

type SessionCredentialIssuerHarness struct {
	Open func(t *testing.T) SessionCredentialIssuer
}

func SessionCredentialIssuerContract(t *testing.T, harness SessionCredentialIssuerHarness) {
	t.Helper()
	if harness.Open == nil {
		t.Fatal("credential issuer contract harness must provide open")
	}
	issuer := harness.Open(t)
	for _, request := range []SessionCredentialRequest{
		{SessionID: "ses_bootstrap", Lineage: SessionCredentialLineage{Kind: SessionCredentialBootstrapInitial}, Generation: 1, RotationID: "rot_bootstrap", RevocationID: "revoke_bootstrap", ExpiresAt: time.Now().Add(time.Minute)},
		{SessionID: "ses_target", Lineage: SessionCredentialLineage{Kind: SessionCredentialTargetAttach, AttachID: "attach_1", JTI: "jti_1"}, Generation: 2, RotationID: "rot_target", RevocationID: "revoke_target", ExpiresAt: time.Now().Add(time.Minute)},
	} {
		prepared, err := issuer.PrepareSessionCredential(context.Background(), request)
		if err != nil {
			t.Fatalf("PrepareSessionCredential() error = %v", err)
		}
		assertPreparedSessionCredential(t, prepared, request)
		duplicate, err := issuer.PrepareSessionCredential(context.Background(), request)
		if err != nil || duplicate != prepared {
			t.Fatalf("idempotent prepare = %+v, %v; want %+v, nil", duplicate, err, prepared)
		}
	}
	target := SessionCredentialRequest{SessionID: "ses_target_bound", Lineage: SessionCredentialLineage{Kind: SessionCredentialTargetAttach, AttachID: "attach_bound", JTI: "jti_bound"}, Generation: 2, RotationID: "rot_shared", RevocationID: "revoke_bound", ExpiresAt: time.Now().Add(time.Minute)}
	baseline, err := issuer.PrepareSessionCredential(context.Background(), target)
	if err != nil {
		t.Fatalf("prepare bound target: %v", err)
	}
	for _, mutate := range []func(*SessionCredentialRequest){
		func(request *SessionCredentialRequest) { request.SessionID = "ses_target_other" },
		func(request *SessionCredentialRequest) { request.Lineage.AttachID = "attach_other" },
		func(request *SessionCredentialRequest) { request.Lineage.JTI = "jti_other" },
		func(request *SessionCredentialRequest) { request.Generation++ },
		func(request *SessionCredentialRequest) { request.ExpiresAt = request.ExpiresAt.Add(time.Second) },
	} {
		variant := target
		mutate(&variant)
		prepared, err := issuer.PrepareSessionCredential(context.Background(), variant)
		if err != nil || prepared.Bearer == baseline.Bearer {
			t.Fatalf("identity-bound prepare = %+v, %v; bearer must differ", prepared, err)
		}
		assertPreparedSessionCredential(t, prepared, variant)
	}
	for _, request := range []SessionCredentialRequest{
		{Lineage: SessionCredentialLineage{Kind: SessionCredentialBootstrapInitial}, Generation: 1, RotationID: "rot", RevocationID: "revoke", ExpiresAt: time.Now().Add(time.Minute)},
		{SessionID: "ses_invalid", Lineage: SessionCredentialLineage{Kind: SessionCredentialTargetAttach, AttachID: "attach"}, Generation: 1, RotationID: "rot", RevocationID: "revoke", ExpiresAt: time.Now().Add(time.Minute)},
		{SessionID: "ses_invalid", Lineage: SessionCredentialLineage{Kind: SessionCredentialLineageKind("unknown")}, Generation: 1, RotationID: "rot", RevocationID: "revoke", ExpiresAt: time.Now().Add(time.Minute)},
		{SessionID: "ses_invalid", Lineage: SessionCredentialLineage{Kind: SessionCredentialBootstrapInitial}, Generation: 0, RotationID: "rot", RevocationID: "revoke", ExpiresAt: time.Now().Add(time.Minute)},
		{SessionID: "ses_invalid", Lineage: SessionCredentialLineage{Kind: SessionCredentialBootstrapInitial}, Generation: 1, RotationID: "rot", RevocationID: "revoke", ExpiresAt: time.Now().Add(-time.Second)},
	} {
		if _, err := issuer.PrepareSessionCredential(context.Background(), request); err == nil {
			t.Fatalf("invalid PrepareSessionCredential(%+v) unexpectedly succeeded", request)
		}
	}
}

func assertPreparedSessionCredential(t *testing.T, prepared PreparedSessionCredential, request SessionCredentialRequest) {
	t.Helper()
	if prepared.Bearer == "" || prepared.SessionID != request.SessionID || prepared.Lineage != request.Lineage || prepared.Generation != request.Generation || prepared.RotationID != request.RotationID || prepared.RevocationID != request.RevocationID || !prepared.ExpiresAt.Equal(request.ExpiresAt) || prepared.Scope != SessionAdapter(request.SessionID) {
		t.Fatalf("prepared credential = %+v, request = %+v", prepared, request)
	}
}

type memorySessionCredentialIssuer struct {
	prepared map[SessionCredentialRequest]PreparedSessionCredential
}

func (issuer *memorySessionCredentialIssuer) PrepareSessionCredential(_ context.Context, request SessionCredentialRequest) (PreparedSessionCredential, error) {
	if request.SessionID == "" || request.Generation < 1 || request.RotationID == "" || request.RevocationID == "" || !request.ExpiresAt.After(time.Now()) {
		return PreparedSessionCredential{}, errors.New("invalid session credential request")
	}
	switch request.Lineage.Kind {
	case SessionCredentialBootstrapInitial:
		if request.Lineage.AttachID != "" || request.Lineage.JTI != "" {
			return PreparedSessionCredential{}, errors.New("invalid bootstrap lineage")
		}
	case SessionCredentialTargetAttach:
		if request.Lineage.AttachID == "" || request.Lineage.JTI == "" {
			return PreparedSessionCredential{}, errors.New("invalid target lineage")
		}
	default:
		return PreparedSessionCredential{}, errors.New("invalid session credential lineage")
	}
	if credential, ok := issuer.prepared[request]; ok {
		return credential, nil
	}
	identity := fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%s\x00%s\x00%d", request.SessionID, request.Lineage.AttachID, request.Lineage.JTI, request.Generation, request.RotationID, request.RevocationID, request.ExpiresAt.UnixNano())
	digest := sha256.Sum256([]byte(identity))
	credential := PreparedSessionCredential{Bearer: fmt.Sprintf("memory-bearer-%x", digest), SessionID: request.SessionID, Lineage: request.Lineage, Generation: request.Generation, RotationID: request.RotationID, RevocationID: request.RevocationID, ExpiresAt: request.ExpiresAt, Scope: SessionAdapter(request.SessionID)}
	issuer.prepared[request] = credential
	return credential, nil
}
