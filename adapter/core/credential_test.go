package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testSessionCredential(t *testing.T, sessionID string) *SessionCredential {
	t.Helper()
	credential, err := NewSessionCredential("bearer-"+sessionID, SessionCredentialMetadata{
		SessionID:  sessionID,
		Lineage:    SessionCredentialLineage{Kind: "target_attach", AttachID: "attach_" + sessionID, JTI: "jti_" + sessionID},
		Generation: 1,
		ExpiresAt:  time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("NewSessionCredential() error = %v", err)
	}
	return credential
}

func TestSessionCredentialRejectsInvalidAndExpiredMetadata(t *testing.T) {
	valid := SessionCredentialMetadata{
		SessionID:  "ses_credential_invalid",
		Lineage:    SessionCredentialLineage{Kind: "bootstrap_initial"},
		Generation: 1,
		ExpiresAt:  time.Now().Add(time.Minute),
	}
	for name, mutate := range map[string]func(*SessionCredentialMetadata){
		"missing session": func(metadata *SessionCredentialMetadata) { metadata.SessionID = "" },
		"missing lineage": func(metadata *SessionCredentialMetadata) { metadata.Lineage.Kind = "" },
		"zero generation": func(metadata *SessionCredentialMetadata) { metadata.Generation = 0 },
		"expired":         func(metadata *SessionCredentialMetadata) { metadata.ExpiresAt = time.Now().Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			metadata := valid
			mutate(&metadata)
			if _, err := NewSessionCredential("secret", metadata); !errors.Is(err, ErrInvalidSessionCredential) {
				t.Fatalf("NewSessionCredential() error = %v, want ErrInvalidSessionCredential", err)
			}
		})
	}
	if _, err := NewSessionCredential("secret\nvalue", valid); !errors.Is(err, ErrInvalidSessionCredential) {
		t.Fatalf("newline credential error = %v, want ErrInvalidSessionCredential", err)
	}
}

func TestSessionCredentialCannotBeSerializedOrLogged(t *testing.T) {
	credential := testSessionCredential(t, "ses_credential_redact")
	for _, rendered := range []string{fmt.Sprint(credential), fmt.Sprintf("%v", credential), fmt.Sprintf("%+v", credential), fmt.Sprintf("%#v", credential)} {
		if strings.Contains(rendered, "bearer-ses_credential_redact") || strings.Contains(rendered, "target_attach") {
			t.Fatalf("credential rendering leaked secret or lineage: %q", rendered)
		}
	}
	if _, err := json.Marshal(credential); !errors.Is(err, ErrSessionCredentialNotSerializable) {
		t.Fatalf("json.Marshal() error = %v, want ErrSessionCredentialNotSerializable", err)
	}
	metadata, err := credential.Metadata()
	if err != nil || metadata.SessionID != "ses_credential_redact" || metadata.Generation != 1 {
		t.Fatalf("Metadata() = %+v, %v", metadata, err)
	}
}

func TestSessionWorkerRoutesOnlyBoundCredentialSession(t *testing.T) {
	receipts := &testDurableReceiptGate{
		commandAck:    CommandRoutingReceipt{CommandID: "cmd_bound", Status: CommandRoutingAccepted},
		ledgerReceipt: LedgerOperationReceipt{OperationID: "op_bound", Version: 1, Status: LedgerOperationPending},
	}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_bound",
		Credential:      testSessionCredential(t, "ses_bound"),
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	var applied atomic.Int32
	apply := func(context.Context) error { applied.Add(1); return nil }
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_bound", Type: "session.send", SessionID: "ses_other"}, apply); !errors.Is(err, ErrSessionCredentialMismatch) {
		t.Fatalf("cross-session route error = %v, want ErrSessionCredentialMismatch", err)
	}
	if applied.Load() != 0 {
		t.Fatal("cross-session route ran Provider side effect")
	}
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_bound", Type: "session.send", SessionID: "ses_bound"}, apply); err != nil {
		t.Fatalf("bound route error = %v", err)
	}
	if applied.Load() != 1 {
		t.Fatalf("bound route applied = %d, want 1", applied.Load())
	}
}

func TestSessionWorkerRouteRequiresCredentialAndRejectsExpiry(t *testing.T) {
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID: "ses_without_credential",
		Provider:  ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_missing", Type: "session.send"}, func(context.Context) error { return nil }); !errors.Is(err, ErrSessionCredentialRequired) {
		t.Fatalf("missing credential error = %v, want ErrSessionCredentialRequired", err)
	}
	credential := testSessionCredential(t, "ses_expiring")
	credential.metadata.ExpiresAt = time.Now().Add(-time.Second)
	if _, err := newSessionWorker(SessionWorkerConfig{
		SessionID:  "ses_expiring",
		Credential: credential,
		Provider:   ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner()); !errors.Is(err, ErrSessionCredentialExpired) {
		t.Fatalf("expired credential error = %v, want ErrSessionCredentialExpired", err)
	}
}
