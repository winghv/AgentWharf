package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
)

func TestAdapterTerminalProjectsAndFencesAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attention := openStore(t, path)
	ctx := context.Background()
	if _, err := attention.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_adapter_terminal", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("initialize adapter connection: %v", err)
	}
	connection, err := attention.AcceptAdapterHello(ctx, "ses_adapter_terminal", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept adapter hello: %v", err)
	}
	grantFence, err := attention.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("allocate adapter grant fence: %v", err)
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence}
	if _, err := attention.AppendAdapterEvents(ctx, "ses_adapter_terminal", admission, []store.PendingEvent{{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
		t.Fatalf("append terminal adapter event: %v", err)
	}
	summary, err := attention.AttentionSnapshot(ctx, []string{"ses_adapter_terminal"})
	if err != nil || len(summary) != 1 || summary[0].TerminalOutcome == nil || *summary[0].TerminalOutcome != "ended" {
		t.Fatalf("terminal adapter attention snapshot = %+v, %v", summary, err)
	}
	connection, err = attention.AdapterConnection(ctx, "ses_adapter_terminal")
	if err != nil || connection.TerminalAt == nil || connection.RevokedAt == nil {
		t.Fatalf("terminal adapter connection = %+v, %v", connection, err)
	}
	if _, err := attention.AppendAdapterEvents(ctx, "ses_adapter_terminal", admission, []store.PendingEvent{{Type: "session.message", Time: testTime(2), Payload: []byte(`{"role":"agent"}`)}}); err == nil {
		t.Fatal("stale adapter admission appended after terminal fence")
	}
	if err := attention.Close(); err != nil {
		t.Fatalf("close terminal adapter store: %v", err)
	}
	reopened, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen terminal adapter store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.AppendAdapterEvents(ctx, "ses_adapter_terminal", admission, []store.PendingEvent{{Type: "session.message", Time: testTime(3), Payload: []byte(`{"role":"agent"}`)}}); err == nil {
		t.Fatal("reopened stale adapter admission appended after terminal fence")
	}
}

func TestAdapterTerminalMustBeFinal(t *testing.T) {
	attention := openStore(t, filepath.Join(t.TempDir(), "events.db"))
	ctx := context.Background()
	if _, err := attention.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_adapter_tail", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("initialize adapter tail connection: %v", err)
	}
	connection, err := attention.AcceptAdapterHello(ctx, "ses_adapter_tail", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept adapter tail hello: %v", err)
	}
	grantFence, err := attention.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("allocate adapter tail grant fence: %v", err)
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence}
	if _, err := attention.AppendAdapterEvents(ctx, "ses_adapter_tail", admission, []store.PendingEvent{
		{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ended"}`)},
		{Type: "session.message", Time: testTime(2), Payload: []byte(`{"role":"agent"}`)},
	}); err == nil {
		t.Fatal("terminal adapter event with tail committed")
	}
	if latest, err := attention.LatestSeq(ctx, "ses_adapter_tail"); err != nil || latest != 0 {
		t.Fatalf("terminal adapter tail left events latest=%d err=%v", latest, err)
	}
}
