package postgres_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/store"
)

func encryptedPendingEventOfType(t *testing.T, id, kind string) store.PendingEvent {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal(err)
	}
	content := json.RawMessage(`{"content":[{"kind":"text","text":"private pending"}]}`)
	projection := e2ee.PublicMetadata{}
	if kind == "permission.respond" {
		projection.RequestID = "approval"
		projection.Decision = "approve"
		content = json.RawMessage(`{"request_id":"approval","decision":"approve"}`)
	}
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "ses_pending_1", Sender: "client", KeyID: "key", MessageID: id, Type: kind}, key, private, projection, content)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": id, "type": kind, "packet": packet})
	if err != nil {
		t.Fatal(err)
	}
	return store.PendingEvent{Type: "session.command", Time: time.Now(), Payload: payload}
}

func TestEncryptedPendingCommandCommitListAndConflict(t *testing.T) {
	for _, kind := range []string{"session.send", "session.interrupt", "session.stop", "permission.respond", "session.settings.change", "session.membership.change"} {
		t.Run(kind, func(t *testing.T) { testEncryptedPendingCommandCommitListAndConflict(t, kind) })
	}
}

func testEncryptedPendingCommandCommitListAndConflict(t *testing.T, kind string) {
	h := newPostgresProposalHarness(t)
	ctx := context.Background()
	if _, err := h.pool.Exec(ctx, `INSERT INTO agent_sessions(id,status) VALUES('ses_pending_1','ready')`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_pending_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	connection, err := h.AcceptAdapterHello(ctx, "ses_pending_1", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := h.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	authority := store.CommandAuthority{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: 1}
	event := encryptedPendingEventOfType(t, "cmd", kind)
	expires := time.Now().Add(20 * time.Second)
	request := store.PendingCommandRequest{CommandID: "cmd", Type: kind, ExpiresAt: expires}
	committed, err := h.CommitEncryptedPendingCommand(ctx, "ses_pending_1", authority, event, request)
	if err != nil || committed.Duplicate || committed.Command.EventSeq != 1 {
		t.Fatalf("commit=%+v %v", committed, err)
	}
	listed, err := h.ListEncryptedPendingCommands(ctx, "ses_pending_1", authority)
	if err != nil || len(listed) != 1 || listed[0].CommandID != "cmd" {
		t.Fatalf("list=%+v %v", listed, err)
	}
	duplicate, err := h.CommitEncryptedPendingCommand(ctx, "ses_pending_1", authority, event, request)
	if err != nil || !duplicate.Duplicate || duplicate.Command.EventSeq != 1 {
		t.Fatalf("duplicate=%+v %v", duplicate, err)
	}
	conflict := encryptedPendingEventOfType(t, "cmd", kind)
	if _, err := h.CommitEncryptedPendingCommand(ctx, "ses_pending_1", authority, conflict, request); err == nil {
		t.Fatal("conflicting command accepted")
	}
	plain := store.PendingEvent{Type: "session.command", Time: time.Now(), Payload: []byte(`{"content":"plaintext"}`)}
	if _, err := h.CommitEncryptedPendingCommand(ctx, "ses_pending_1", authority, plain, store.PendingCommandRequest{CommandID: "plain", Type: "session.send", ExpiresAt: expires}); err == nil {
		t.Fatal("plaintext command accepted")
	}
	if _, err := h.ListPendingCommands(ctx, "ses_pending_1", authority); err == nil {
		t.Fatal("legacy listing accepted encrypted row")
	}
	if _, err := h.pool.Exec(ctx, `UPDATE session_events SET payload='{}' WHERE session_id='ses_pending_1' AND seq=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ClaimEncryptedPendingCommand(ctx, "ses_pending_1", authority, "cmd"); err == nil {
		t.Fatal("corrupt event claimed")
	}
	var persistedStatus string
	if err := h.pool.QueryRow(ctx, `SELECT status FROM session_pending_commands WHERE session_id='ses_pending_1' AND cmd_id='cmd'`).Scan(&persistedStatus); err != nil || persistedStatus != "pending" {
		t.Fatal("failed claim changed state", err)
	}
	if _, err := h.pool.Exec(ctx, `UPDATE session_events SET payload=$1 WHERE session_id='ses_pending_1' AND seq=1`, event.Payload); err != nil {
		t.Fatal(err)
	}
	claimed, err := h.ClaimEncryptedPendingCommand(ctx, "ses_pending_1", authority, "cmd")
	if err != nil || !claimed.Claimed || claimed.Command.Status != store.PendingCommandReceived {
		t.Fatalf("encrypted claim=%+v %v", claimed, err)
	}
	if _, err := h.pool.Exec(ctx, `UPDATE session_events SET payload='{}' WHERE session_id='ses_pending_1' AND seq=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ResolveEncryptedPendingCommand(ctx, "ses_pending_1", authority, "cmd", store.PendingCommandCompleted); err == nil {
		t.Fatal("corrupt event resolved")
	}
	if err := h.pool.QueryRow(ctx, `SELECT status FROM session_pending_commands WHERE session_id='ses_pending_1' AND cmd_id='cmd'`).Scan(&persistedStatus); err != nil || persistedStatus != "received" {
		t.Fatal("failed resolve changed state", err)
	}
	if _, err := h.pool.Exec(ctx, `UPDATE session_events SET payload=$1 WHERE session_id='ses_pending_1' AND seq=1`, event.Payload); err != nil {
		t.Fatal(err)
	}
	resolved, err := h.ResolveEncryptedPendingCommandUnknown(ctx, "ses_pending_1", "cmd")
	if err != nil || resolved.Status != store.PendingCommandOutcomeUnknown {
		t.Fatalf("encrypted resolve=%+v %v", resolved, err)
	}
	if _, err := h.ClaimEncryptedPendingCommand(ctx, "ses_pending_1", authority, "cmd"); err == nil {
		t.Fatal("resolved encrypted command reclaimed")
	}
	_ = fence
}
