package sqlite_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestEncryptedPendingLifecycleSQLite(t *testing.T) {
	for _, kind := range []string{"session.send", "session.interrupt", "session.stop", "permission.respond", "session.settings.change", "session.membership.change"} {
		t.Run(kind, func(t *testing.T) { testEncryptedPendingLifecycleSQLite(t, kind) })
	}
}

func testEncryptedPendingLifecycleSQLite(t *testing.T, kind string) {
	ctx := context.Background()
	s := openStore(t, filepath.Join(t.TempDir(), "pending.db"))
	if _, err := s.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "session", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	connection, err := s.AcceptAdapterHello(ctx, "session", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	authority := store.CommandAuthority{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: 1}
	encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	// Store validates structure only. These synthetic bytes confer no endpoint authority.
	wire := protocol.EncryptedPacketCarrier{Version: 1, Scope: "command", KeyID: "key", Sender: "client", MessageID: "cmd", Type: kind, Packet: protocol.OpaqueContentPacket{Version: 1, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}}
	if kind == "permission.respond" {
		wire.Packet.Public = protocol.EncryptedProjection{RequestID: "approval", Decision: "approve"}
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	event := store.PendingEvent{Type: "session.command", Time: time.Now(), Payload: payload}
	request := store.PendingCommandRequest{CommandID: "cmd", Type: kind, ExpiresAt: time.Now().Add(20 * time.Second)}
	if _, err = s.CommitEncryptedPendingCommand(ctx, "session", authority, event, request); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimEncryptedPendingCommand(ctx, "session", authority, "cmd")
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v %v", claim, err)
	}
	duplicate, err := s.ClaimEncryptedPendingCommand(ctx, "session", authority, "cmd")
	if err != nil || duplicate.Claimed {
		t.Fatalf("duplicate=%+v %v", duplicate, err)
	}
	resolved, err := s.ResolveEncryptedPendingCommand(ctx, "session", authority, "cmd", store.PendingCommandCompleted)
	if err != nil || resolved.Status != store.PendingCommandCompleted {
		t.Fatalf("resolve=%+v %v", resolved, err)
	}
	if _, err = s.ClaimEncryptedPendingCommand(ctx, "session", authority, "cmd"); err == nil {
		t.Fatal("completed command reclaimed")
	}
}
