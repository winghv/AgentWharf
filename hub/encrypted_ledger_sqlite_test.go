package hub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
)

func TestHubEncryptedLedgerWithSQLite(t *testing.T) {
	for _, kind := range []string{"session.send", "session.interrupt", "session.stop", "permission.respond", "session.settings.change"} {
		for _, status := range []protocol.AckStatus{protocol.AckAccepted, protocol.AckDuplicate, protocol.AckRejected} {
			t.Run(kind+"/"+string(status), func(t *testing.T) { testHubEncryptedLedgerWithSQLite(t, kind, status) })
		}
	}
}

func testHubEncryptedLedgerWithSQLite(t *testing.T, kind string, status protocol.AckStatus) {
	ctx := context.Background()
	backend, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if _, err := backend.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "session", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	connection, err := backend.AcceptAdapterHello(ctx, "session", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	authority := store.CommandAuthority{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: 1}
	encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	carrier := protocol.EncryptedPacketCarrier{Version: 1, Scope: "command", KeyID: "key", Sender: "client", MessageID: "command", Type: kind, Packet: protocol.OpaqueContentPacket{Version: 1, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}}
	if kind == "permission.respond" {
		carrier.Packet.Public = protocol.EncryptedProjection{RequestID: "approval", Decision: "approve"}
	}
	payload, err := json.Marshal(carrier)
	if err != nil {
		t.Fatal(err)
	}
	var ledger store.CommandLedgerStore = encryptedCommandLedger{backend}
	if _, err := ledger.CommitPendingCommand(ctx, "session", authority, store.PendingEvent{Type: "session.command", Time: time.Now(), Payload: payload}, store.PendingCommandRequest{CommandID: "command", Type: kind, ExpiresAt: time.Now().Add(20 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	pending, err := ledger.ListPendingCommands(ctx, "session", authority)
	if err != nil || len(pending) != 1 {
		t.Fatalf("encrypted queue missing: %v %v", pending, err)
	}
	handler := &webSocketHandler{events: backend}
	command, err := handler.commandFromPendingEvent(ctx, pending[0], true)
	if err != nil || string(command.Payload) != string(payload) || string(command.Type) != kind {
		t.Fatal("carrier changed during replay", err)
	}
	claim, err := ledger.ClaimPendingCommand(ctx, "session", authority, "command")
	if err != nil || !claim.Claimed {
		t.Fatal("claim failed", err)
	}
	adapter := &adapterConnection{sessionID: "session", contentMode: protocol.ContentModeRequired, admission: store.AdapterConnectionAdmission{ConnectionEpoch: authority.ConnectionEpoch, CredentialGeneration: authority.CredentialGeneration}}
	client := &pendingCommandClient{adapter: adapter, commandID: "command"}
	handler.adapters = map[string]*adapterConnection{"session": adapter}
	handler.pendingCommandClients = map[string]*pendingCommandClient{"pending": client}
	handler.acceptedCommands = make(map[string]struct{})
	err = handler.handlePendingCommandAck(ctx, adapter, &protocol.CommandAck{CommandID: "command", Status: status}, "pending", client)
	if (err != nil) != (status == protocol.AckRejected) {
		t.Fatalf("ack result: %v", err)
	}
	if _, err := ledger.ClaimPendingCommand(ctx, "session", authority, "command"); err == nil {
		t.Fatal("completed duplicate command reclaimed")
	}
	if len(handler.pendingCommandClients) != 0 {
		t.Fatal("ack retained pending waiter")
	}
	if _, ok := handler.acceptedCommands["command"]; ok != (status != protocol.AckRejected) {
		t.Fatal("incorrect accepted-command state")
	}

}
