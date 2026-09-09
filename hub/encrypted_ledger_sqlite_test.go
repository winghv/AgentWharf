package hub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
	"nhooyr.io/websocket"
)

func TestHubEncryptedLedgerWithSQLite(t *testing.T) {
	for _, kind := range []string{"session.send", "session.interrupt", "session.stop", "permission.respond", "session.settings.change"} {
		for _, status := range []protocol.AckStatus{protocol.AckAccepted, protocol.AckDuplicate, protocol.AckRejected} {
			t.Run(kind+"/"+string(status), func(t *testing.T) { testHubEncryptedLedgerWithSQLite(t, kind, status, "") })
		}
	}
	for _, reason := range []string{"invalid_file_request", "file_unavailable", "private_adapter_detail"} {
		t.Run("session.file.read/rejected/"+reason, func(t *testing.T) {
			testHubEncryptedLedgerWithSQLite(t, "session.file.read", protocol.AckRejected, reason)
		})
	}
}

func testHubEncryptedLedgerWithSQLite(t *testing.T, kind string, status protocol.AckStatus, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
	serverConn := make(chan *managedConn, 1)
	releaseServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, acceptErr := acceptManagedConn(w, r)
		if acceptErr != nil {
			return
		}
		serverConn <- conn
		<-releaseServer
		_ = conn.CloseNow()
	}))
	clientConn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		close(releaseServer)
		server.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = clientConn.CloseNow()
		close(releaseServer)
		server.Close()
	}()
	peer := newClientConnection(<-serverConn, protocol.ProtocolVersionV2, nil, false, nil)
	client := &pendingCommandClient{adapter: adapter, peer: peer, commandID: "command", commandType: protocol.CommandType(kind)}
	handler.adapters = map[string]*adapterConnection{"session": adapter}
	handler.pendingCommandClients = map[string]*pendingCommandClient{"pending": client}
	handler.acceptedCommands = make(map[string]struct{})
	err = handler.handlePendingCommandAck(ctx, adapter, &protocol.CommandAck{CommandID: "command", Status: status, Reason: reason}, "pending", client)
	terminallyRejected := status == protocol.AckRejected && (reason == "invalid_file_request" || reason == "file_unavailable")
	if (err != nil) != (status == protocol.AckRejected && !terminallyRejected) {
		t.Fatalf("ack result: %v", err)
	}
	_, ackData, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.Decode(ackData)
	if err != nil {
		t.Fatal(err)
	}
	clientAck, ok := frame.(*protocol.CommandAck)
	if !ok {
		t.Fatalf("client acknowledgement = %T", frame)
	}
	wantStatus, wantReason := protocol.AckAccepted, ""
	if status == protocol.AckRejected {
		wantStatus = protocol.AckRejected
		wantReason = "adapter_delivery_failed"
		if terminallyRejected {
			wantReason = reason
		}
	}
	if clientAck.Status != wantStatus || clientAck.Reason != wantReason {
		t.Fatalf("client acknowledgement = %s %q, want %s %q", clientAck.Status, clientAck.Reason, wantStatus, wantReason)
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
	pending, listErr := ledger.ListPendingCommands(ctx, "session", authority)
	if listErr != nil || len(pending) != 0 {
		t.Fatalf("resolved command remained replayable: pending=%v err=%v", pending, listErr)
	}
	summaries, summaryErr := backend.AttentionSnapshot(ctx, []string{"session"})
	if summaryErr != nil || len(summaries) != 1 {
		t.Fatalf("attention snapshot: summaries=%v err=%v", summaries, summaryErr)
	}
	if terminallyRejected {
		if summaries[0].Blocker != nil && summaries[0].Blocker.Kind == store.AttentionBlockerOutcomeUnknown {
			t.Fatal("terminal rejection created an outcome-unknown blocker")
		}
	} else if status == protocol.AckRejected && reason == "private_adapter_detail" {
		if summaries[0].Blocker == nil || summaries[0].Blocker.Kind != store.AttentionBlockerOutcomeUnknown {
			t.Fatalf("private rejection did not resolve outcome unknown: %+v", summaries[0].Blocker)
		}
	}

}
