package hub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

func TestEncryptedFileCommandsUseCurrentSessionAdmission(t *testing.T) {
	for _, kind := range []protocol.CommandType{protocol.CommandFileRead, protocol.CommandFileList} {
		if action := commandAdmissionAction(kind); action != "send" {
			t.Fatalf("%s admission action = %q", kind, action)
		}
	}
}

func TestEncryptedDurableControlDeliveryOverWebSocket(t *testing.T) {
	for _, kind := range []string{"session.send", "session.interrupt", "session.stop", "permission.respond", "session.settings.change", "session.membership.change", "session.file.read", "session.file.list"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
			grantFence, err := backend.AllocateAdapterGrantFence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
			carrier := protocol.EncryptedPacketCarrier{Version: 1, Scope: "command", KeyID: "key", Sender: "client", MessageID: "command", Type: kind, Packet: protocol.OpaqueContentPacket{Version: 1, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}}
			if kind == "permission.respond" {
				carrier.Packet.Public = protocol.EncryptedProjection{RequestID: "approval", Decision: "approve"}
			}
			payload, err := json.Marshal(carrier)
			if err != nil {
				t.Fatal(err)
			}
			command := &protocol.Command{SessionID: "session", CommandID: "command", Type: protocol.CommandType(kind), Payload: payload}
			done := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := acceptManagedConn(w, r)
				if err != nil {
					done <- err
					return
				}
				defer conn.CloseNow()
				adapter := &adapterConnection{conn: conn, writeGate: newContextWriteGate(), sessionID: "session", contentMode: protocol.ContentModeRequired, admission: store.AdapterConnectionAdmission{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: 1, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence}}
				handler := &webSocketHandler{events: backend, adapterAuthority: &adapterDispatchAuthority{store: backend}, adapters: map[string]*adapterConnection{"session": adapter}, pendingCommandClients: make(map[string]*pendingCommandClient), acceptedCommands: make(map[string]struct{})}
				adapter.handler = handler
				err = handler.handleDurableSessionSendMode(ctx, nil, nil, command, backend, true)
				if err != nil {
					done <- err
					return
				}
				_, data, err := conn.Read(ctx)
				if err != nil {
					done <- err
					return
				}
				frame, err := protocol.Decode(data)
				if err != nil {
					done <- err
					return
				}
				ack, ok := frame.(*protocol.CommandAck)
				if !ok {
					done <- context.Canceled
					return
				}
				key := settingsCommandKey("session", "command")
				handler.commandMu.Lock()
				pending := handler.pendingCommandClients[key]
				handler.commandMu.Unlock()
				if err := handler.handlePendingCommandAck(ctx, adapter, ack, key, pending); err != nil {
					done <- err
					return
				}
				// A cached command ID must not acknowledge a different ciphertext.
				changedCarrier := carrier
				changedCarrier.Packet.Encrypted.Ciphertext = base64.RawURLEncoding.EncodeToString(make([]byte, 17))
				changedPayload, err := json.Marshal(changedCarrier)
				if err != nil {
					done <- err
					return
				}
				changed := *command
				changed.Payload = changedPayload
				err = handler.handleDurableSessionSendMode(ctx, conn, nil, &changed, backend, true)
				if err == nil {
					done <- fmt.Errorf("cached ID bypassed durable conflict check")
					return
				}
				done <- nil
			}))
			defer server.Close()
			client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseNow()
			_, data, err := client.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := protocol.Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			delivered, ok := frame.(*protocol.Command)
			if !ok || delivered.Type != command.Type || string(delivered.Payload) != string(payload) {
				t.Fatal("changed command on transport")
			}
			persisted := 0
			if err := backend.Replay(ctx, "session", 0, func(event store.Event) error {
				persisted++
				if event.Type != "session.command" || event.Seq != 1 || string(event.Payload) != string(delivered.Payload) {
					t.Error("network command differs from durable event")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if persisted != 1 {
				t.Fatalf("delivery has %d durable events", persisted)
			}
			ack, err := protocol.Encode(&protocol.CommandAck{CommandID: "command", Status: protocol.AckAccepted})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Write(ctx, websocket.MessageText, ack); err != nil {
				t.Fatal(err)
			}
			_, rejectionData, err := client.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rejectionFrame, err := protocol.Decode(rejectionData)
			if err != nil {
				t.Fatal(err)
			}
			rejection, ok := rejectionFrame.(*protocol.CommandAck)
			if !ok || rejection.Status != protocol.AckRejected || rejection.Reason != "persist_failed" {
				t.Fatal("conflicting retry was not rejected")
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if _, err := backend.ClaimEncryptedPendingCommand(ctx, "session", store.CommandAuthority{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: 1}, "command"); err == nil {
				t.Fatal("acknowledged control reclaimed")
			}
		})
	}
}
