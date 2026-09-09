package hub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"nhooyr.io/websocket"
)

func TestRequiredReplayOverWebSocket(t *testing.T) {
	for _, valid := range []bool{true, false} {
		name := "opaque"
		if !valid {
			name = "plaintext_rejected"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
			payload, err := json.Marshal(protocol.EncryptedPacketCarrier{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: "message", Type: "session.state", Packet: protocol.OpaqueContentPacket{Version: 1, Public: protocol.EncryptedProjection{State: "ready"}, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}})
			if err != nil {
				t.Fatal(err)
			}
			if !valid {
				payload = []byte(`{"state":"ready","private":"synthetic-replay-canary"}`)
			}
			handler := &webSocketHandler{events: pendingReplayStore{event: store.Event{SessionID: "session", Seq: 1, Type: "session.state", Time: time.Now(), Payload: payload}}}
			done := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := acceptManagedConn(w, r)
				if err != nil {
					done <- err
					return
				}
				defer conn.Close(websocket.StatusNormalClosure, "")
				subscriptions := []protocol.Subscription{{SessionID: "session"}}
				peer := newClientConnection(conn, protocol.ProtocolVersionV2, subscriptions, true, nil)
				done <- handler.replayAccepted(ctx, peer, AcceptedPeer{Role: protocol.RoleClient, ContentMode: protocol.ContentModeRequired, Subscribed: subscriptions, Admissions: map[string]auth.SessionAdmissionDecision{"session": {Mode: auth.SessionAdmissionCurrent}}})
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
			if strings.Contains(string(data), "synthetic-replay-canary") {
				t.Fatal("plaintext left replay boundary")
			}
			frame, err := protocol.Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			if valid {
				event, ok := frame.(*protocol.Event)
				if !ok || event.Seq == nil || *event.Seq != 1 || string(event.Payload) != string(payload) {
					t.Fatalf("changed event: %T", frame)
				}
			} else {
				failure, ok := frame.(*protocol.Error)
				if !ok || failure.Code != "replay_failed" || failure.Message != "session replay failed" || !failure.Fatal {
					t.Fatalf("missing fixed failure: %T", frame)
				}
			}
			if err := <-done; (err == nil) != valid {
				t.Fatalf("replay result: %v", err)
			}
		})
	}
}
