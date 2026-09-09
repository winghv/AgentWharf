package hub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func TestRequiredLiveRejectsPlaintextBeforeReplayBuffer(t *testing.T) {
	peer := newClientConnection(nil, protocol.ProtocolVersionV2, []protocol.Subscription{{SessionID: "session"}}, true, nil)
	peer.contentMode = protocol.ContentModeRequired
	seq := int64(1)
	event := protocol.Event{SessionID: "session", Seq: &seq, Type: "session.state", Payload: []byte(`{"state":"ready","detail":"synthetic private content"}`)}
	if err := peer.sendLiveEvent(context.Background(), event); err == nil {
		t.Fatal("plaintext accepted into live buffer")
	}
	if len(peer.subscriptions["session"].buffered) != 0 {
		t.Fatal("plaintext retained")
	}
	for _, kind := range []string{"log.tail", "agent.activity", "resource.sample", "presence"} {
		if err := peer.sendLiveEvent(context.Background(), protocol.Event{SessionID: "session", Type: kind, Payload: []byte(`{"detail":"synthetic private content"}`)}); err == nil {
			t.Fatalf("unsequenced %s escaped encrypted boundary", kind)
		}
	}
	if len(peer.subscriptions["session"].buffered) != 0 {
		t.Fatal("ephemeral plaintext retained")
	}
	encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	payload, err := json.Marshal(protocol.EncryptedPacketCarrier{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: "event", Type: "session.state", Packet: protocol.OpaqueContentPacket{Version: 1, Public: protocol.EncryptedProjection{State: "ready"}, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}})
	if err != nil {
		t.Fatal(err)
	}
	event.Payload = payload
	if err := peer.sendLiveEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	buffered := peer.subscriptions["session"].buffered
	if len(buffered) != 1 || string(buffered[0].Payload) != string(payload) {
		t.Fatal("opaque live event changed")
	}
	legacy := newClientConnection(nil, protocol.ProtocolVersionV2, []protocol.Subscription{{SessionID: "session"}}, true, nil)
	event.Payload = []byte(`{"state":"ready"}`)
	if err := legacy.sendLiveEvent(context.Background(), event); err != nil {
		t.Fatal("legacy live behavior changed", err)
	}
}
