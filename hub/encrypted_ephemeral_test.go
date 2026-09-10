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

func TestRequiredEphemeralForwardingDoesNotPersist(t *testing.T) {
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
	fence, err := backend.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &adapterConnection{sessionID: "session", contentMode: protocol.ContentModeRequired, admission: store.AdapterConnectionAdmission{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: 1, AcceptedFence: connection.AcceptedFence, GrantFence: fence}}
	peer := newClientConnection(nil, 2, []protocol.Subscription{{SessionID: "session"}}, true, nil)
	peer.contentMode = protocol.ContentModeRequired
	handler := &webSocketHandler{events: backend, adapterAuthority: &adapterDispatchAuthority{store: backend}, adapters: map[string]*adapterConnection{"session": adapter}, subscribers: map[string]map[*clientConnection]struct{}{"session": {peer: {}}}}
	adapter.handler = handler
	encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	for _, kind := range []string{"agent.activity", "log.tail", "resource.sample", "presence"} {
		carrier := protocol.EncryptedPacketCarrier{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: kind, Type: kind, Packet: protocol.OpaqueContentPacket{Version: 1, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}}
		payload, err := json.Marshal(carrier)
		if err != nil {
			t.Fatal(err)
		}
		if err := handler.handleAdapterEvent(ctx, adapter, AcceptedPeer{SessionID: "session", ProtocolVersion: 2, ContentMode: protocol.ContentModeRequired}, &protocol.Event{SessionID: "session", Type: kind, Time: 1, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	buffered := peer.subscriptions["session"].buffered
	if len(buffered) != 4 {
		t.Fatalf("forwarded %d events", len(buffered))
	}
	for _, event := range buffered {
		if event.Seq != nil || event.ProposalID != "" {
			t.Fatal("ephemeral assigned durable identity")
		}
	}
	seq, err := backend.LatestSeq(ctx, "session")
	if err != nil || seq != 0 {
		t.Fatalf("ephemeral persisted: %d %v", seq, err)
	}
}
