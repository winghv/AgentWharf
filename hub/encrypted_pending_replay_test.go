package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

type pendingReplayStore struct {
	store.EventStore
	event store.Event
}

func (s pendingReplayStore) Replay(ctx context.Context, session string, after int64, visit func(store.Event) error) error {
	return visit(s.event)
}

func TestEncryptedPendingReplayValidatesTransportIdentity(t *testing.T) {
	for _, kind := range []string{"session.send", "session.interrupt", "session.stop", "permission.respond", "session.settings.change"} {
		t.Run(kind, func(t *testing.T) { testEncryptedPendingReplay(t, kind) })
	}
}
func testEncryptedPendingReplay(t *testing.T, kind string) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	public := e2ee.PublicMetadata{}
	payload := json.RawMessage(`{"content":[]}`)
	if kind == "permission.respond" {
		public.RequestID = "approval"
		public.Decision = "approve"
		payload = json.RawMessage(`{"request_id":"approval","decision":"approve"}`)
	}
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "command", Type: kind}, key, private, public, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "command", "type": kind, "packet": packet})
	if err != nil {
		t.Fatal(err)
	}
	handler := &webSocketHandler{events: pendingReplayStore{event: store.Event{SessionID: "session", Seq: 1, Type: "session.command", Payload: wire}}}
	pending := store.PendingCommand{SessionID: "session", CommandID: "command", Type: kind, EventSeq: 1}
	command, err := handler.commandFromPendingEvent(context.Background(), pending, true)
	if err != nil || string(command.Payload) != string(wire) || string(command.Type) != kind {
		t.Fatal("valid carrier changed", err)
	}
	if err := validateClientCommandMode(&command, protocol.ContentModeRequired); err != nil {
		t.Fatal("encrypted command validation", err)
	}
	plain := command
	plain.Payload = []byte(`{}`)
	if err := validateClientCommandMode(&plain, protocol.ContentModeRequired); err == nil {
		t.Fatal("plaintext admitted")
	}
	for _, field := range []string{"id", "type"} {
		changed := pending
		if field == "id" {
			changed.CommandID = "other"
		} else {
			changed.Type = "unsupported"
		}
		if _, err := handler.commandFromPendingEvent(context.Background(), changed, true); err == nil {
			t.Fatal("mismatched reference replayed")
		}
	}
	handler.events = pendingReplayStore{event: store.Event{SessionID: "session", Seq: 1, Type: "session.command", Payload: []byte(`{"text":"plaintext"}`)}}
	if _, err := handler.commandFromPendingEvent(context.Background(), pending, true); err == nil {
		t.Fatal("plaintext carrier replayed")
	}
}
