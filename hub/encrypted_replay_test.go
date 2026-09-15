package hub

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestRequiredReplayRejectsPlaintextAndRoutingMismatch(t *testing.T) {
	encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	for _, kind := range []string{"session.state", "session.send", "session.stop", "session.settings.change"} {
		t.Run(kind, func(t *testing.T) {
			wire := protocol.EncryptedPacketCarrier{Version: 1, Scope: "command", KeyID: "key", Sender: "sender", MessageID: "message", Type: kind, Packet: protocol.OpaqueContentPacket{Version: 1, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}}
			event := store.Event{SessionID: "session", Seq: 1, Type: "session.command"}
			if kind == "session.state" {
				wire.Scope = "event"
				wire.Packet.Public.State = "ready"
				event.Type = kind
			}
			payload, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			event.Payload = payload
			if err := validateRequiredReplayEvent(event, "session"); err != nil {
				t.Fatal(err)
			}
			if err := validateRequiredReplayEvent(event, "other"); err == nil {
				t.Fatal("cross-session replay accepted")
			}
			event.Payload = []byte(`{"content":"synthetic plaintext"}`)
			if err := validateRequiredReplayEvent(event, "session"); err == nil {
				t.Fatal("plaintext replay accepted")
			}
			event.Payload = payload
			event.Type = "session.message"
			if err := validateRequiredReplayEvent(event, "session"); err == nil {
				t.Fatal("type substitution accepted")
			}
		})
	}
}

func TestDurableSessionStateReadsPlaintextAndSealedProjection(t *testing.T) {
	if state, ok := durableSessionState([]byte(`{"state":"busy"}`)); !ok || state != "busy" {
		t.Fatalf("plaintext state = %q ok=%v", state, ok)
	}
	if _, ok := durableSessionState([]byte(`{"nope":1}`)); ok {
		t.Fatal("unknown plaintext state accepted")
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"state":"ended"}`)
	projection, err := e2ee.ProjectPublicMetadata("session.state", payload)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "event", Session: "session", Sender: "machine", KeyID: "key", MessageID: "message", Type: "session.state"}, make([]byte, 32), private, projection, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(e2ee.EventWire{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: "message", Type: "session.state", Packet: packet})
	if err != nil {
		t.Fatal(err)
	}
	if state, ok := durableSessionState(wire); !ok || state != "ended" {
		t.Fatalf("sealed state = %q ok=%v", state, ok)
	}
}

func TestEncryptedRunControlOutcomeMapping(t *testing.T) {
	for value, want := range map[string]store.RunControlOutcome{
		"completed":       store.RunControlCompleted,
		"rejected":        store.RunControlRejected,
		"timeout":         store.RunControlTimeout,
		"outcome_unknown": store.RunControlOutcomeUnknown,
	} {
		got, err := encryptedRunControlOutcome(value)
		if err != nil || got != want {
			t.Fatalf("outcome %q = %q err=%v, want %q", value, got, err, want)
		}
	}
	if _, err := encryptedRunControlOutcome("bogus"); err == nil {
		t.Fatal("unknown run-control outcome accepted")
	}
}

func TestRequiredReplayAllowsOnlyTheStoreRecoveryOutcomeAsPlaintext(t *testing.T) {
	event := store.Event{SessionID: "session", Seq: 1, Type: "session.run.outcome", Payload: []byte(`{"cmd_id":"cmd","operation":"stop","outcome":"outcome_unknown","completion_state":null,"reason_code":"recovery_unconfirmed"}`)}
	if err := validateRequiredReplayEvent(event, "session"); err != nil {
		t.Fatalf("plaintext recovery outcome rejected: %v", err)
	}
	event.Type = "session.state"
	event.Payload = []byte(`{"state":"ready"}`)
	if err := validateRequiredReplayEvent(event, "session"); err == nil {
		t.Fatal("plaintext session state accepted")
	}
}
