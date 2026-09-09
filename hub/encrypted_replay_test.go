package hub

import (
	"encoding/base64"
	"encoding/json"
	"testing"

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
