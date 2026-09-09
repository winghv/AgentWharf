package e2ee

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
)

func TestCommandWireBindsRoutingToSignedContext(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	ctx := Context{Scope: "command", Session: "session", Sender: "client", KeyID: "key", MessageID: "command", Type: "session.send"}
	packet, err := SealPacket(ctx, key, private, PublicMetadata{}, json.RawMessage(`{"content":[{"kind":"text","text":"synthetic canary"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	wire := map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "command", "type": "session.send", "packet": packet}
	encoded, _ := json.Marshal(wire)
	decoded, p, err := DecodeCommandWire("session", "command", "session.send", encoded)
	if err != nil || decoded != ctx {
		t.Fatalf("context=%+v error=%v", decoded, err)
	}
	if _, err = OpenPacket(decoded, key, public, p); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"sender", "key_id", "message_id", "type", "scope"} {
		t.Run(field, func(t *testing.T) {
			changed := map[string]any{}
			for k, v := range wire {
				changed[k] = v
			}
			changed[field] = "substitute"
			raw, _ := json.Marshal(changed)
			c, p, err := DecodeCommandWire("session", "command", "session.send", raw)
			if err == nil {
				_, err = OpenPacket(c, key, public, p)
			}
			if err == nil {
				t.Fatal("substituted context accepted")
			}
		})
	}
	c, p, err := DecodeCommandWire("other-session", "command", "session.send", encoded)
	if err == nil {
		_, err = OpenPacket(c, key, public, p)
	}
	if err == nil {
		t.Fatal("cross session accepted")
	}
	for _, raw := range [][]byte{append(encoded, []byte(`{}`)...), append([]byte(`{"scope":"command",`), encoded[1:]...)} {
		if _, _, err := DecodeCommandWire("session", "command", "session.send", raw); err == nil {
			t.Fatal("malformed wire accepted")
		}
	}
}
