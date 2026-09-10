package e2ee

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
)

func TestPacketBindsControlProjection(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	ctx := Context{"event", "session", "sender", "key", "event_1", "session.state"}
	payload := json.RawMessage(`{"state":"busy","metadata":{"title":"synthetic private title"}}`)
	packet, err := SealPacket(ctx, key, private, PublicMetadata{State: "busy"}, payload)
	if err != nil {
		t.Fatal(err)
	}
	result, err := OpenPacket(ctx, key, public, packet)
	if err != nil || string(result) != string(payload) {
		t.Fatal("payload changed", err)
	}
	changed := packet
	changed.Public.State = "ready"
	if _, err := OpenPacket(ctx, key, public, changed); err == nil {
		t.Fatal("accepted substituted state")
	}
	changed = packet
	changed.Version = 0
	if _, err := OpenPacket(ctx, key, public, changed); err == nil {
		t.Fatal("accepted downgraded version")
	}
	ctx.Type = "permission.request"
	packet, err = SealPacket(ctx, key, private, PublicMetadata{RequestID: "permission_1"}, json.RawMessage(`{"request_id":"permission_1","summary":"synthetic private command"}`))
	if err != nil {
		t.Fatal(err)
	}
	changed = packet
	changed.Public.RequestID = "permission_2"
	if _, err := OpenPacket(ctx, key, public, changed); err == nil {
		t.Fatal("accepted substituted permission ID")
	}
	// An authenticated TS sender may emit literal HTML characters and a
	// different JSON field order. This must not require Go byte serialization.
	ctx.Type = "session.state"
	sealed, err := Seal(ctx, key, private, []byte(`{"payload":{"state":"busy","text":"<literal>"},"public":{"state":"busy"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPacket(ctx, key, public, ContentPacket{1, PublicMetadata{State: "busy"}, sealed}); err != nil {
		t.Fatal("rejected valid foreign JSON encoding", err)
	}
	for _, raw := range []string{`{"public":{"state":"busy","state":"busy"},"payload":{}}`, `{"public":{"state":"busy"},"payload":{},"payload":{}}`, `{"public":{"state":"busy"},"payload":{}} {}`} {
		sealed, err := Seal(ctx, key, private, []byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := OpenPacket(ctx, key, public, ContentPacket{1, PublicMetadata{State: "busy"}, sealed}); err == nil {
			t.Fatal("accepted ambiguous protected container")
		}
	}
	if _, err := SealPacket(ctx, key, private, PublicMetadata{State: "ready"}, json.RawMessage(`{"state":"busy"}`)); err == nil {
		t.Fatal("accepted caller projection mismatch")
	}
	inconsistent, err := Seal(ctx, key, private, []byte(`{"public":{"state":"ready"},"payload":{"state":"busy"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPacket(ctx, key, public, ContentPacket{1, PublicMetadata{State: "ready"}, inconsistent}); err == nil {
		t.Fatal("accepted authenticated but inconsistent projection")
	}
	ctx.Type = "session.tool_call"
	if _, err := SealPacket(ctx, key, private, PublicMetadata{Role: "agent"}, payload); err == nil {
		t.Fatal("unexpected public metadata accepted")
	}
}
