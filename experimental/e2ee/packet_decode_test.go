package e2ee

import (
	"crypto/ed25519"
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeContentPacketNetworkBoundary(t *testing.T) {
	private := ed25519.NewKeyFromSeed(make([]byte, 32))
	ctx := Context{"event", "session", "device", "key", "message", "session.state"}
	packet, err := SealPacket(ctx, make([]byte, 32), private, PublicMetadata{State: "busy"}, json.RawMessage(`{"state":"busy"}`))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(packet)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeContentPacket(ctx.Type, wire)
	if err != nil || decoded != packet {
		t.Fatal("round trip", err)
	}
	for _, bad := range []string{
		strings.Replace(string(wire), `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(string(wire), `"state":"busy"`, `"state":"busy","st\u0061te":"ready"`, 1),
		strings.Replace(string(wire), `"version":1`, `"version":null`, 1),
		strings.Replace(string(wire), `"version":1`, `"version":2`, 1),
		strings.Replace(string(wire), `"version":1`, `"version":1,"extra":true`, 1),
		string(wire) + `{}`, strings.Repeat(" ", 48*1024) + string(wire),
	} {
		if _, err := DecodeContentPacket(ctx.Type, []byte(bad)); err == nil {
			t.Fatal("accepted malformed wire")
		}
	}
}
