package protocol_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func TestHelloAckContentModeRoundTrip(t *testing.T) {
	for _, mode := range []string{"", protocol.ContentModeLegacy, protocol.ContentModeRequired} {
		encoded, err := protocol.Encode(&protocol.HelloAck{ProtocolVersion: 2, ContentMode: mode, Sessions: []protocol.SessionSummary{{SessionID: "session"}}})
		if err != nil {
			t.Fatal(err)
		}
		frame, err := protocol.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if frame.(*protocol.HelloAck).ContentMode != mode {
			t.Fatal("content mode lost")
		}
		if mode == "" && bytes.Contains(encoded, []byte("content_mode")) {
			t.Fatal("legacy omission changed")
		}
	}
}

func TestEncryptedContentProposedEncoding(t *testing.T) {
	encode := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	payload := protocol.EncryptedContentPayload{Version: 1, Scope: "command", KeyID: "key_1", Sender: "device_1", MessageID: "cmd_1", Type: "session.send", Nonce: encode(12), Ciphertext: encode(16), Signature: encode(64)}
	wire, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.DecodeEncryptedContentPayload(wire)
	if err != nil || decoded != payload {
		t.Fatalf("roundtrip: %v", err)
	}
	invalid := map[string][]byte{
		"duplicate": bytes.Replace(wire, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"trailing":  append(append([]byte(nil), wire...), []byte(`{}`)...),
		"unknown":   bytes.Replace(wire, []byte(`"version":1`), []byte(`"version":1,"extra":true`), 1),
		"null":      bytes.Replace(wire, []byte(`"version":1`), []byte(`"version":null`), 1),
		"scope":     bytes.Replace(wire, []byte(`"command"`), []byte(`"invalid"`), 1),
		"huge":      []byte(strings.Repeat(" ", 48*1024+1)),
	}
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := protocol.DecodeEncryptedContentPayload(data); err == nil {
				t.Fatal("accepted invalid input")
			}
		})
	}
	for _, bad := range []string{encode(11), encode(13), encode(12) + "=", encode(12) + "\n"} {
		changed := payload
		changed.Nonce = bad
		data, _ := json.Marshal(changed)
		if _, err := protocol.DecodeEncryptedContentPayload(data); err == nil {
			t.Fatal("accepted invalid nonce")
		}
	}
	changed := payload
	changed.Signature = encode(64)[:85] + "B"
	data, _ := json.Marshal(changed)
	if _, err := protocol.DecodeEncryptedContentPayload(data); err == nil {
		t.Fatal("accepted noncanonical pad bits")
	}
}
