package protocol_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func TestEncryptedCarrierAcceptsRealEndpointPacketsWithoutPlaintext(t *testing.T) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ scope, kind, payload string }{
		{"command", "session.send", `{"content":[{"kind":"text","text":"private canary"}]}`},
		{"event", "session.message", `{"role":"agent","text":"private canary"}`},
		{"event", "session.state", `{"state":"ready","reason":"private canary"}`},
		{"event", "permission.request", `{"request_id":"request","action":"private canary"}`},
		{"command", "permission.respond", `{"request_id":"request","decision":"approve"}`},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			ctx := e2ee.Context{Scope: tc.scope, Session: "session", Sender: "device", KeyID: "key", MessageID: "message", Type: tc.kind}
			projection, err := e2ee.ProjectPublicMetadata(tc.kind, json.RawMessage(tc.payload))
			if err != nil {
				t.Fatal(err)
			}
			packet, err := e2ee.SealPacket(ctx, key, private, projection, json.RawMessage(tc.payload))
			if err != nil {
				t.Fatal(err)
			}
			wire, err := json.Marshal(map[string]any{"version": 1, "scope": tc.scope, "key_id": "key", "sender": "device", "message_id": "message", "type": tc.kind, "packet": packet})
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(wire, []byte("private canary")) {
				t.Fatal("plaintext present")
			}
			id := ""
			if tc.scope == "command" {
				id = "message"
			}
			decoded, err := protocol.DecodeEncryptedPacketCarrier(wire, tc.scope, tc.kind, id)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Packet.Encrypted.Ciphertext != packet.Encrypted.Ciphertext || decoded.MessageID != "message" {
				t.Fatal("ciphertext or context changed")
			}
			roundtrip, err := json.Marshal(decoded.Packet)
			if err != nil {
				t.Fatal(err)
			}
			endpointPacket, err := e2ee.DecodeContentPacket(tc.kind, roundtrip)
			if err != nil {
				t.Fatal(err)
			}
			plaintext, err := e2ee.OpenPacket(ctx, key, private.Public().(ed25519.PublicKey), endpointPacket)
			if err != nil || string(plaintext) != tc.payload {
				t.Fatal("protocol translation broke endpoint authentication", err)
			}
			for _, invalid := range [][]byte{
				append(append([]byte(nil), wire...), []byte(`{}`)...),
				bytes.Replace(wire, []byte(`"key_id":"key"`), []byte(`"key_id":"key","key_id":"key"`), 1),
				bytes.Replace(wire, []byte(`"packet":{`), []byte(`"packet":{"plaintext":"leak",`), 1),
				bytes.Replace(wire, []byte(`"encrypted":{`), []byte(`"encrypted":{"plaintext":"leak",`), 1),
				bytes.Replace(wire, []byte(`"public":{`), []byte(`"public":{"path":"leak",`), 1),
				bytes.Replace(wire, []byte(`"version":1`), []byte(`"version":null`), 1),
				[]byte(strings.Repeat(" ", protocol.MaxEncryptedPacketCarrierBytes+1)),
			} {
				if _, err := protocol.DecodeEncryptedPacketCarrier(invalid, tc.scope, tc.kind, id); err == nil {
					t.Fatal("invalid carrier accepted")
				}
			}
			if _, err := protocol.DecodeEncryptedPacketCarrier(wire, tc.scope, "different.type", id); err == nil {
				t.Fatal("type substitution accepted")
			}
			if tc.scope == "command" {
				if _, err := protocol.DecodeEncryptedPacketCarrier(wire, tc.scope, tc.kind, "other"); err == nil {
					t.Fatal("command ID substitution accepted")
				}
			}
		})
	}
}
