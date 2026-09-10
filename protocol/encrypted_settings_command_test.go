package protocol_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func TestDecodeEncryptedSettingsCommandPreservesStrictCarrier(t *testing.T) {
	encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	carrier := protocol.EncryptedPacketCarrier{Version: 1, Scope: "command", KeyID: "key", Sender: "client", MessageID: "settings", Type: "session.settings.change", Packet: protocol.OpaqueContentPacket{Version: 1, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}}
	payload, err := json.Marshal(carrier)
	if err != nil {
		t.Fatal(err)
	}
	command := protocol.Command{CommandID: "settings", Type: protocol.CommandSettingsChange, SessionID: "session", Payload: payload}
	wire, err := protocol.Encode(&command)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := frame.(*protocol.Command)
	if !ok || string(decoded.Payload) != string(payload) {
		t.Fatal("opaque settings changed")
	}
	for _, mutation := range []string{"message_id", "type", "plaintext", "duplicate"} {
		t.Run(mutation, func(t *testing.T) {
			changed := carrier
			switch mutation {
			case "message_id":
				changed.MessageID = "other"
			case "type":
				changed.Type = "session.stop"
			}
			changedPayload, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if mutation == "plaintext" {
				changedPayload = append(changedPayload[:len(changedPayload)-1], []byte(`,"model_id":"synthetic-private"}`)...)
			}
			if mutation == "duplicate" {
				changedPayload = append(changedPayload[:len(changedPayload)-1], []byte(`,"message_id":"settings"}`)...)
			}
			bad := command
			bad.Payload = changedPayload
			wire, err := protocol.Encode(&bad)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := protocol.Decode(wire); err == nil {
				t.Fatal("invalid carrier accepted")
			}
		})
	}
}
