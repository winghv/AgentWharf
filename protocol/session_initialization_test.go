package protocol_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func TestSessionInitializationRelayCarrier(t *testing.T) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := e2ee.SignSessionInitialization(e2ee.SessionInitialization{Machine: "machine", Account: "account", Session: "session", KeyID: "key", Device: "client"}, private)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	request, err := protocol.DecodeSessionInitialization(raw, "machine", "session")
	if err != nil || request.Signature != signed.Signature {
		t.Fatal("valid signature carrier changed", err)
	}
	for _, route := range [][2]string{{"other", "session"}, {"machine", "other"}, {"", "session"}} {
		if _, err := protocol.DecodeSessionInitialization(raw, route[0], route[1]); err == nil {
			t.Fatal("route substitution accepted")
		}
	}
	for _, bad := range [][]byte{
		append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"device":"other"}`)...),
		append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"content":"private"}`)...),
		append(append([]byte(nil), raw...), []byte(`{}`)...),
		bytes.Repeat([]byte(" "), 2049),
	} {
		if _, err := protocol.DecodeSessionInitialization(bad, "machine", "session"); err == nil {
			t.Fatal("ambiguous request accepted")
		}
	}
}
