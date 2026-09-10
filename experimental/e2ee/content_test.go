package e2ee

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
)

func TestContent(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	ctx := Context{"command", "ses_test", "device_test", "epoch_1", "cmd_test", "session.send"}
	plaintext := []byte("synthetic private instruction")
	envelope, err := Seal(ctx, key, priv, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeEnvelope(wire)
	if err != nil || decoded != envelope {
		t.Fatal("envelope decode failed")
	}
	for _, invalid := range [][]byte{
		append(append([]byte(nil), wire...), []byte(`{}`)...),
		[]byte(`{"nonce":"a","nonce":"b","ciphertext":"c","signature":"d"}`),
		[]byte(`{"nonce":null,"ciphertext":"c","signature":"d"}`),
		[]byte(`{"extra":"a"}`),
		[]byte(`[]`),
	} {
		if _, err := DecodeEnvelope(invalid); err == nil {
			t.Fatal("accepted invalid wire envelope")
		}
	}
	result, err := Open(ctx, key, pub, envelope)
	if err != nil || !bytes.Equal(result, plaintext) {
		t.Fatalf("round trip: %v", err)
	}
	for _, field := range []string{"scope", "session", "sender", "key", "message", "type"} {
		t.Run(field, func(t *testing.T) {
			other := ctx
			switch field {
			case "scope":
				other.Scope = "event"
			case "session":
				other.Session = "other"
			case "sender":
				other.Sender = "other"
			case "key":
				other.KeyID = "other"
			case "message":
				other.MessageID = "other"
			case "type":
				other.Type = "session.stop"
			}
			if _, err := Open(other, key, pub, envelope); err == nil {
				t.Fatal("accepted changed context")
			}
		})
	}
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := Open(ctx, key, otherPub, envelope); err == nil {
		t.Fatal("accepted wrong signer")
	}
	wrongKey := append([]byte(nil), key...)
	wrongKey[0] ^= 1
	if _, err := Open(ctx, wrongKey, pub, envelope); err == nil {
		t.Fatal("accepted wrong content key")
	}
	tampered := envelope
	tampered.Nonce += "="
	if _, err := Open(ctx, key, pub, tampered); err == nil {
		t.Fatal("accepted noncanonical nonce")
	}
	tampered = envelope
	tampered.Ciphertext = envelope.Ciphertext[1:]
	if _, err := Open(ctx, key, pub, tampered); err == nil {
		t.Fatal("accepted tampering")
	}
	if _, err := Seal(ctx, key, priv, make([]byte, MaxContentBytes+1)); err == nil {
		t.Fatal("accepted oversized content")
	}
	for _, size := range []int{0, MaxContentBytes} {
		input := make([]byte, size)
		sealed, err := Seal(ctx, key, priv, input)
		if err != nil {
			t.Fatal(err)
		}
		output, err := Open(ctx, key, pub, sealed)
		if err != nil || !bytes.Equal(input, output) {
			t.Fatal("boundary round trip failed")
		}
	}
}
