package protocol

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestDecodeTrustedSessionKeyRequest(t *testing.T) {
	signing := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	wrapping := base64.RawURLEncoding.EncodeToString(make([]byte, 65))
	signature := base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	body := map[string]any{"machine": "machine", "account": "account", "session": "session", "key_id": "key", "device": "device", "signing_key": signing, "wrapping_key": wrapping, "signature": signature}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := DecodeTrustedSessionKeyRequest(raw, "machine", "session", "device", "key")
	if err != nil || request.SigningKey != signing || request.WrappingKey != wrapping {
		t.Fatalf("valid v2 request rejected: %v", err)
	}
	body["extra"] = true
	raw, _ = json.Marshal(body)
	if _, err := DecodeTrustedSessionKeyRequest(raw, "machine", "session", "device", "key"); err == nil {
		t.Fatal("extra field accepted")
	}
	delete(body, "extra")
	body["signing_key"] = base64.RawURLEncoding.EncodeToString(make([]byte, 31))
	raw, _ = json.Marshal(body)
	if _, err := DecodeTrustedSessionKeyRequest(raw, "machine", "session", "device", "key"); err == nil {
		t.Fatal("short signing key accepted")
	}
	body["signing_key"] = signing
	raw, _ = json.Marshal(body)
	if _, err := DecodeTrustedSessionKeyRequest(raw, "machine", "session", "device", "other"); err == nil {
		t.Fatal("routing mismatch accepted")
	}
}
