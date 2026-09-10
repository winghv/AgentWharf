package protocol

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestEncryptedPairingRelayGrammar(t *testing.T) {
	request := EncryptedPairingRequest{Enc: base64.RawURLEncoding.EncodeToString(make([]byte, 65)), Ciphertext: base64.RawURLEncoding.EncodeToString(make([]byte, 1024))}
	wire, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeEncryptedPairingRequest(wire)
	if err != nil || got != request {
		t.Fatalf("round trip: %v", err)
	}
	for name, wire := range map[string]string{
		"secret":    strings.TrimSuffix(string(wire), "}") + `,"secret":"local-only"}`,
		"duplicate": strings.TrimSuffix(string(wire), "}") + `,"enc":"other"}`,
		"null":      `{"enc":null,"ciphertext":null}`,
		"trailing":  string(wire) + `{}`,
		"oversize":  strings.Repeat(" ", 2049),
		"missing":   `{"enc":"abc"}`,
		"padded":    strings.Replace(string(wire), request.Enc, request.Enc+"=", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeEncryptedPairingRequest([]byte(wire)); err == nil {
				t.Fatal("invalid relay payload accepted")
			}
		})
	}
}
