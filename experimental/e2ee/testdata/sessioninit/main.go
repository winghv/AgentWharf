package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"github.com/winghv/agentwharf/experimental/e2ee"
	"io"
	"os"
)

func main() {
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 2049))
	if err != nil {
		panic(err)
	}
	request, err := e2ee.DecodeSessionInitialization(raw)
	if err != nil {
		panic(err)
	}
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 32)
	}
	key := ed25519.NewKeyFromSeed(seed)
	expected, err := e2ee.SignSessionInitialization(e2ee.SessionInitialization{Machine: "machine", Account: "account", Session: "session", KeyID: "key", Device: "client"}, key)
	if err != nil || request != expected {
		panic("initialization signature mismatch")
	}
	public := key.Public().(ed25519.PublicKey)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"signature": expected.Signature, "public": base64.RawURLEncoding.EncodeToString(public)})
}
