// Synthetic test harness only. Never use with real content or keys.
package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

func run() error {
	var request struct {
		Operation string
		Context   e2ee.Context
		Envelope  e2ee.Envelope
		Plaintext string
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 128*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return e2ee.ErrInvalid
	}
	// Public fixture keys, deliberately deterministic for cross-language tests.
	key := make([]byte, 32)
	seed := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
		seed[i] = byte(i + 32)
	}
	private := ed25519.NewKeyFromSeed(seed)
	switch request.Operation {
	case "seal":
		envelope, err := e2ee.Seal(request.Context, key, private, []byte(request.Plaintext))
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Envelope  e2ee.Envelope
			PublicKey string
		}{envelope, base64.RawURLEncoding.EncodeToString(private.Public().(ed25519.PublicKey))})
	case "open":
		plaintext, err := e2ee.Open(request.Context, key, private.Public().(ed25519.PublicKey), request.Envelope)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(string(plaintext))
	default:
		return e2ee.ErrInvalid
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "interop request failed")
		os.Exit(1)
	}
}
