package main

import (
	"crypto/ed25519"
	"encoding/json"
	"io"
	"os"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

func main() {
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 8193))
	if err != nil {
		panic("read fixture")
	}
	request, err := e2ee.DecodeSessionMembershipChange(raw)
	if err != nil {
		panic("decode fixture")
	}
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 32)
	}
	signed, err := e2ee.SignSessionMembershipChange(request, ed25519.NewKeyFromSeed(seed))
	if err != nil {
		panic("sign fixture")
	}
	if signed.Signature != request.Signature {
		panic("signature mismatch")
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]bool{"verified": true}); err != nil {
		panic("encode fixture")
	}
}
