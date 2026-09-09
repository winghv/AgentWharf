// Synthetic interoperability harness; never use with real keys.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/cloudflare/circl/hpke"
	"github.com/winghv/agentwharf/experimental/e2ee"
)

func run() error {
	var request struct {
		Operation string
		Wrapped   e2ee.WrappedKey
	}
	d := json.NewDecoder(io.LimitReader(os.Stdin, 4096))
	d.DisallowUnknownFields()
	if err := d.Decode(&request); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return e2ee.ErrInvalid
	}
	sender := make([]byte, 32)
	sender[31] = 1
	recipient := make([]byte, 32)
	recipient[31] = 2
	scheme := hpke.KEM_P256_HKDF_SHA256.Scheme()
	sk, err := scheme.UnmarshalBinaryPrivateKey(sender)
	if err != nil {
		return err
	}
	rk, err := scheme.UnmarshalBinaryPrivateKey(recipient)
	if err != nil {
		return err
	}
	senderPub, err := sk.Public().MarshalBinary()
	if err != nil {
		return err
	}
	recipientPub, err := rk.Public().MarshalBinary()
	if err != nil {
		return err
	}
	ctx := e2ee.WrapContext{Session: "session", KeyID: "key_1", Sender: "sender", Recipient: "recipient"}
	key := bytes.Repeat([]byte{42}, 32)
	switch request.Operation {
	case "seal":
		wrapped, err := e2ee.WrapKey(ctx, sender, recipientPub, key)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Wrapped                       e2ee.WrappedKey
			SenderPublic, RecipientPublic []byte
		}{wrapped, senderPub, recipientPub})
	case "open":
		result, err := e2ee.UnwrapKey(ctx, recipient, senderPub, request.Wrapped)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(bytes.Equal(result, key))
	default:
		return e2ee.ErrInvalid
	}
}
func main() {
	if run() != nil {
		fmt.Fprintln(os.Stderr, "keywrap interop failed")
		os.Exit(1)
	}
}
