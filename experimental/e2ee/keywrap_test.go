package e2ee

import (
	"bytes"
	"testing"
)

func TestAuthenticatedKeyWrap(t *testing.T) {
	sender, senderPub, err := GenerateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	recipient, recipientPub, err := GenerateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	ctx := WrapContext{"session", "key_1", "sender", "recipient"}
	key := bytes.Repeat([]byte{42}, 32)
	wrapped, err := WrapKey(ctx, sender, recipientPub, key)
	if err != nil {
		t.Fatal(err)
	}
	result, err := UnwrapKey(ctx, recipient, senderPub, wrapped)
	if err != nil || !bytes.Equal(result, key) {
		t.Fatal("roundtrip", err)
	}
	_, wrongPub, err := GenerateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnwrapKey(ctx, recipient, wrongPub, wrapped); err == nil {
		t.Fatal("wrong sender accepted")
	}
	if _, err := UnwrapKey(ctx, sender, senderPub, wrapped); err == nil {
		t.Fatal("wrong recipient accepted")
	}
	for _, changed := range []WrapContext{{"other", "key_1", "sender", "recipient"}, {"session", "key_2", "sender", "recipient"}, {"session", "key_1", "other", "recipient"}, {"session", "key_1", "sender", "other"}} {
		if _, err := UnwrapKey(changed, recipient, senderPub, wrapped); err == nil {
			t.Fatal("context substitution accepted")
		}
	}
	tampered := wrapped
	tampered.Ciphertext += "="
	if _, err := UnwrapKey(ctx, recipient, senderPub, tampered); err == nil {
		t.Fatal("noncanonical data accepted")
	}
	if _, err := WrapKey(ctx, sender, recipientPub, key[:31]); err == nil {
		t.Fatal("invalid key length accepted")
	}
}
