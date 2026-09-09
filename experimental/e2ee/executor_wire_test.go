package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestWireIngressRequiresLocalGrantAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoint.sqlite")
	journal, db := openJournal(t, path)
	identity, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewCommandExecutor(journal, vault)
	if err != nil {
		t.Fatal(err)
	}
	clientPublic, clientPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	attackerPublic, attackerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ReplaceGrants(ctx, "session", "key", 0, []DeviceGrant{{"client", clientPublic, true}, {"viewer", attackerPublic, false}}); err != nil {
		t.Fatal(err)
	}
	key, err := vault.Load(ctx, "session", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	payload := json.RawMessage(`{"content":[{"kind":"text","text":"private-wire-canary"}]}`)
	makeWire := func(sender, id string, signer ed25519.PrivateKey) []byte {
		t.Helper()
		command := Context{Scope: "command", Session: "session", Sender: sender, KeyID: "key", MessageID: id, Type: "session.send"}
		packet, err := SealPacket(command, key, signer, PublicMetadata{}, payload)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": sender, "message_id": id, "type": "session.send", "packet": packet})
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte("private-wire-canary")) {
			t.Fatal("plaintext entered wire")
		}
		return raw
	}
	calls := 0
	deliver := func(_ context.Context, got json.RawMessage) error {
		calls++
		if !bytes.Equal(got, payload) {
			t.Fatal("payload changed")
		}
		var state string
		if err := db.QueryRow(`SELECT state FROM e2ee_local_commands WHERE session='session' AND message='valid'`).Scan(&state); err != nil || state != "claimed" {
			t.Fatalf("provider reached before durable claim: %q %v", state, err)
		}
		return nil
	}
	valid := makeWire("client", "valid", clientPrivate)
	for _, tc := range []struct {
		name, session, id string
		wire              []byte
	}{
		{"plaintext", "session", "valid", payload},
		{"outer ID substitution", "session", "other", valid},
		{"session substitution", "other-session", "valid", valid},
		{"key holder without grant", "session", "untrusted", makeWire("unknown", "untrusted", attackerPrivate)},
		{"view only key holder", "session", "view", makeWire("viewer", "view", attackerPrivate)},
		{"forged authorized identity", "session", "forged", makeWire("client", "forged", attackerPrivate)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := executor.ExecuteWire(ctx, tc.session, tc.id, "session.send", tc.wire, deliver); err == nil {
				t.Fatal("unauthorized wire accepted")
			}
			if calls != 0 {
				t.Fatal("unauthorized wire reached provider")
			}
		})
	}
	result, err := executor.ExecuteWire(ctx, "session", "valid", "session.send", valid, deliver)
	if err != nil || !result.Execute || result.State != "completed" || calls != 1 {
		t.Fatalf("delivery=%+v %v calls=%d", result, err, calls)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	journal, db = openJournal(t, path)
	vault, err = NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	executor, err = NewCommandExecutor(journal, vault)
	if err != nil {
		t.Fatal(err)
	}
	result, err = executor.ExecuteWire(ctx, "session", "valid", "session.send", valid, deliver)
	if err != nil || result.Execute || calls != 1 {
		t.Fatalf("restart replay=%+v %v calls=%d", result, err, calls)
	}
	if err := executor.VerifyWire(ctx, "session", "valid", "session.send", valid); err != nil {
		t.Fatal("restored launch preflight", err)
	}
	if err := executor.ReplaceGrants(ctx, "session", "next-key", 1, []DeviceGrant{{"client", clientPublic, false}}); err != nil {
		t.Fatal(err)
	}
	if err := executor.VerifyWire(ctx, "session", "valid", "session.send", valid); err == nil {
		t.Fatal("revoked original launch passed preflight")
	}
	if _, err := executor.ExecuteWire(ctx, "session", "after-revoke", "session.send", makeWire("client", "after-revoke", clientPrivate), deliver); err == nil {
		t.Fatal("revoked old-key command accepted")
	}
	if calls != 1 {
		t.Fatal("revoked command reached provider")
	}
}
