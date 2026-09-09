package e2ee

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestProcessStartSerializesAgainstOtherEndpointOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "endpoint.db")
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
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ReplaceGrants(ctx, "session", "key", 0, []DeviceGrant{{"device", public, true}}); err != nil {
		t.Fatal(err)
	}
	key, err := vault.Load(ctx, "session", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	packet, err := SealPacket(Context{Scope: "command", Session: "session", Sender: "device", KeyID: "key", MessageID: "launch", Type: "session.send"}, key, private, PublicMetadata{}, json.RawMessage(`{"content":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "device", "message_id": "launch", "type": "session.send", "packet": packet})
	if err != nil {
		t.Fatal(err)
	}
	otherJournal, otherDB := openJournal(t, path)
	otherDB.SetMaxOpenConns(1)
	if _, err := otherDB.Exec(`PRAGMA busy_timeout=0`); err != nil {
		t.Fatal(err)
	}
	otherVault, err := NewSessionKeyVault(ctx, otherDB, identity)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewCommandExecutor(otherJournal, otherVault)
	if err != nil {
		t.Fatal(err)
	}
	revoke := func() error {
		return other.ReplaceGrants(context.Background(), "session", "next-key", 1, []DeviceGrant{{"device", public, false}})
	}
	starts := 0
	if err := executor.WithProcessStart(ctx, "session", "launch", wire, func() error {
		starts++
		cancel()
		// Cancellation must not auto-rollback the authority lock before the
		// synchronous process creation callback has returned.
		if err := revoke(); err == nil {
			t.Fatal("revocation committed during process start")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := revoke(); err != nil {
		t.Fatal("revocation after start", err)
	}
	if err := executor.WithProcessStart(context.Background(), "session", "launch", wire, func() error { starts++; return nil }); err == nil {
		t.Fatal("revoked launch started")
	}
	if starts != 1 {
		t.Fatalf("starts=%d", starts)
	}
	var commands int
	if err := db.QueryRow(`SELECT count(*) FROM e2ee_local_commands`).Scan(&commands); err != nil || commands != 0 {
		t.Fatal("process authorization consumed prompt", err)
	}
}
