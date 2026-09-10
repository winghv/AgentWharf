package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"path/filepath"
	"testing"
)

func TestSessionTransitionAtomicRollbackAndRotation(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.sqlite"))
	identity, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	grants := []DeviceGrant{{identity.Device, ed25519.NewKeyFromSeed(identity.SigningSeed).Public().(ed25519.PublicKey), true}}
	if _, err := db.Exec(`CREATE TRIGGER fail_key BEFORE INSERT ON e2ee_session_keys BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := vault.TransitionSession(ctx, journal, "session", "key_1", 0, grants); err == nil {
		t.Fatal("accepted key write failure")
	}
	for _, table := range []string{"e2ee_local_sessions", "e2ee_local_grants", "e2ee_local_key_epochs", "e2ee_session_keys"} {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("partial transition", table, count, err)
		}
	}
	if _, err := db.Exec(`DROP TRIGGER fail_key`); err != nil {
		t.Fatal(err)
	}
	if err := vault.TransitionSession(ctx, journal, "session", "key_1", 0, grants); err != nil {
		t.Fatal(err)
	}
	first, err := vault.Load(ctx, "session", "key_1")
	if err != nil {
		t.Fatal(err)
	}
	if err := vault.TransitionSession(ctx, journal, "session", "key_2", 0, grants); err == nil {
		t.Fatal("accepted stale epoch")
	}
	if _, err := vault.Load(ctx, "session", "key_2"); err == nil {
		t.Fatal("orphan key after CAS failure")
	}
	if err := vault.TransitionSession(ctx, journal, "session", "key_2", 1, grants); err != nil {
		t.Fatal(err)
	}
	second, err := vault.Load(ctx, "session", "key_2")
	if err != nil || bytes.Equal(first, second) {
		t.Fatal("rotation failed", err)
	}
	retained, err := vault.Load(ctx, "session", "key_1")
	if err != nil || !bytes.Equal(first, retained) {
		t.Fatal("lost history key", err)
	}
	if err := vault.TransitionSession(ctx, journal, "session", "key_1", 2, grants); err == nil {
		t.Fatal("reused old key epoch")
	}
}
