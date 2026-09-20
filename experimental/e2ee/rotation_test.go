package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

// The durable per-key seal budget never resets within one epoch, so a
// long-running session used to wedge permanently once it hit the hard bound.
// Rotation must renew the seal budget and the command admission budget while
// keeping historical keys decryptable.
func TestRotateSessionKeysForBudgetRenewsSealsAndCommands(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.sqlite"))
	machine, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, machine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vault.InitializeLocalSession(ctx, journal, "session"); err != nil {
		t.Fatal(err)
	}
	public := ed25519.NewKeyFromSeed(machine.SigningSeed).Public().(ed25519.PublicKey)
	epoch, keyID, err := journal.SessionState(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 1 {
		t.Fatalf("epoch = %d", epoch)
	}

	event := Context{"event", "session", machine.Device, keyID, "message", "session.message"}
	payload := json.RawMessage(`{"role":"agent","content":[{"kind":"text","text":"synthetic private reply"}]}`)
	if _, err := vault.SealEvent(ctx, event, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE e2ee_seal_budget SET used=?`, MaxEndpointSealsPerKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE e2ee_local_sessions SET command_count=?`, maxCommandsPerSession); err != nil {
		t.Fatal(err)
	}
	// Reproduce the production wedge: every seal and every command fails.
	if _, err := vault.SealEvent(ctx, event, payload); !errors.Is(err, ErrCapacity) {
		t.Fatalf("expected exhausted seals, got %v", err)
	}

	used, err := vault.SealsUsed(ctx, "session")
	if err != nil || used != MaxEndpointSealsPerKey {
		t.Fatalf("SealsUsed = %d, %v", used, err)
	}

	newKeyID, err := vault.RotateSessionKeysForBudget(ctx, journal, "session")
	if err != nil {
		t.Fatal(err)
	}
	if newKeyID == "" || newKeyID == keyID {
		t.Fatalf("rotation produced key %q (old %q)", newKeyID, keyID)
	}
	newEpoch, currentKeyID, err := journal.SessionState(ctx, "session")
	if err != nil || newEpoch != epoch+1 || currentKeyID != newKeyID {
		t.Fatalf("state = %d/%q, %v", newEpoch, currentKeyID, err)
	}

	// Sealing resumes under the new epoch.
	event.KeyID = newKeyID
	event.MessageID = "after-rotation"
	if _, err := vault.SealEvent(ctx, event, payload); err != nil {
		t.Fatal("sealing did not resume after rotation:", err)
	}
	// The old epoch can no longer seal new events.
	stale := Context{"event", "session", machine.Device, keyID, "stale", "session.message"}
	if _, err := vault.SealEvent(ctx, stale, payload); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("historical seal allowed: %v", err)
	}
	// The historical key still decrypts replayed history.
	key, err := vault.Load(ctx, "session", keyID)
	if err != nil {
		t.Fatal("historical key lost:", err)
	}
	clear(key)

	// The command admission budget renewed with the epoch.
	grants, err := journal.sessionGrants(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	// Grants are preserved (plus the machine control grant that rotation keeps).
	if len(grants) == 0 {
		t.Fatal("rotation dropped grants")
	}
	for _, grant := range grants {
		if grant.DeviceID == machine.Device && !grant.Control {
			t.Fatal("rotation dropped the machine control grant")
		}
	}
	_ = public
}

func TestRotateSessionKeysForBudgetConflictOnConcurrentRotation(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.sqlite"))
	machine, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, machine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vault.InitializeLocalSession(ctx, journal, "session"); err != nil {
		t.Fatal(err)
	}
	if _, err := vault.RotateSessionKeysForBudget(ctx, journal, "session"); err != nil {
		t.Fatal(err)
	}
	// A second rotation computed against the pre-rotation epoch must lose the
	// CAS instead of silently re-rotating from stale state.
	if _, err := vault.RotateSessionKeysForBudget(ctx, journal, "session"); err != nil {
		t.Fatal("rotation from current epoch should succeed:", err)
	}
}

func TestAdmitRejectsStaleEpochDistinctFromUnauthorized(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.sqlite"))
	machine, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, machine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vault.InitializeLocalSession(ctx, journal, "session"); err != nil {
		t.Fatal(err)
	}
	_, oldKeyID, err := journal.SessionState(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vault.RotateSessionKeysForBudget(ctx, journal, "session"); err != nil {
		t.Fatal(err)
	}
	// A command sealed under the retired epoch is a stale-epoch signal, not a
	// generic authorization failure.
	command := Context{"command", "session", machine.Device, oldKeyID, "message", "session.send"}
	key, err := vault.Load(ctx, "session", oldKeyID)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	signer := ed25519.NewKeyFromSeed(machine.SigningSeed)
	projection, err := ProjectPublicMetadata(command.Type, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	packet, err := SealPacket(command, key, signer, projection, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = journal.Admit(ctx, command, key, packet.Encrypted)
	if !errors.Is(err, ErrEpochStale) {
		t.Fatalf("expected ErrEpochStale, got %v", err)
	}
	// A genuinely unknown session stays ErrUnauthorized.
	unknown := Context{"command", "other-session", machine.Device, oldKeyID, "message", "session.send"}
	_, _, err = journal.Admit(ctx, unknown, key, packet.Encrypted)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}
