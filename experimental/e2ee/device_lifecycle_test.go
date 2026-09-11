package e2ee

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func enrollOfferDevice(t *testing.T, ctx context.Context, registry *DeviceRegistry, id LocalIdentity) {
	t.Helper()
	public, err := id.Public()
	if err != nil {
		t.Fatal(err)
	}
	invitation, offer, err := NewPairingInvitation("machine", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := EncryptPairingIdentity(offer, public, ed25519.NewKeyFromSeed(id.SigningSeed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Enroll(ctx, invitation, enrollment, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// Trust provenance separates offer enrollment from account-terminal trust, and
// RevokeTrusted removes only the latter with its grants, keys and seal budget.
func TestDeviceTrustProvenanceAndRevokeTrusted(t *testing.T) {
	ctx := context.Background()
	_, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.db"))
	machine, _ := NewLocalIdentity()
	if _, err := NewSessionKeyVault(ctx, db, machine); err != nil {
		t.Fatal(err)
	}
	registry, err := NewDeviceRegistry(ctx, db, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := NewLocalIdentity()
	trusted, _ := NewLocalIdentity()
	enrollOfferDevice(t, ctx, registry, owner)
	if err := registry.EnrollTrusted(ctx, mustPublic(t, trusted)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO e2ee_local_grants(session, device, verify_key, control) VALUES('session', ?, ?, 0)", trusted.Device, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO e2ee_session_keys(device, session, key_id, enc, ciphertext) VALUES(?, 'session', 'key', 'a', 'b')", trusted.Device); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO e2ee_seal_budget(device, session, key_id, used) VALUES(?, 'session', 'key', 1)", trusted.Device); err != nil {
		t.Fatal(err)
	}

	records, err := registry.ListDetailed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	provenance := map[string]bool{}
	for _, record := range records {
		provenance[record.Identity.Device] = record.Trusted
	}
	if provenance[owner.Device] || !provenance[trusted.Device] {
		t.Fatalf("provenance = %v", provenance)
	}

	revoked, err := registry.RevokeTrusted(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(revoked) != 1 || revoked[0] != trusted.Device {
		t.Fatalf("revoked = %v", revoked)
	}
	if _, err := registry.Device(ctx, trusted.Device); err != ErrUnauthorized {
		t.Fatalf("revoked device lookup = %v, want ErrUnauthorized", err)
	}
	if _, err := registry.Device(ctx, owner.Device); err != nil {
		t.Fatalf("offer device was revoked: %v", err)
	}
	for _, table := range []string{"e2ee_local_grants", "e2ee_session_keys", "e2ee_seal_budget"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE device=?", trusted.Device).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows for revoked device = %d, want 0", table, count)
		}
	}
}

// A database created before the trusted flag existed gains the column with the
// safe offer-enrolled default, so trust-off never revokes a pre-existing device.
func TestDeviceRegistryMigratesLegacyDeviceTable(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE e2ee_devices (machine TEXT NOT NULL, account TEXT NOT NULL, device TEXT NOT NULL, signing_key TEXT NOT NULL, wrapping_key TEXT NOT NULL, PRIMARY KEY(machine,account,device))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO e2ee_devices(machine,account,device,signing_key,wrapping_key) VALUES('machine','account','legacy','s','w')`); err != nil {
		t.Fatal(err)
	}
	registry, err := NewDeviceRegistry(ctx, db, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ListDetailed(ctx); err != nil {
		t.Fatal(err)
	}
	revoked, err := registry.RevokeTrusted(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(revoked) != 0 {
		t.Fatalf("legacy revoke = %v, want none", revoked)
	}
}

func mustPublic(t *testing.T, id LocalIdentity) PairingIdentity {
	t.Helper()
	public, err := id.Public()
	if err != nil {
		t.Fatal(err)
	}
	return public
}

// Turning account-terminal trust off must revoke the trust-enrolled devices and
// advance active sessions to a fresh key so those devices lose future content.
func TestRevokeTrustedTerminalsRotatesSessions(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.db"))
	machine, _ := NewLocalIdentity()
	vault, err := NewSessionKeyVault(ctx, db, machine)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewCommandExecutor(journal, vault)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewDeviceRegistry(ctx, db, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := NewLocalIdentity()
	trusted, _ := NewLocalIdentity()
	enrollOfferDevice(t, ctx, registry, owner)
	if err := registry.EnrollTrusted(ctx, mustPublic(t, trusted)); err != nil {
		t.Fatal(err)
	}
	init, err := SignSessionInitialization(SessionInitialization{Machine: "machine", Account: "account", Session: "session", KeyID: "key1", Device: owner.Device}, ed25519.NewKeyFromSeed(owner.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.InitializeSession(ctx, registry, init); err != nil {
		t.Fatal(err)
	}
	epochBefore, keyBefore, err := journal.SessionState(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	var grantsBefore int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM e2ee_local_grants WHERE session='session'").Scan(&grantsBefore); err != nil {
		t.Fatal(err)
	}
	if grantsBefore != 2 {
		t.Fatalf("grants before = %d, want 2", grantsBefore)
	}

	revoked, err := vault.RevokeTrustedTerminals(ctx, journal, registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(revoked) != 1 || revoked[0] != trusted.Device {
		t.Fatalf("revoked = %v", revoked)
	}
	epochAfter, keyAfter, err := journal.SessionState(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if epochAfter != epochBefore+1 || keyAfter == keyBefore {
		t.Fatalf("rotation epoch %d->%d key %q->%q", epochBefore, epochAfter, keyBefore, keyAfter)
	}
	var trustedGrants int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM e2ee_local_grants WHERE session='session' AND device=?", trusted.Device).Scan(&trustedGrants); err != nil {
		t.Fatal(err)
	}
	if trustedGrants != 0 {
		t.Fatalf("revoked device still has %d grants", trustedGrants)
	}
	var ownerControl int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM e2ee_local_grants WHERE session='session' AND device=? AND control=1", owner.Device).Scan(&ownerControl); err != nil {
		t.Fatal(err)
	}
	if ownerControl != 1 {
		t.Fatal("owner control grant was lost during rotation")
	}
	if _, err := registry.Device(ctx, trusted.Device); err != ErrUnauthorized {
		t.Fatalf("revoked device lookup = %v, want ErrUnauthorized", err)
	}
}

func TestInitializeLocalSessionCreatesMachineOwnedKey(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.db"))
	machine, _ := NewLocalIdentity()
	vault, err := NewSessionKeyVault(ctx, db, machine)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := vault.InitializeLocalSession(ctx, journal, "session")
	if err != nil {
		t.Fatal(err)
	}
	epoch, storedKeyID, err := journal.SessionState(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 1 || storedKeyID != keyID {
		t.Fatalf("epoch=%d key=%q want %q", epoch, storedKeyID, keyID)
	}
	key, err := vault.Load(ctx, "session", keyID)
	if err != nil || len(key) != 32 {
		t.Fatalf("load len=%d err=%v", len(key), err)
	}
	clear(key)
	grants, err := journal.sessionGrants(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 1 || grants[0].DeviceID != machine.Device || !grants[0].Control {
		t.Fatalf("grants = %+v", grants)
	}
}
