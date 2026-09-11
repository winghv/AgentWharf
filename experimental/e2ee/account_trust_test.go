package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"path/filepath"
	"testing"
	"time"
)

func TestTrustedAccountTerminalEnrollment(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.db"))
	machine, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
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
	enroll := func(id LocalIdentity) {
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
	enroll(owner)
	enroll(viewer)
	init, err := SignSessionInitialization(SessionInitialization{Machine: "machine", Account: "account", Session: "session", KeyID: "key1", Device: owner.Device}, ed25519.NewKeyFromSeed(owner.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.InitializeSession(ctx, registry, init); err != nil {
		t.Fatal(err)
	}

	newbie, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	newbiePublic, err := newbie.Public()
	if err != nil {
		t.Fatal(err)
	}
	request, err := SignTrustedSessionKeyRequest(TrustedSessionKeyRequest{Machine: "machine", Account: "account", Session: "session", KeyID: "key1", Device: newbie.Device, SigningKey: newbiePublic.SigningKey, WrappingKey: newbiePublic.WrappingKey}, ed25519.NewKeyFromSeed(newbie.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}

	// Trust disabled: rejected and never enrolled.
	if _, err := executor.RecoverSessionKeyTrusted(ctx, registry, request, false, true); err != ErrUnauthorized {
		t.Fatalf("trust disabled error = %v, want ErrUnauthorized", err)
	}
	if _, err := registry.Device(ctx, newbie.Device); err == nil {
		t.Fatal("trust disabled enrolled the device")
	}

	// Trust enabled: enrolled, granted, and the wrapped key opens to the session key.
	wrapped, err := executor.RecoverSessionKeyTrusted(ctx, registry, request, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Device(ctx, newbie.Device); err != nil {
		t.Fatalf("trusted device not enrolled: %v", err)
	}
	var control int
	if err := db.QueryRowContext(ctx, "SELECT control FROM e2ee_local_grants WHERE session='session' AND device=?", newbie.Device).Scan(&control); err != nil {
		t.Fatal(err)
	}
	if control != 1 {
		t.Fatal("trusted account terminal was not granted control")
	}
	// An already enrolled view grant is upgraded when the terminal re-requests
	// under account-terminal trust, so a previously added terminal gains control.
	viewerPublic, err := viewer.Public()
	if err != nil {
		t.Fatal(err)
	}
	viewerRequest, err := SignTrustedSessionKeyRequest(TrustedSessionKeyRequest{Machine: "machine", Account: "account", Session: "session", KeyID: "key1", Device: viewer.Device, SigningKey: viewerPublic.SigningKey, WrappingKey: viewerPublic.WrappingKey}, ed25519.NewKeyFromSeed(viewer.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.RecoverSessionKeyTrusted(ctx, registry, viewerRequest, true, true); err != nil {
		t.Fatal(err)
	}
	var viewerControl int
	if err := db.QueryRowContext(ctx, "SELECT control FROM e2ee_local_grants WHERE session='session' AND device=?", viewer.Device).Scan(&viewerControl); err != nil {
		t.Fatal(err)
	}
	if viewerControl != 1 {
		t.Fatal("existing view grant was not upgraded to control")
	}
	sessionKey, err := vault.Load(ctx, "session", "key1")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(sessionKey)
	machinePublic, err := machine.Public()
	if err != nil {
		t.Fatal(err)
	}
	senderPublic, err := decode(machinePublic.WrappingKey, 65, 65)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := UnwrapKey(WrapContext{Session: "session", KeyID: "key1", Sender: machinePublic.Device, Recipient: newbie.Device}, newbie.WrappingPrivate, senderPublic, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(opened)
	if !bytes.Equal(opened, sessionKey) {
		t.Fatal("trusted wrapped key does not match session key")
	}

	// A request that substitutes another device's wrapping key must be rejected.
	tampered := request
	ownerPublic, err := owner.Public()
	if err != nil {
		t.Fatal(err)
	}
	tampered.WrappingKey = ownerPublic.WrappingKey
	if _, err := executor.RecoverSessionKeyTrusted(ctx, registry, tampered, true, true); err != ErrUnauthorized {
		t.Fatalf("tampered request error = %v, want ErrUnauthorized", err)
	}

	// A later session seeds every enrolled device: owner + viewer + newbie.
	init2, err := SignSessionInitialization(SessionInitialization{Machine: "machine", Account: "account", Session: "session2", KeyID: "key2", Device: owner.Device}, ed25519.NewKeyFromSeed(owner.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.InitializeSession(ctx, registry, init2); err != nil {
		t.Fatal(err)
	}
	var grants int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM e2ee_local_grants WHERE session='session2'").Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if grants != 3 {
		t.Fatalf("seeded grants = %d, want 3", grants)
	}
}
