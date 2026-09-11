package e2ee

import (
	"context"
	"crypto/ed25519"
	"path/filepath"
	"testing"
)

func TestSessionMembershipDirectorySignsCurrentGrants(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.db"))
	machine, err := NewLocalIdentity()
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
	owner, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ownerKey := ed25519.NewKeyFromSeed(owner.SigningSeed).Public().(ed25519.PublicKey)
	if err := executor.ReplaceGrants(ctx, "session", "key1", 0, []DeviceGrant{{DeviceID: owner.Device, VerifyKey: ownerKey, Control: true}}); err != nil {
		t.Fatal(err)
	}
	directory, err := vault.SignSessionMembershipDirectory(ctx, journal, "session")
	if err != nil {
		t.Fatal(err)
	}
	machinePublic, err := machine.Public()
	if err != nil {
		t.Fatal(err)
	}
	machineKey, err := decode(machinePublic.SigningKey, 32, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySessionMembershipDirectory(directory, machineKey); err != nil {
		t.Fatalf("directory did not verify: %v", err)
	}
	if directory.Machine != machinePublic.Device || directory.Session != "session" || directory.KeyID != "key1" {
		t.Fatalf("directory identity = %+v", directory)
	}
	// The signed member list carries the authorized session devices, not the
	// endpoint itself; the endpoint identity is the separate Machine field.
	if len(directory.Members) != 1 || directory.Members[0].Device != owner.Device || !directory.Members[0].Control {
		t.Fatalf("directory members = %+v", directory.Members)
	}

	// A relay that flips authority cannot reuse the signature.
	tampered := directory
	tampered.Members = append([]SessionMemberKey(nil), directory.Members...)
	tampered.Members[0].Control = !tampered.Members[0].Control
	if err := VerifySessionMembershipDirectory(tampered, machineKey); err == nil {
		t.Fatal("tampered directory verified")
	}
	// A relay that substitutes the machine identity cannot reuse the signature.
	other, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	otherPublic, err := other.Public()
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := decode(otherPublic.SigningKey, 32, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySessionMembershipDirectory(directory, otherKey); err == nil {
		t.Fatal("directory verified under a substituted machine key")
	}
}
