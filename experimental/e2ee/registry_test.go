package e2ee

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"path/filepath"
	"testing"
	"time"
)

func TestDeviceEnrollmentPersistsIdentityAndReceipt(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	path := filepath.Join(t.TempDir(), "local.sqlite")
	_, db := openJournal(t, path)
	registry, err := NewDeviceRegistry(ctx, db, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, wrapping, err := GenerateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	identity := PairingIdentity{"device", base64.RawURLEncoding.EncodeToString(public), base64.RawURLEncoding.EncodeToString(wrapping)}
	inv, offer, err := NewPairingInvitation("machine", now)
	if err != nil {
		t.Fatal(err)
	}
	request, err := EncryptPairingIdentity(offer, identity, private, now)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := registry.Enroll(ctx, inv, request, now)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	_, db = openJournal(t, path)
	registry, err = NewDeviceRegistry(ctx, db, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	got, err := registry.Device(ctx, "device")
	if err != nil || got != identity {
		t.Fatal("lost enrolled identity", err)
	}
	recovered, err := registry.RecoverReceipt(ctx, offer.ID, request, now)
	if err != nil || recovered != receipt {
		t.Fatal("lost receipt", err)
	}
	other, err := NewDeviceRegistry(ctx, db, "machine", "other")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Device(ctx, "device"); err == nil {
		t.Fatal("cross-account identity leak")
	}
	if _, err := other.RecoverReceipt(ctx, offer.ID, request, now); err == nil {
		t.Fatal("cross-account receipt leak")
	}
	if _, err := registry.RecoverReceipt(ctx, offer.ID, request, now.Add(5*time.Minute)); err == nil {
		t.Fatal("expired receipt recovered")
	}
	inv2, offer2, err := NewPairingInvitation("machine", now)
	if err != nil {
		t.Fatal(err)
	}
	changed := identity
	otherPublic, otherPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	changed.SigningKey = base64.RawURLEncoding.EncodeToString(otherPublic)
	request2, err := EncryptPairingIdentity(offer2, changed, otherPrivate, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Enroll(ctx, inv2, request2, now); err == nil {
		t.Fatal("replaced trusted device key")
	}
	got, err = registry.Device(ctx, "device")
	if err != nil || got != identity {
		t.Fatal("identity mutated on conflict")
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_receipt BEFORE INSERT ON e2ee_pairing_receipts BEGIN SELECT RAISE(ABORT,'synthetic'); END`); err != nil {
		t.Fatal(err)
	}
	inv3, offer3, err := NewPairingInvitation("machine", now)
	if err != nil {
		t.Fatal(err)
	}
	changed = identity
	changed.Device = "new_device"
	request3, err := EncryptPairingIdentity(offer3, changed, private, now)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := registry.Enroll(ctx, inv3, request3, now)
	if err == nil || failed.Confirmation != "" {
		t.Fatal("failed commit returned receipt")
	}
	if _, err := registry.Device(ctx, "new_device"); err == nil {
		t.Fatal("device committed without receipt")
	}
}
