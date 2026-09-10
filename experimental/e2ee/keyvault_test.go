package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSessionKeyVaultReopenConcurrentAndWrapping(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "keys.sqlite")
	_, db := openJournal(t, path)
	identity, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	keys := make(chan []byte, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := vault.Create(ctx, "session", "epoch_1")
			if err != nil {
				t.Error(err)
				return
			}
			keys <- key
		}()
	}
	wg.Wait()
	close(keys)
	var expected []byte
	count := 0
	for key := range keys {
		if count == 0 {
			expected = key
		} else if !bytes.Equal(key, expected) {
			t.Error("returned losing key")
		}
		count++
	}
	if count != 12 {
		t.Fatal("missing key results")
	}
	if db.Close() != nil {
		t.Fatal("close failed")
	}
	_, db = openJournal(t, path)
	vault, err = NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := vault.Load(ctx, "session", "epoch_1")
	if err != nil || !bytes.Equal(expected, reloaded) {
		t.Fatal("lost durable key", err)
	}
	other, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	otherVault, err := NewSessionKeyVault(ctx, db, other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherVault.Load(ctx, "session", "epoch_1"); err == nil {
		t.Fatal("other device read session key")
	}
	public, err := other.Public()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewDeviceRegistry(ctx, db, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	inv, offer, err := NewPairingInvitation("machine", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	request, err := EncryptPairingIdentity(offer, public, ed25519.NewKeyFromSeed(other.SigningSeed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Enroll(ctx, inv, request, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := vault.WrapForDevice(ctx, "session", "epoch_1", registry, public.Device); err == nil {
		t.Fatal("enrollment alone granted session key")
	}
	journal, err := NewCommandJournal(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	signingPublic, err := decode(public.SigningKey, 32, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.ReplaceGrants(ctx, "session", "epoch_1", 0, []DeviceGrant{{public.Device, signingPublic, false}}); err != nil {
		t.Fatal(err)
	}
	wrapped, err := vault.WrapForDevice(ctx, "session", "epoch_1", registry, public.Device)
	if err != nil {
		t.Fatal(err)
	}
	senderPublic, err := decode(vault.public.WrappingKey, 65, 65)
	if err != nil {
		t.Fatal(err)
	}
	received, err := UnwrapKey(WrapContext{"session", "epoch_1", identity.Device, other.Device}, other.WrappingPrivate, senderPublic, wrapped)
	if err != nil || !bytes.Equal(received, expected) {
		t.Fatal("wrapped recovery key mismatch", err)
	}
	if err := journal.ReplaceGrants(ctx, "session", "epoch_2", 1, []DeviceGrant{{identity.Device, signingPublic, true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := vault.WrapForDevice(ctx, "session", "epoch_1", registry, public.Device); err == nil {
		t.Fatal("old epoch still distributed after rotation")
	}
	if _, err := db.Exec(`UPDATE e2ee_session_keys SET ciphertext='invalid'`); err != nil {
		t.Fatal(err)
	}
	if _, err := vault.Create(ctx, "session", "epoch_1"); err == nil {
		t.Fatal("replaced corrupt key silently")
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_key BEFORE INSERT ON e2ee_session_keys BEGIN SELECT RAISE(ABORT,'synthetic'); END`); err != nil {
		t.Fatal(err)
	}
	key, err := vault.Create(ctx, "session", "epoch_2")
	if err == nil || key != nil {
		t.Fatal("released unpersisted key")
	}
}
