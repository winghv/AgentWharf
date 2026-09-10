package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSignedSessionInitialization(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.db"))
	machine, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewLocalIdentity()
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
	request, err := SignSessionInitialization(SessionInitialization{Machine: "machine", Account: "account", Session: "session", KeyID: "key", Device: client.Device}, ed25519.NewKeyFromSeed(client.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSessionInitialization(raw)
	if err != nil || decoded != request {
		t.Fatal("valid request decode failed", err)
	}
	for _, invalid := range [][]byte{
		append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"session":"other"}`)...),
		append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"extra":true}`)...),
		append(append([]byte(nil), raw...), []byte(`{}`)...),
		bytes.Repeat([]byte(" "), 2049),
	} {
		if _, err := DecodeSessionInitialization(invalid); err == nil {
			t.Fatal("ambiguous initialization accepted")
		}
	}
	if _, err := executor.InitializeSession(ctx, registry, request); err == nil {
		t.Fatal("unpaired device initialized session")
	}
	public, err := client.Public()
	if err != nil {
		t.Fatal(err)
	}
	invitation, offer, err := NewPairingInvitation("machine", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := EncryptPairingIdentity(offer, public, ed25519.NewKeyFromSeed(client.SigningSeed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Enroll(ctx, invitation, enrollment, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.VerifySessionInitialization(ctx, request); err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.Session = "other"
	if _, err := registry.VerifySessionInitialization(ctx, changed); err == nil {
		t.Fatal("read-only verifier accepted changed request")
	}
	if _, err := executor.InitializeSession(ctx, registry, changed); err == nil {
		t.Fatal("changed signed session accepted")
	}
	machinePublic, err := machine.Public()
	if err != nil {
		t.Fatal(err)
	}
	wrappingPublic, err := decode(machinePublic.WrappingKey, 65, 65)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_init_key BEFORE INSERT ON e2ee_session_keys BEGIN SELECT RAISE(ABORT,'synthetic key failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.InitializeSession(ctx, registry, request); err == nil {
		t.Fatal("failed key write accepted")
	}
	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM e2ee_local_sessions)+(SELECT count(*) FROM e2ee_local_grants)+(SELECT count(*) FROM e2ee_local_key_epochs)`).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("initialization leaked authority: %d %v", remaining, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_init_key`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	keys := make(chan []byte, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped, err := executor.InitializeSession(ctx, registry, request)
			if err != nil {
				t.Error(err)
				return
			}
			key, err := UnwrapKey(WrapContext{"session", "key", machine.Device, client.Device}, client.WrappingPrivate, wrappingPublic, wrapped)
			if err != nil {
				t.Error(err)
				return
			}
			keys <- key
		}()
	}
	wg.Wait()
	close(keys)
	var first []byte
	for key := range keys {
		if first == nil {
			first = key
		} else if !bytes.Equal(first, key) {
			t.Fatal("concurrent initialization generated different keys")
		}
	}
	if first == nil {
		t.Fatal("no initialization succeeded")
	}
	for i := 0; i < 2; i++ {
		wrapped, err := executor.InitializeSession(ctx, registry, request)
		if err != nil {
			t.Fatal(err)
		}
		key, err := UnwrapKey(WrapContext{"session", "key", machine.Device, client.Device}, client.WrappingPrivate, wrappingPublic, wrapped)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = key
		} else if !bytes.Equal(first, key) {
			t.Fatal("retry rotated key")
		}
	}
	changed = request
	changed.KeyID = "replacement"
	changed, err = SignSessionInitialization(changed, ed25519.NewKeyFromSeed(client.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.InitializeSession(ctx, registry, changed); err == nil {
		t.Fatal("initialization replaced existing session")
	}
	verify, err := decode(public.SigningKey, 32, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ReplaceGrants(ctx, "session", "rotated", 1, []DeviceGrant{{DeviceID: client.Device, VerifyKey: verify, Control: false}}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.InitializeSession(ctx, registry, request); err == nil {
		t.Fatal("old initialization restored revoked control")
	}
	var epoch int
	if err := db.QueryRowContext(ctx, `SELECT epoch FROM e2ee_local_sessions WHERE session='session'`).Scan(&epoch); err != nil || epoch != 2 {
		t.Fatalf("replay changed epoch: %d %v", epoch, err)
	}
}
