package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestAuthenticatedMembershipRotationAndRetry(t *testing.T) {
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
	registry, err := NewDeviceRegistry(ctx, db, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	enroll := func() LocalIdentity {
		identity, err := NewLocalIdentity()
		if err != nil {
			t.Fatal(err)
		}
		public, err := identity.Public()
		if err != nil {
			t.Fatal(err)
		}
		invitation, offer, err := NewPairingInvitation("machine", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		proof, err := EncryptPairingIdentity(offer, public, ed25519.NewKeyFromSeed(identity.SigningSeed), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := registry.Enroll(ctx, invitation, proof, time.Now()); err != nil {
			t.Fatal(err)
		}
		return identity
	}
	owner, viewer := enroll(), enroll()
	init, err := SignSessionInitialization(SessionInitialization{Machine: "machine", Account: "account", Session: "session", KeyID: "key1", Device: owner.Device}, ed25519.NewKeyFromSeed(owner.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.InitializeSession(ctx, registry, init); err != nil {
		t.Fatal(err)
	}
	old, err := vault.Load(ctx, "session", "key1")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(old)
	members := []SessionMember{{Device: owner.Device, Control: true}, {Device: viewer.Device, Control: false}}
	sort.Slice(members, func(i, j int) bool { return members[i].Device < members[j].Device })
	request, err := SignSessionMembershipChange(SessionMembershipChange{Machine: "machine", Account: "account", Session: "session", Device: owner.Device, KeyID: "key2", ExpectedEpoch: 1, Members: members}, ed25519.NewKeyFromSeed(owner.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	request, err = DecodeSessionMembershipChange(raw)
	if err != nil {
		t.Fatal(err)
	}
	forged := request
	forged.KeyID = "substituted"
	if err := executor.ApplyMembership(ctx, registry, forged); err == nil {
		t.Fatal("tampered membership accepted")
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_membership_key BEFORE INSERT ON e2ee_session_keys WHEN NEW.key_id='key2' BEGIN SELECT RAISE(ABORT,'synthetic membership failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := executor.ApplyMembership(ctx, registry, request); err == nil {
		t.Fatal("failed key write reported success")
	}
	var epoch, receipts, grants int
	if err := db.QueryRowContext(ctx, `SELECT epoch,(SELECT count(*) FROM e2ee_membership_receipts),(SELECT count(*) FROM e2ee_local_grants WHERE session='session') FROM e2ee_local_sessions WHERE session='session'`).Scan(&epoch, &receipts, &grants); err != nil || epoch != 1 || receipts != 0 || grants != 1 {
		t.Fatal("failed rotation changed authority", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_membership_key`); err != nil {
		t.Fatal(err)
	}
	wirePacket, err := SealPacket(Context{Scope: "command", Session: "session", Sender: owner.Device, KeyID: "key1", MessageID: "membership", Type: "session.membership.change"}, old, ed25519.NewKeyFromSeed(owner.SigningSeed), PublicMetadata{}, raw)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key1", "sender": owner.Device, "message_id": "membership", "type": "session.membership.change", "packet": wirePacket})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- executor.ApplyMembershipWire(ctx, registry, "session", "membership", wire)
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh, err := vault.Load(ctx, "session", "key2")
	if err != nil || bytes.Equal(fresh, old) {
		t.Fatal("rotation did not create fresh key", err)
	}
	defer clear(fresh)
	keyRequest, err := SignSessionKeyRequest(SessionKeyRequest{Machine: "machine", Account: "account", Session: "session", KeyID: "key2", Device: viewer.Device}, ed25519.NewKeyFromSeed(viewer.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	wrongPurpose, err := SignSessionInitialization(SessionInitialization(keyRequest), ed25519.NewKeyFromSeed(viewer.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.RecoverSessionKey(ctx, registry, SessionKeyRequest(wrongPurpose)); err == nil {
		t.Fatal("initialization signature authorized key recovery")
	}
	wrapped, err := executor.RecoverSessionKey(ctx, registry, keyRequest)
	if err != nil {
		t.Fatal(err)
	}
	public, err := machine.Public()
	if err != nil {
		t.Fatal(err)
	}
	wrapping, err := decode(public.WrappingKey, 65, 65)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := UnwrapKey(WrapContext{Session: "session", KeyID: "key2", Sender: machine.Device, Recipient: viewer.Device}, viewer.WrappingPrivate, wrapping, wrapped)
	if err != nil || !bytes.Equal(opened, fresh) {
		t.Fatal("new member key unavailable", err)
	}
	clear(opened)
	viewerRequest := request
	viewerRequest.Device = viewer.Device
	viewerRequest.KeyID = "key3"
	viewerRequest.ExpectedEpoch = 2
	viewerRequest, err = SignSessionMembershipChange(viewerRequest, ed25519.NewKeyFromSeed(viewer.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.ApplyMembership(ctx, registry, viewerRequest); err == nil {
		t.Fatal("view-only device changed membership")
	}
	original := request
	request.ExpectedEpoch = 2
	request.KeyID = "key3"
	request.Members = []SessionMember{{Device: viewer.Device, Control: true}}
	request, err = SignSessionMembershipChange(request, ed25519.NewKeyFromSeed(owner.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := executor.ApplyMembership(ctx, registry, request); err != nil {
			t.Fatal("self-removal retry", err)
		}
	}
	if err := executor.ApplyMembership(ctx, registry, original); err == nil {
		t.Fatal("old membership receipt accepted after a later epoch")
	}
	removedRequest, err := SignSessionKeyRequest(SessionKeyRequest{Machine: "machine", Account: "account", Session: "session", KeyID: "key3", Device: owner.Device}, ed25519.NewKeyFromSeed(owner.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.RecoverSessionKey(ctx, registry, removedRequest); err == nil {
		t.Fatal("removed controller received new key")
	}
}
