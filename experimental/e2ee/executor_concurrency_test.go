package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestExecutorRevocationWaitsForDeliveryAndInvalidPacketNeverClaims(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.sqlite"))
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
	private := ed25519.NewKeyFromSeed(identity.SigningSeed)
	public := private.Public().(ed25519.PublicKey)
	if err := executor.ReplaceGrants(ctx, "session", "key_1", 0, []DeviceGrant{{identity.Device, public, true}}); err != nil {
		t.Fatal(err)
	}
	key, err := vault.Load(ctx, "session", "key_1")
	if err != nil {
		t.Fatal(err)
	}
	command := Context{"command", "session", identity.Device, "key_1", "command_1", "permission.respond"}
	envelope, err := Seal(command, key, private, []byte(`{"public":{"request_id":"permission_1","decision":"approve"},"payload":{"request_id":"permission_2","decision":"approve"}}`))
	if err != nil {
		t.Fatal(err)
	}
	packet := ContentPacket{1, PublicMetadata{RequestID: "permission_1", Decision: "approve"}, envelope}
	if _, err := executor.Execute(ctx, command, packet, func(context.Context, json.RawMessage) error { t.Error("invalid packet delivered"); return nil }); err == nil {
		t.Fatal("invalid packet accepted")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM e2ee_local_commands`).Scan(&count); err != nil || count != 0 {
		t.Fatal("invalid packet mutated journal", err)
	}
	payload := json.RawMessage(`{"request_id":"permission_1","decision":"approve"}`)
	packet, err = SealPacket(command, key, private, packet.Public, payload)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := executor.Execute(ctx, command, packet, func(context.Context, json.RawMessage) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("delivery never entered")
	}
	// A canceled revocation waiting for the same lane must not mutate membership.
	cancelled, cancelRevocation := context.WithCancel(ctx)
	cancelRevocation()
	if err := executor.ReplaceGrants(cancelled, "session", "key_2", 1, []DeviceGrant{{identity.Device, public, false}}); err == nil {
		t.Fatal("cancelled revocation committed")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := executor.ReplaceGrants(ctx, "session", "key_2", 1, []DeviceGrant{{identity.Device, public, false}}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(ctx, command, packet, func(context.Context, json.RawMessage) error { t.Error("revoked delivery executed"); return nil }); err == nil {
		t.Fatal("revocation ignored")
	}
}
