package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestExecutorAuthenticatesClaimsAndDeliversOnlyOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoint.sqlite")
	journal, db := openJournal(t, path)
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
	command := Context{"command", "session", identity.Device, "key_1", "command_1", "session.send"}
	payload := json.RawMessage(`{"content":[{"kind":"text","text":"synthetic instruction"}]}`)
	packet, err := SealPacket(command, key, private, PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	deliver := func(_ context.Context, got json.RawMessage) error {
		calls++
		if string(got) != string(payload) {
			t.Fatal("changed payload")
		}
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM e2ee_local_commands WHERE state='claimed'`).Scan(&count); err != nil || count != 1 {
			t.Fatal("effect before durable reservation", err)
		}
		return nil
	}
	launch := command
	launch.Scope = "launch"
	launchPacket, err := SealPacket(launch, key, private, PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(ctx, launch, launchPacket, deliver); err == nil || calls != 0 {
		t.Fatal("launch authorization accepted as ordinary command")
	}
	result, err := executor.Execute(ctx, command, packet, deliver)
	if err != nil || result.State != "completed" || calls != 1 {
		t.Fatal("delivery failed", result, err)
	}
	db.Close()
	journal, db = openJournal(t, path)
	vault, err = NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	executor, err = NewCommandExecutor(journal, vault)
	if err != nil {
		t.Fatal(err)
	}
	result, err = executor.Execute(ctx, command, packet, deliver)
	if err != nil || result.Execute || calls != 1 {
		t.Fatal("replayed after restart", result, err)
	}
	command.MessageID = "command_2"
	packet, err = SealPacket(command, key, private, PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	result, err = executor.Execute(ctx, command, packet, func(context.Context, json.RawMessage) error { calls++; return errors.New("synthetic provider failure") })
	if err != nil || result.State != "outcome_unknown" {
		t.Fatal("lost ambiguity", result, err)
	}
	result, err = executor.Execute(ctx, command, packet, deliver)
	if err != nil || result.Execute || calls != 2 {
		t.Fatal("retried ambiguous delivery")
	}
	if err := executor.ReplaceGrants(ctx, "session", "key_2", 1, []DeviceGrant{{identity.Device, public, false}}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(ctx, command, packet, deliver); err == nil || calls != 2 {
		t.Fatal("revoked command reached provider")
	}
}
