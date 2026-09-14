package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"path/filepath"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

// A trust-off rotation replaces a session's key and drops the terminal's grant,
// so the original terminal-signed launch carrier can no longer validate. The
// endpoint must still recover the Session by re-sealing the launch it validated
// earlier under its own current control grant.
func TestResolveEncryptedLaunchResealsAfterKeyRotation(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	runtime, err := openMachineE2EERuntime(ctx, directory, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()

	clientPublic, clientPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key1", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: clientPublic, Control: true}}); err != nil {
		t.Fatal(err)
	}
	key, err := runtime.vault.Load(ctx, "session", "key1")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	payload := json.RawMessage(`{"content":[],"launch":{"provider":"test-provider","working_directory":"/synthetic/private","model_id":"reasoning","permission_mode_id":"ask"}}`)
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key1", Sender: "client", MessageID: "launch-1", Type: "session.send"}, key, clientPrivate, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key1", "sender": "client", "message_id": "launch-1", "type": "session.send", "packet": packet})
	if err != nil {
		t.Fatal(err)
	}

	handoff := machineServeDispatch{SessionID: "session", Provider: "test-provider", EncryptedFirstInstruction: string(wire)}
	var cfg wrapConfig
	resolved, err := resolveEncryptedLaunch(ctx, runtime, handoff, &cfg, io.Discard)
	if err != nil {
		t.Fatal("initial launch", err)
	}
	if cfg.WorkingDirectory != "/synthetic/private" || cfg.LaunchSettings.ModelID != "reasoning" || cfg.LaunchSettings.PermissionModeID != "ask" {
		t.Fatalf("initial launch configuration = %+v", cfg)
	}
	if resolved.EncryptedFirstInstruction != string(wire) {
		t.Fatal("initial launch carrier changed")
	}
	if provider, _, err := runtime.loadLaunchRecovery(ctx, "session"); err != nil || provider != "test-provider" {
		t.Fatalf("validated launch was not retained for recovery: provider=%q err=%v", provider, err)
	}

	// Trust-off revocation: the terminal grant is removed and the session key
	// advances to a new epoch whose control grant belongs to the machine.
	journal, err := e2ee.NewCommandJournal(ctx, runtime.database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.database.ExecContext(ctx, `DELETE FROM e2ee_local_grants WHERE session='session'`); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.vault.RotateSessionAfterRevoke(ctx, journal, "session"); err != nil {
		t.Fatal(err)
	}
	if _, err := applyEncryptedLaunchConfiguration(ctx, runtime, handoff, &wrapConfig{}); err == nil {
		t.Fatal("rotated session still accepted the original terminal carrier")
	}

	var recovered wrapConfig
	recovery := machineServeDispatch{SessionID: "session", Provider: "test-provider"}
	resealed, err := resolveEncryptedLaunch(ctx, runtime, recovery, &recovered, io.Discard)
	if err != nil {
		t.Fatal("recovery after rotation", err)
	}
	if recovered.WorkingDirectory != "/synthetic/private" || recovered.LaunchSettings.ModelID != "reasoning" || recovered.LaunchSettings.PermissionModeID != "ask" {
		t.Fatalf("recovered launch configuration = %+v", recovered)
	}
	if resealed.EncryptedFirstInstruction == "" || resealed.EncryptedFirstInstruction == string(wire) {
		t.Fatal("recovery did not re-seal the launch as the endpoint")
	}
	if _, err := applyEncryptedLaunchConfiguration(ctx, runtime, resealed, &wrapConfig{}); err != nil {
		t.Fatal("re-sealed carrier did not validate against the machine control grant", err)
	}
}

// Without a validated launch recorded before rotation there is no endpoint-owned
// configuration to re-seal, so recovery must fail rather than guess.
func TestResolveEncryptedLaunchWithoutRecordedConfigurationFailsClosed(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	runtime, err := openMachineE2EERuntime(ctx, directory, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	recovery := machineServeDispatch{SessionID: "session", Provider: "test-provider"}
	if _, err := resolveEncryptedLaunch(ctx, runtime, recovery, &wrapConfig{}, io.Discard); err == nil {
		t.Fatal("recovery without a recorded launch configuration accepted")
	}
}
