package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"path/filepath"
	"testing"
)

func TestTrustedInitializationCreatesSessionForAccountTerminal(t *testing.T) {
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
	terminal, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public, err := terminal.Public()
	if err != nil {
		t.Fatal(err)
	}
	request, err := SignTrustedSessionKeyRequest(TrustedSessionKeyRequest{Machine: "machine", Account: "account", Session: "session", KeyID: "key", Device: terminal.Device, SigningKey: public.SigningKey, WrappingKey: public.WrappingKey}, ed25519.NewKeyFromSeed(terminal.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}

	// Trust disabled: rejected and never enrolled.
	if _, err := executor.InitializeSessionTrusted(ctx, registry, request, false, true); err != ErrUnauthorized {
		t.Fatalf("trust disabled error = %v, want ErrUnauthorized", err)
	}
	if _, err := registry.Device(ctx, terminal.Device); err == nil {
		t.Fatal("trust disabled enrolled the device")
	}

	// Trust enabled: the terminal creates the session and receives the key.
	wrapped, err := executor.InitializeSessionTrusted(ctx, registry, request, true, true)
	if err != nil {
		t.Fatal(err)
	}
	sessionKey, err := vault.Load(ctx, "session", "key")
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
	opened, err := UnwrapKey(WrapContext{Session: "session", KeyID: "key", Sender: machinePublic.Device, Recipient: terminal.Device}, terminal.WrappingPrivate, senderPublic, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(opened)
	if !bytes.Equal(opened, sessionKey) {
		t.Fatal("trusted wrapped key does not match session key")
	}
	var control int
	if err := db.QueryRowContext(ctx, "SELECT control FROM e2ee_local_grants WHERE session='session' AND device=?", terminal.Device).Scan(&control); err != nil {
		t.Fatal(err)
	}
	if control != 1 {
		t.Fatal("trusted terminal was not the control creator")
	}
	if err := executor.VerifyInitializedSessionTrusted(ctx, registry, request); err != nil {
		t.Fatalf("completed verification failed: %v", err)
	}
}
