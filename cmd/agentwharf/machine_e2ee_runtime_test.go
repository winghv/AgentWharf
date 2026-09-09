package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

func TestMachineRuntimeOpenDoesNotAuthorizeSession(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	for i := 0; i < 2; i++ {
		runtime, err := openMachineE2EERuntime(ctx, directory, "machine", "account")
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.requireSession(ctx, "session"); err == nil {
			t.Fatal("unprovisioned session passed startup check")
		}
		if _, err := runtime.vault.Load(ctx, "session", "key"); err != e2ee.ErrUnauthorized {
			t.Fatalf("runtime fabricated key: %v", err)
		}
		if _, err := runtime.registry.Device(ctx, "device"); err != e2ee.ErrUnauthorized {
			t.Fatalf("runtime fabricated device: %v", err)
		}
		if err := runtime.database.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, binding := range [][2]string{{"machine", "other-account"}, {"other-machine", "account"}} {
		runtime, err := openMachineE2EERuntime(ctx, directory, binding[0], binding[1])
		if err == nil {
			_ = runtime.database.Close()
			t.Fatal("foreign binding reopened endpoint database")
		}
	}
	runtime, err := openMachineE2EERuntime(ctx, directory, "machine", "account")
	if err != nil {
		t.Fatalf("failed foreign open damaged binding: %v", err)
	}
	defer runtime.database.Close()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "device", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.requireSession(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.requireSession(ctx, "other-session"); err == nil {
		t.Fatal("cross-session startup allowed")
	}
	if _, err := runtime.database.ExecContext(ctx, `UPDATE e2ee_session_keys SET ciphertext='invalid' WHERE session='session'`); err != nil {
		t.Fatal(err)
	}
	if err := runtime.requireSession(ctx, "session"); err == nil {
		t.Fatal("corrupt key passed startup check")
	}
}
