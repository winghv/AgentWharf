//go:build linux

package core

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func linuxOwnershipProviderUID() uint32 {
	uid := os.Getuid()
	if uid == 1 {
		return 2
	}
	return uint32(uid + 1)
}

func newLinuxOwnershipForTest(t *testing.T) (*LinuxProcessTreeOwnership, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "adapter-state")
	providerUID := linuxOwnershipProviderUID()
	if err := InitializeLinuxProcessOwnershipRoot(root, providerUID); err != nil {
		t.Fatalf("InitializeLinuxProcessOwnershipRoot() error = %v", err)
	}
	owner, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
		Root: root, ProviderUID: providerUID, CleanupTimeout: 2 * time.Second, MaxTrackedProcs: 16,
	})
	if err != nil {
		t.Fatalf("NewLinuxProcessTreeOwnership() error = %v", err)
	}
	return owner, root
}

func TestLinuxProcessOwnershipRequiresProtectedRootAndSubreaper(t *testing.T) {
	owner, root := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatalf("read prepared manifest: %v", err)
	}
	if !bytes.Contains(manifest, []byte(`"state":"prepared"`)) {
		t.Fatalf("prepared manifest = %s", manifest)
	}
	for _, forbidden := range []string{"command", "credential", "content", "provider_path"} {
		if strings.Contains(string(manifest), forbidden) {
			t.Fatalf("manifest contains forbidden field %q: %s", forbidden, manifest)
		}
	}
	if err := owner.AbortStart(context.Background(), 1); err != nil {
		t.Fatalf("AbortStart() error = %v", err)
	}
	if !owner.Quiescent() {
		t.Fatal("owner is not quiescent after an aborted start")
	}
}

func TestLinuxProcessOwnershipRefreshUncertaintyQuarantines(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	owner.mu.Lock()
	owner.state, owner.quiescent = "started", false
	err := owner.finalizeQuiescenceLocked(func() error { return ErrProcessOwnershipUncertain })
	owner.mu.Unlock()
	if !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("finalizeQuiescenceLocked() error = %v, want quarantine", err)
	}
	if owner.Quiescent() {
		t.Fatal("refresh uncertainty incorrectly produced a quiescent owner")
	}
}

func TestLinuxProcessOwnershipRejectsUnexpectedRootEntry(t *testing.T) {
	owner, root := newLinuxOwnershipForTest(t)
	if owner == nil {
		t.Fatal("owner is nil")
	}
	if err := os.WriteFile(filepath.Join(root, ".manifest-stale"), []byte("residue"), 0600); err != nil {
		t.Fatalf("write residue: %v", err)
	}
	_, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
		Root: root, ProviderUID: linuxOwnershipProviderUID(), CleanupTimeout: time.Second,
	})
	if !errors.Is(err, ErrProcessOwnershipUncertain) {
		t.Fatalf("NewLinuxProcessTreeOwnership() error = %v, want uncertainty", err)
	}
}

func TestLinuxProcessOwnershipDoesNotResetRecordResidueWithoutManifest(t *testing.T) {
	root := filepath.Join(t.TempDir(), "adapter-state")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "record-stale.json"), []byte(`{"version":1}`), 0600); err != nil {
		t.Fatalf("write record residue: %v", err)
	}
	if err := InitializeLinuxProcessOwnershipRoot(root, linuxOwnershipProviderUID()); !errors.Is(err, ErrProcessOwnershipUncertain) {
		t.Fatalf("InitializeLinuxProcessOwnershipRoot() error = %v, want uncertainty", err)
	}
}

func TestLinuxProcessOwnershipDoesNotCreateManifestInExistingEmptyRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "adapter-state")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := InitializeLinuxProcessOwnershipRoot(root, linuxOwnershipProviderUID()); !errors.Is(err, ErrProcessOwnershipUncertain) {
		t.Fatalf("InitializeLinuxProcessOwnershipRoot() error = %v, want uncertainty", err)
	}
}

func TestLinuxProcessOwnershipTracksAndQuiescesDoubleForkLikeTree(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	provider := exec.Command("sh", "-c", "sleep 30 & wait")
	if err := provider.Start(); err != nil {
		t.Fatalf("provider.Start() error = %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: provider.Process.Pid}); err != nil {
		_ = provider.Process.Kill()
		_ = provider.Wait()
		t.Fatalf("ObserveStarted() error = %v", err)
	}
	if err := owner.Quiesce(context.Background()); err != nil {
		_ = provider.Process.Kill()
		_ = provider.Wait()
		t.Fatalf("Quiesce() error = %v", err)
	}
	_ = provider.Wait()
	if !owner.Quiescent() {
		t.Fatal("owner did not prove quiescence")
	}
}

func TestLinuxProcessOwnershipRefreshesDescendantsBeforeRelease(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	provider := exec.Command("sh", "-c", "sleep 0.2; sleep 30 & wait")
	if err := provider.Start(); err != nil {
		t.Fatalf("provider.Start() error = %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: provider.Process.Pid}); err != nil {
		_ = provider.Process.Kill()
		_ = provider.Wait()
		t.Fatalf("ObserveStarted() error = %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := owner.Quiesce(context.Background()); err != nil {
		_ = provider.Process.Kill()
		_ = provider.Wait()
		t.Fatalf("Quiesce() error = %v", err)
	}
	_ = provider.Wait()
	if !owner.Quiescent() {
		t.Fatal("owner did not prove refreshed descendant quiescence")
	}
}

func TestLinuxProcessOwnershipUsesBoundedKillWhenTermIsIgnored(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	owner.timeout = 300 * time.Millisecond
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	provider := exec.Command("sh", "-c", "trap '' TERM; sleep 30 & wait")
	if err := provider.Start(); err != nil {
		t.Fatalf("provider.Start() error = %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: provider.Process.Pid}); err != nil {
		_ = provider.Process.Kill()
		_ = provider.Wait()
		t.Fatalf("ObserveStarted() error = %v", err)
	}
	if err := owner.Quiesce(context.Background()); err != nil {
		_ = provider.Process.Kill()
		_ = provider.Wait()
		t.Fatalf("Quiesce() error = %v", err)
	}
	_ = provider.Wait()
	if !owner.Quiescent() {
		t.Fatal("owner did not prove bounded KILL quiescence")
	}
}

func TestLinuxProcessOwnershipRestartUncertaintyStaysQuarantined(t *testing.T) {
	owner, root := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	restarted, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
		Root: root, ProviderUID: linuxOwnershipProviderUID(), CleanupTimeout: time.Second,
	})
	if !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("restart error = %v, want ErrProcessOwnershipQuarantined", err)
	}
	if restarted == nil || restarted.Quiescent() {
		t.Fatal("restart uncertainty was not retained as quarantine")
	}
}

func TestLinuxQuiescenceLeaseReleasesOnlyAfterProof(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	lease, err := NewLinuxQuiescenceLease(owner, "generation-1")
	if err != nil {
		t.Fatalf("NewLinuxQuiescenceLease() error = %v", err)
	}
	if err := lease.Release(context.Background()); err == nil {
		t.Fatal("Release() error = nil before quiescence proof")
	}
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	if err := owner.AbortStart(context.Background(), 1); err != nil {
		t.Fatalf("AbortStart() error = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() after proof error = %v", err)
	}
	if lease.Held() {
		t.Fatal("lease remains held after release")
	}
}
