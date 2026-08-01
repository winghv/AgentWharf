//go:build linux

package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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

func TestLinuxProcessOwnershipQuarantineIsDurableAcrossOperations(t *testing.T) {
	owner, root := newLinuxOwnershipForTest(t)
	if err := owner.Quarantine(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Quarantine() error = %v, want quarantined", err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatalf("read quarantined manifest: %v", err)
	}
	if !bytes.Contains(manifest, []byte(`"state":"quarantine"`)) {
		t.Fatalf("quarantined manifest = %s", manifest)
	}
	if err := owner.PrepareStart(context.Background(), 1); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("PrepareStart() after quarantine error = %v", err)
	}
	if err := owner.AbortStart(context.Background(), 1); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("AbortStart() after quarantine error = %v", err)
	}
	if err := owner.Quiesce(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Quiesce() after quarantine error = %v", err)
	}
	if owner.Quiescent() {
		t.Fatal("quarantined owner reported quiescence")
	}
}

func TestLinuxProcessOwnershipQuarantinePersistsTrackedRecords(t *testing.T) {
	owner, root := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	provider := exec.Command("sleep", "30")
	if err := provider.Start(); err != nil {
		t.Fatalf("provider.Start() error = %v", err)
	}
	defer func() {
		_ = provider.Process.Kill()
		_ = provider.Wait()
	}()
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: provider.Process.Pid}); err != nil {
		t.Fatalf("ObserveStarted() error = %v", err)
	}
	if err := owner.Quarantine(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Quarantine() error = %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read ownership root: %v", err)
	}
	records := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "record-") {
			records++
		}
	}
	if records == 0 {
		t.Fatal("quarantine persisted no durable process records")
	}
	restarted, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
		Root: root, ProviderUID: linuxOwnershipProviderUID(), CleanupTimeout: time.Second,
	})
	if !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("restart error = %v, want retained quarantine", err)
	}
	if restarted == nil || restarted.Quiescent() {
		t.Fatal("restarted quarantined owner reported quiescence")
	}
}

func TestLinuxProcessOwnershipReconcilesExitedPreviousTree(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	provider := exec.Command("sleep", "0.05")
	if err := provider.Start(); err != nil {
		t.Fatalf("provider.Start() error = %v", err)
	}
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: provider.Process.Pid}); err != nil {
		_ = provider.Process.Kill()
		_ = provider.Wait()
		t.Fatalf("ObserveStarted() error = %v", err)
	}
	_ = provider.Wait()
	owner.mu.Lock()
	err := owner.reconcilePreviousLocked()
	state := owner.state
	owner.mu.Unlock()
	if err != nil {
		t.Fatalf("reconcilePreviousLocked() error = %v", err)
	}
	if state != "clean" {
		t.Fatalf("state after reconcile = %q, want clean", state)
	}
}

func TestLinuxProcessOwnershipReconcileIsUncertainWhileTreeRuns(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	owner.timeout = 200 * time.Millisecond
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	provider := exec.Command("sleep", "30")
	if err := provider.Start(); err != nil {
		t.Fatalf("provider.Start() error = %v", err)
	}
	defer func() {
		_ = provider.Process.Kill()
		_ = provider.Wait()
	}()
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: provider.Process.Pid}); err != nil {
		t.Fatalf("ObserveStarted() error = %v", err)
	}
	owner.mu.Lock()
	err := owner.reconcilePreviousLocked()
	owner.mu.Unlock()
	if !errors.Is(err, ErrProcessOwnershipUncertain) {
		t.Fatalf("reconcilePreviousLocked() error = %v, want uncertainty for a live tree", err)
	}
}

func TestLinuxProcessOwnershipReconcileRejectsUnexpectedState(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	owner.mu.Lock()
	owner.state = "prepared"
	err := owner.reconcilePreviousLocked()
	owner.mu.Unlock()
	if !errors.Is(err, ErrProcessOwnershipUncertain) {
		t.Fatalf("reconcilePreviousLocked() error = %v, want uncertainty", err)
	}
}

func TestLinuxProcessOwnershipPrepareRejectsForeignManifest(t *testing.T) {
	owner, root := newLinuxOwnershipForTest(t)
	foreign := linuxOwnershipManifest{
		Schema: ownershipManifestSchema, Version: 7,
		Runtime: "0123456789abcdef0123456789abcdef", State: "clean",
		Records: []linuxProcessRecord{},
	}
	if err := writeOwnershipManifest(root, foreign); err != nil {
		t.Fatalf("writeOwnershipManifest() error = %v", err)
	}
	if err := owner.PrepareStart(context.Background(), 1); !errors.Is(err, ErrProcessOwnershipUncertain) {
		t.Fatalf("PrepareStart() error = %v, want uncertainty for foreign manifest", err)
	}
}

func TestLinuxProcessOwnershipPrepareHonorsContextCancellation(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := owner.PrepareStart(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("PrepareStart(canceled) error = %v, want context cancellation", err)
	}
}

func TestLinuxProcessOwnershipRejectsInvalidStartObservations(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: 0}); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("ObserveStarted(pid 0) error = %v, want unavailable", err)
	}
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 2, PID: os.Getpid()}); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("ObserveStarted(wrong attempt) error = %v, want quarantine", err)
	}
}

func TestLinuxProcessOwnershipAbortIgnoresMismatchedAttempt(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	if err := owner.AbortStart(context.Background(), 2); err != nil {
		t.Fatalf("AbortStart(mismatched attempt) error = %v, want nil no-op", err)
	}
	if owner.Quiescent() {
		t.Fatal("mismatched abort cleared the prepared start")
	}
	if err := owner.AbortStart(context.Background(), 1); err != nil {
		t.Fatalf("AbortStart(matching attempt) error = %v", err)
	}
	if !owner.Quiescent() {
		t.Fatal("matching abort did not restore quiescence")
	}
}

func TestLinuxProcessOwnershipQuiesceFromCleanAndInvalidStates(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	if err := owner.Quiesce(context.Background()); err != nil {
		t.Fatalf("Quiesce() from clean state error = %v", err)
	}
	if !owner.Quiescent() {
		t.Fatal("clean owner did not prove quiescence")
	}
	owner.mu.Lock()
	owner.state, owner.quiescent = "started", false
	owner.tracked = make(map[linuxProcessIdentity]*linuxTrackedProcess)
	owner.mu.Unlock()
	if err := owner.Quiesce(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Quiesce() with lost tracking error = %v, want quarantine", err)
	}
}

func TestLinuxProcessOwnershipBoundsTrackedTree(t *testing.T) {
	root := filepath.Join(t.TempDir(), "adapter-state")
	providerUID := linuxOwnershipProviderUID()
	if err := InitializeLinuxProcessOwnershipRoot(root, providerUID); err != nil {
		t.Fatalf("InitializeLinuxProcessOwnershipRoot() error = %v", err)
	}
	owner, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
		Root: root, ProviderUID: providerUID, CleanupTimeout: 2 * time.Second, MaxTrackedProcs: 1,
	})
	if err != nil {
		t.Fatalf("NewLinuxProcessTreeOwnership() error = %v", err)
	}
	if err := owner.PrepareStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareStart() error = %v", err)
	}
	provider := exec.Command("sh", "-c", "sleep 30 & wait")
	if err := provider.Start(); err != nil {
		t.Fatalf("provider.Start() error = %v", err)
	}
	defer func() {
		_ = provider.Process.Kill()
		_ = provider.Wait()
	}()
	time.Sleep(100 * time.Millisecond)
	if err := owner.ObserveStarted(context.Background(), ProcessEvent{Type: ProcessEventStarted, Attempt: 1, PID: provider.Process.Pid}); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("ObserveStarted() over bound error = %v, want quarantine", err)
	}
}

func TestLinuxOwnershipManifestValidationFailsClosed(t *testing.T) {
	recordID := "1-2"
	recordDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(recordID)))
	tests := []struct {
		name     string
		manifest string
	}{
		{name: "malformed json", manifest: `{`},
		{name: "wrong schema", manifest: `{"schema":2,"version":1,"runtime":"r","state":"clean","records":[]}`},
		{name: "zero version", manifest: `{"schema":1,"version":0,"runtime":"r","state":"clean","records":[]}`},
		{name: "empty runtime", manifest: `{"schema":1,"version":1,"runtime":"","state":"clean","records":[]}`},
		{name: "unknown state", manifest: `{"schema":1,"version":1,"runtime":"r","state":"weird","records":[]}`},
		{name: "bad record digest", manifest: `{"schema":1,"version":1,"runtime":"r","state":"started","records":[{"id":"1-2","pid":1,"start_time":2,"digest":"00"}]}`},
		{name: "clean with records", manifest: `{"schema":1,"version":1,"runtime":"r","state":"clean","records":[{"id":"` + recordID + `","pid":1,"start_time":2,"digest":"` + recordDigest + `"}]}`},
		{name: "record without record file", manifest: `{"schema":1,"version":1,"runtime":"r","state":"started","records":[{"id":"` + recordID + `","pid":1,"start_time":2,"digest":"` + recordDigest + `"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "adapter-state")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatalf("mkdir root: %v", err)
			}
			if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte(test.manifest), 0600); err != nil {
				t.Fatalf("write manifest: %v", err)
			}
			_, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
				Root: root, ProviderUID: linuxOwnershipProviderUID(), CleanupTimeout: time.Second,
			})
			if !errors.Is(err, ErrProcessOwnershipUncertain) {
				t.Fatalf("NewLinuxProcessTreeOwnership() error = %v, want uncertainty", err)
			}
		})
	}
}

func TestLinuxOwnershipRecordFilesMustMatchManifest(t *testing.T) {
	recordID := "1-2"
	recordDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(recordID)))
	startedManifest := `{"schema":1,"version":1,"runtime":"r","state":"started","records":[{"id":"` + recordID + `","pid":1,"start_time":2,"digest":"` + recordDigest + `"}]}`

	t.Run("stray record file", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "adapter-state")
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatalf("mkdir root: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte(`{"schema":1,"version":1,"runtime":"r","state":"clean","records":[]}`), 0600); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "record-stray.json"), []byte(`{"version":1,"record":{"id":"stray","pid":1,"start_time":2,"digest":"00"}}`), 0600); err != nil {
			t.Fatalf("write stray record: %v", err)
		}
		_, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
			Root: root, ProviderUID: linuxOwnershipProviderUID(), CleanupTimeout: time.Second,
		})
		if !errors.Is(err, ErrProcessOwnershipUncertain) {
			t.Fatalf("NewLinuxProcessTreeOwnership() error = %v, want uncertainty", err)
		}
	})

	t.Run("record file version mismatch", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "adapter-state")
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatalf("mkdir root: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte(startedManifest), 0600); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		stale := `{"version":9,"record":{"id":"` + recordID + `","pid":1,"start_time":2,"digest":"` + recordDigest + `"}}`
		if err := os.WriteFile(filepath.Join(root, "record-"+recordID+".json"), []byte(stale), 0600); err != nil {
			t.Fatalf("write stale record: %v", err)
		}
		_, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
			Root: root, ProviderUID: linuxOwnershipProviderUID(), CleanupTimeout: time.Second,
		})
		if !errors.Is(err, ErrProcessOwnershipUncertain) {
			t.Fatalf("NewLinuxProcessTreeOwnership() error = %v, want uncertainty", err)
		}
	})
}

func TestLinuxOwnershipRootProtectionFailsClosed(t *testing.T) {
	if err := InitializeLinuxProcessOwnershipRoot("", 1234); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("empty root error = %v, want unavailable", err)
	}
	if err := InitializeLinuxProcessOwnershipRoot(filepath.Join(t.TempDir(), "r"), 0); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("zero provider UID error = %v, want unavailable", err)
	}
	if err := InitializeLinuxProcessOwnershipRoot(filepath.Join(t.TempDir(), "r"), uint32(os.Getuid())); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("provider UID equal to adapter UID error = %v, want unavailable", err)
	}
	loose := filepath.Join(t.TempDir(), "loose")
	if err := os.MkdirAll(loose, 0700); err != nil {
		t.Fatalf("mkdir loose root: %v", err)
	}
	if err := os.Chmod(loose, 0755); err != nil {
		t.Fatalf("chmod loose root: %v", err)
	}
	if err := InitializeLinuxProcessOwnershipRoot(loose, linuxOwnershipProviderUID()); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("loose permissions error = %v, want unavailable", err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0700); err != nil {
		t.Fatalf("write file root: %v", err)
	}
	if _, err := NewLinuxProcessTreeOwnership(LinuxProcessOwnershipConfig{
		Root: file, ProviderUID: linuxOwnershipProviderUID(), CleanupTimeout: time.Second,
	}); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("non-directory root error = %v, want unavailable", err)
	}
}

func TestSortedPIDsOrdersIdentities(t *testing.T) {
	processes := map[linuxProcessIdentity]struct{}{
		{PID: 30, StartTime: 1}: {},
		{PID: 4, StartTime: 2}:  {},
		{PID: 19, StartTime: 3}: {},
	}
	got := sortedPIDs(processes)
	want := []int{4, 19, 30}
	if len(got) != len(want) {
		t.Fatalf("sortedPIDs() = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("sortedPIDs() = %v, want %v", got, want)
		}
	}
}

func TestReadLinuxProcessStatRejectsMissingProcess(t *testing.T) {
	if _, _, _, err := readLinuxProcessStat(0); err == nil {
		t.Fatal("readLinuxProcessStat(0) succeeded for a missing process")
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
