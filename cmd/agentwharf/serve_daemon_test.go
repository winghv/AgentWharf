package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDaemonPIDRoundTrip(t *testing.T) {
	t.Setenv("AGENTWHARF_DAEMON_DIR", t.TempDir())

	if err := writeDaemonPID(4242); err != nil {
		t.Fatalf("writeDaemonPID: %v", err)
	}
	pid, err := readDaemonPID()
	if err != nil || pid != 4242 {
		t.Fatalf("readDaemonPID = %d, %v", pid, err)
	}
	removeDaemonPIDIfOwner(4242)
	if _, err := readDaemonPID(); err != errDaemonNotRunning {
		t.Fatalf("readDaemonPID after remove = %v, want errDaemonNotRunning", err)
	}
}

func TestRemoveDaemonPIDOnlyRemovesCurrentOwner(t *testing.T) {
	t.Setenv("AGENTWHARF_DAEMON_DIR", t.TempDir())
	if err := writeDaemonPID(4242); err != nil {
		t.Fatalf("writeDaemonPID: %v", err)
	}
	removeDaemonPIDIfOwner(4343)
	pid, err := readDaemonPID()
	if err != nil || pid != 4242 {
		t.Fatalf("pid after non-owner cleanup = %d, %v; want 4242", pid, err)
	}
}

func TestServeStatusNotRunning(t *testing.T) {
	t.Setenv("AGENTWHARF_DAEMON_DIR", t.TempDir())
	var out strings.Builder
	if err := statusDaemon(&out); err != nil {
		t.Fatalf("statusDaemon: %v", err)
	}
	if !strings.Contains(out.String(), "not running") {
		t.Fatalf("statusDaemon output = %q", out.String())
	}
}

func TestServeStopNotRunning(t *testing.T) {
	t.Setenv("AGENTWHARF_DAEMON_DIR", t.TempDir())
	var out strings.Builder
	if err := stopDaemon(&out); err != nil {
		t.Fatalf("stopDaemon: %v", err)
	}
	if !strings.Contains(out.String(), "not running") {
		t.Fatalf("stopDaemon output = %q", out.String())
	}
}

func TestServeUnknownArgument(t *testing.T) {
	err := runServeCommand(context.Background(), []string{"bogus"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unknown wharf serve argument") {
		t.Fatalf("runServeCommand error = %v", err)
	}
}

func TestServeForegroundRequiresPairing(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTWHARF_DAEMON_DIR", dir)
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(dir, "machine.json"))
	err := runServeCommand(context.Background(), []string{"--foreground"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not paired") {
		t.Fatalf("runServeCommand(--foreground) error = %v, want pairing guidance", err)
	}
}

func TestServeBackgroundRequiresPairing(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTWHARF_DAEMON_DIR", dir)
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(dir, "machine.json"))
	err := runServeCommand(context.Background(), nil, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not paired") {
		t.Fatalf("runServeCommand() error = %v, want pairing guidance", err)
	}
}

func TestDaemonAlreadyRunningRejectsStalePID(t *testing.T) {
	t.Setenv("AGENTWHARF_DAEMON_DIR", t.TempDir())
	// A pid far above any live process id; Signal(0) reports it absent.
	if err := writeDaemonPID(2147483647); err != nil {
		t.Fatalf("writeDaemonPID: %v", err)
	}
	if daemonAlreadyRunning() {
		t.Fatal("daemonAlreadyRunning = true for a non-existent pid")
	}
}

func TestEnsureBackgroundDaemonNotPairedIsSilent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTWHARF_DAEMON_DIR", dir)
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(dir, "machine.json"))
	var stderr strings.Builder
	ensureBackgroundDaemon(&stderr)
	if stderr.Len() != 0 {
		t.Fatalf("ensureBackgroundDaemon wrote %q, want silence", stderr.String())
	}
}

func TestMachineServePollRetryMessage(t *testing.T) {
	network := errors.New("get cloud api request: Get \"https://cloud.superwhv.me/v1/machine-task-claims/pending\": unexpected EOF")
	got := machineServePollRetryMessage(network, 10*time.Second)
	if !strings.Contains(got, "cloud unreachable") || !strings.Contains(got, "retrying in 10s") {
		t.Fatalf("machineServePollRetryMessage(network) = %q", got)
	}

	auth := errors.New("machine credential rejected; refreshing once")
	gotAuth := machineServePollRetryMessage(auth, 10*time.Second)
	if strings.Contains(gotAuth, "cloud unreachable") || !strings.Contains(gotAuth, "retrying in 10s") {
		t.Fatalf("machineServePollRetryMessage(auth) = %q", gotAuth)
	}
}

func TestServeDaemonLockIsExclusivePerCredential(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTWHARF_DAEMON_DIR", dir)

	credA := filepath.Join(dir, "machine-a.json")
	credB := filepath.Join(dir, "machine-b.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credA)

	lockA, err := acquireServeDaemonLock()
	if err != nil {
		t.Fatalf("acquireServeDaemonLock(A): %v", err)
	}
	// A second daemon for the same credential must fail closed instead of
	// silently stacking beside the first one.
	if _, err := acquireServeDaemonLock(); !errors.Is(err, errServeDaemonLocked) {
		t.Fatalf("second acquire = %v, want errServeDaemonLocked", err)
	}
	// A different credential in the same state directory is a separate daemon.
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credB)
	lockB, err := acquireServeDaemonLock()
	if err != nil {
		t.Fatalf("acquireServeDaemonLock(B): %v", err)
	}
	if err := lockB.Close(); err != nil {
		t.Fatalf("close lock B: %v", err)
	}
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credA)
	if err := lockA.Close(); err != nil {
		t.Fatalf("close lock A: %v", err)
	}
	// A released lock must be acquirable again (daemon restart after stop).
	lockAgain, err := acquireServeDaemonLock()
	if err != nil {
		t.Fatalf("acquireServeDaemonLock after release: %v", err)
	}
	if err := lockAgain.Close(); err != nil {
		t.Fatalf("close reacquired lock: %v", err)
	}
}
