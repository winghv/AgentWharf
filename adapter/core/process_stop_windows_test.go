//go:build windows

package core

import (
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

// The Job Object binding is the Windows tree-kill guarantee: Kill must reach
// the whole provider tree (cmd.exe shim -> Node bridge -> provider CLI), not
// just the entry process. This test runs a real grandchild tree so the
// property -- grandchild death -- is observed from the OS, not mocked.
// It only executes on a real Windows host; CI compiles it with
// `GOOS=windows go test -c ./adapter/core`.
func TestBindProviderProcessTreeKillsGrandchildren(t *testing.T) {
	if testing.Short() {
		t.Skip("real process tree test")
	}
	shell, err := exec.LookPath("cmd.exe")
	if err != nil {
		t.Fatalf("cmd.exe is unavailable: %v", err)
	}
	// cmd.exe starts a grandchild that outlives its parent unless the Job
	// Object terminates the whole tree.
	cmd := exec.Command(shell, "/d", "/c", "start /b /wait cmd.exe /c ping -n 30 127.0.0.1 >nul")
	applyProviderConsolePolicy(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd.exe child: %v", err)
	}
	killTree, err := bindProviderProcessTree(cmd)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("bindProviderProcessTree: %v", err)
	}

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	if err := killTree(); err != nil {
		t.Fatalf("killTree: %v", err)
	}
	select {
	case err := <-waitErr:
		if err == nil {
			t.Fatal("tree kill reported a clean exit; the child must be terminated by the job")
		}
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.Success() {
			t.Fatalf("wait error = %v, want an abnormal termination", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("child did not exit after the job was terminated")
	}
}

// interruptProcess must classify the Windows "no graceful signal" failure as
// ErrInterruptUnsupported so Stop escalates to Kill, while a child that has
// already exited keeps reporting os.ErrProcessDone unchanged.
func TestInterruptProcessClassifiesUnsupportedSignal(t *testing.T) {
	shell, err := exec.LookPath("cmd.exe")
	if err != nil {
		t.Fatalf("cmd.exe is unavailable: %v", err)
	}
	live := exec.Command(shell, "/d", "/c", "ping -n 30 127.0.0.1 >nul")
	applyProviderConsolePolicy(live)
	if err := live.Start(); err != nil {
		t.Fatalf("start live child: %v", err)
	}
	defer func() { _ = live.Process.Kill() }()

	err = interruptProcess(live.Process)
	if !errors.Is(err, ErrInterruptUnsupported) {
		t.Fatalf("interruptProcess(live child) = %v, want ErrInterruptUnsupported", err)
	}

	done := exec.Command(shell, "/d", "/c", "exit 0")
	if err := done.Start(); err != nil {
		t.Fatalf("start exiting child: %v", err)
	}
	_ = done.Wait()
	if err := interruptProcess(done.Process); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("interruptProcess(exited child) = %v, want os.ErrProcessDone or nil", err)
	}
}
