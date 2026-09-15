//go:build windows

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLockFileExclusiveSerializesTheDaemonLock guards the Windows daemon lock.
// The call previously passed a NULL OVERLAPPED to LockFileEx, which faults
// inside kernel32 instead of reporting the lock, so the daemon crashed right
// after pairing. This test exercises the real API: the first holder succeeds and
// a second handle must be refused.
func TestLockFileExclusiveSerializesTheDaemonLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serve.lock")
	first, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err := lockFileExclusive(first); err != nil {
		t.Fatalf("first lock: %v", err)
	}
	second, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := lockFileExclusive(second); err == nil {
		t.Fatal("a second exclusive daemon lock was granted")
	}
}
