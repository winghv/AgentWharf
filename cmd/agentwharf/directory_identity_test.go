package main

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDirectoryIdentitySeparatesSameBasenamePathsWithoutExposingThem(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x5a}, directoryIdentityKeySize)
	firstPath := filepath.Join("/Users/alice/Projects/first", "superwhv")
	secondPath := filepath.Join("/Users/alice/Projects/second", "superwhv")
	first := directoryIdentityForPath(firstPath, key)
	second := directoryIdentityForPath(secondPath, key)

	if first == "" || second == "" || first == second {
		t.Fatalf("directory identities = %q and %q, want distinct opaque IDs", first, second)
	}
	if again := directoryIdentityForPath(firstPath, key); again != first {
		t.Fatalf("directory identity changed for same path: %q then %q", first, again)
	}
	if !validDirectoryIdentityID(first) || !validDirectoryIdentityID(second) {
		t.Fatalf("directory identities have invalid format: %q %q", first, second)
	}
	if bytes.Contains([]byte(first), []byte("alice")) || bytes.Contains([]byte(second), []byte("Projects")) {
		t.Fatalf("directory identity contains source path data: %q %q", first, second)
	}
}

func TestDirectoryIdentityKeyPersistsWithPrivatePermissions(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "agentwharf", "directory-identity.key")
	first, err := loadOrCreateDirectoryIdentityKey(path)
	if err != nil {
		t.Fatalf("create directory identity key: %v", err)
	}
	second, err := loadOrCreateDirectoryIdentityKey(path)
	if err != nil {
		t.Fatalf("load directory identity key: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("directory identity key changed after reload")
	}
	if len(first) != directoryIdentityKeySize {
		t.Fatalf("key size = %d, want %d", len(first), directoryIdentityKeySize)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat directory identity key: %v", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("directory identity key permissions = %o, want owner-only", info.Mode().Perm())
	}
}

func TestConcurrentDirectoryIdentityKeyCreationConverges(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "agentwharf", "directory-identity.key")
	keys := make([][]byte, 8)
	errs := make([]error, len(keys))
	var workers sync.WaitGroup
	for index := range keys {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			keys[index], errs[index] = loadOrCreateDirectoryIdentityKey(path)
		}(index)
	}
	workers.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("load key in worker %d: %v", index, err)
		}
		if !bytes.Equal(keys[0], keys[index]) {
			t.Fatalf("worker %d loaded a different key", index)
		}
	}
}

func TestDirectoryIdentityRegistryResolvesLocallyAndKeepsPathsOffTheHandle(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(directory, "machine.json"))
	path := filepath.Join(directory, "workspace")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	otherPath := filepath.Join(directory, "another", "workspace")
	if err := os.MkdirAll(otherPath, 0o700); err != nil {
		t.Fatal(err)
	}
	keyPath, err := directoryIdentityKeyFile()
	if err != nil {
		t.Fatal(err)
	}
	key, err := loadOrCreateDirectoryIdentityKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	id := directoryIdentityForPath(path, key)
	otherID := directoryIdentityForPath(otherPath, key)
	if id == otherID {
		t.Fatal("same-basename directories received the same identity")
	}
	if err := rememberDirectoryIdentity(id, path); err != nil {
		t.Fatalf("remember directory identity: %v", err)
	}
	if err := rememberDirectoryIdentity(otherID, otherPath); err != nil {
		t.Fatalf("remember second directory identity: %v", err)
	}
	resolved, err := resolveDirectoryIdentity(id)
	if err != nil || resolved != path {
		t.Fatalf("resolved directory = %q, err=%v, want %q", resolved, err, path)
	}
	resolvedOther, err := resolveDirectoryIdentity(otherID)
	if err != nil || resolvedOther != otherPath {
		t.Fatalf("resolved second directory = %q, err=%v, want %q", resolvedOther, err, otherPath)
	}
	if bytes.Contains([]byte(id), []byte(path)) {
		t.Fatal("directory handle contains the absolute path")
	}
	registryPath, err := directoryIdentityRegistryFile()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("directory identity registry permissions = %o, want owner-only", info.Mode().Perm())
	}
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveDirectoryIdentity(id); err == nil {
		t.Fatal("removed directory identity path resolved successfully")
	}
}

func TestDirectoryIdentityRejectsInvalidKeyOrRelativePath(t *testing.T) {
	t.Parallel()

	if got := directoryIdentityForPath("relative/project", bytes.Repeat([]byte{1}, directoryIdentityKeySize)); got != "" {
		t.Fatalf("relative path identity = %q, want omitted", got)
	}
	if got := directoryIdentityForPath("/absolute/project", []byte("short")); got != "" {
		t.Fatalf("identity with invalid key = %q, want omitted", got)
	}
	if validDirectoryIdentityID("dir_v1_not-an-id") {
		t.Fatal("accepted malformed directory identity")
	}
}
