//go:build windows

package e2ee

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An identity file this user cannot open must not brick the machine: the
// directory belongs to the user, so the file can be renamed aside and a fresh
// identity minted without Administrator rights.
func TestQuarantineForeignIdentityKeepsEvidenceAndFreesTheName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")
	if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("seed identity: %v", err)
	}
	if err := quarantineForeignIdentity(path); err != nil {
		t.Fatalf("quarantineForeignIdentity: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("identity.json still present after quarantine: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	found := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "identity.json.rejected-") {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("quarantined files = %d; want exactly 1 so the rejected key stays available for diagnosis", found)
	}
	// The name must be free for the regenerated identity.
	if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("recreate identity after quarantine: %v", err)
	}
}

// Quarantine must fail loudly rather than pretend, so a directory the user
// cannot rename in still surfaces an actionable error.
func TestQuarantineForeignIdentityFailsWhenFileIsGone(t *testing.T) {
	if err := quarantineForeignIdentity(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("quarantine must report an error when the file does not exist")
	}
}
