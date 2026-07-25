package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartFixedEntryHealthRequiresRegularMarker(t *testing.T) {
	t.Parallel()
	if _, err := StartFixedEntryHealth(context.Background(), filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("StartFixedEntryHealth() error = nil, want missing marker rejection")
	}
}

func TestStartFixedEntryHealthRejectsSymlinkMarker(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	marker := filepath.Join(dir, "health")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, marker); err != nil {
		t.Fatalf("symlink marker: %v", err)
	}
	if _, err := StartFixedEntryHealth(context.Background(), marker); err == nil {
		t.Fatal("StartFixedEntryHealth() error = nil, want symlink rejection")
	}
}

func TestStartFixedEntryHealthTouchesExistingMarker(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(t.TempDir(), "health")
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	before := time.Now().Add(-time.Minute)
	if err := os.Chtimes(marker, before, before); err != nil {
		t.Fatalf("backdate marker: %v", err)
	}
	stop, err := StartFixedEntryHealth(context.Background(), marker)
	if err != nil {
		t.Fatalf("StartFixedEntryHealth() error = %v", err)
	}
	defer stop()
	if info, err := os.Stat(marker); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !info.ModTime().After(before) {
		t.Fatalf("marker = %v, %v", info, err)
	}
}
