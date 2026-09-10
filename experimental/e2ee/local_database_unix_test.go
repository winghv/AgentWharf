//go:build linux || darwin

package e2ee

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalDatabasePersistsAndRejectsUnsafeFiles(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	database, err := OpenLocalDatabase(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `CREATE TABLE test_state(value TEXT); INSERT INTO test_state VALUES('durable')`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = OpenLocalDatabase(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	var value string
	if err := database.QueryRowContext(ctx, `SELECT value FROM test_state`).Scan(&value); err != nil || value != "durable" {
		t.Fatalf("reopen: %v", err)
	}
	_ = database.Close()
	if err := os.Chmod(filepath.Join(directory, "endpoint.db"), 0644); err != nil {
		t.Fatal(err)
	}
	if db, err := OpenLocalDatabase(ctx, directory); err == nil {
		db.Close()
		t.Fatal("public database accepted")
	}
}

func TestLocalDatabaseRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(directory, "endpoint.db")); err != nil {
		t.Fatal(err)
	}
	if db, err := OpenLocalDatabase(context.Background(), directory); err == nil {
		db.Close()
		t.Fatal("symlink accepted")
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "unchanged" {
		t.Fatal("symlink target altered")
	}
}
