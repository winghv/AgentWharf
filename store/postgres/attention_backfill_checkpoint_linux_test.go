//go:build linux

package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/store/postgres"
)

func TestAttentionBackfillFileCheckpointStore(t *testing.T) {
	path := t.TempDir() + "/checkpoint.json"
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: path}
	want := postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_checkpoint"}
	if err := storeFile.Save(context.Background(), want); err != nil {
		t.Fatalf("save file checkpoint: %v", err)
	}
	got, err := storeFile.Load(context.Background())
	if err != nil || got != want {
		t.Fatalf("load file checkpoint = %+v, %v", got, err)
	}
}

func TestAttentionBackfillCheckpointStoreRejectsUntrustedInput(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/checkpoint.json"
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: path}
	if err := os.WriteFile(dir+"/target", []byte(`{"AfterSessionID":"ses"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir+"/target", path); err != nil {
		t.Fatal(err)
	}
	if _, err := storeFile.Load(context.Background()); err == nil {
		t.Fatal("symlink checkpoint unexpectedly loaded")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 4097), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := storeFile.Load(context.Background()); err == nil {
		t.Fatal("oversized checkpoint unexpectedly loaded")
	}
	if err := os.WriteFile(path, []byte(`{"AfterSessionID":"`+strings.Repeat("x", 256)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := storeFile.Load(context.Background()); err == nil {
		t.Fatal("invalid checkpoint cursor unexpectedly loaded")
	}
}

func TestAttentionBackfillCheckpointStoreRejectsRelativePath(t *testing.T) {
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: "relative/checkpoint.json"}
	if _, err := storeFile.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative checkpoint path Load = %v", err)
	}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_relative"}); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative checkpoint path Save = %v", err)
	}
}

func TestAttentionBackfillCheckpointSaveRejectsInvalidCursor(t *testing.T) {
	path := t.TempDir() + "/checkpoint.json"
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: path}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_bad\x00cursor"}); err == nil {
		t.Fatal("save with NUL-byte cursor unexpectedly succeeded")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("invalid checkpoint save must not create a file, stat err = %v", statErr)
	}
}

func TestAttentionBackfillCheckpointStoreLoadsMissingFileAsZero(t *testing.T) {
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: t.TempDir() + "/does-not-exist.json"}
	got, err := storeFile.Load(context.Background())
	if err != nil || got != (postgres.AttentionBackfillCheckpoint{}) {
		t.Fatalf("load missing checkpoint = %+v, %v", got, err)
	}
}

func TestAttentionBackfillCheckpointStoreRejectsWritableDirectory(t *testing.T) {
	dir := t.TempDir()
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: filepath.Join(dir, "checkpoint.json")}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_dir_trust"}); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod checkpoint directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := storeFile.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "not trusted") {
		t.Fatalf("load with writable directory = %v", err)
	}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_dir_trust_2"}); err == nil || !strings.Contains(err.Error(), "not trusted") {
		t.Fatalf("save with writable directory = %v", err)
	}
}

func TestAttentionBackfillCheckpointStoreRejectsWorldReadableFile(t *testing.T) {
	path := t.TempDir() + "/checkpoint.json"
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: path}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_file_trust"}); err != nil {
		t.Fatalf("save trusted checkpoint: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod checkpoint file: %v", err)
	}
	if _, err := storeFile.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "not trusted") {
		t.Fatalf("load world-readable file = %v", err)
	}
}
