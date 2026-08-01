//go:build !linux

package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/store/postgres"
)

func TestAttentionBackfillFileCheckpointStoreRejectsUnsupportedPlatform(t *testing.T) {
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: "/tmp/checkpoint.json"}
	if _, err := storeFile.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "require linux") {
		t.Fatalf("Load unsupported platform error = %v", err)
	}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{}); err == nil || !strings.Contains(err.Error(), "require linux") {
		t.Fatalf("Save unsupported platform error = %v", err)
	}
}
