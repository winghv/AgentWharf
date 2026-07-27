package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

// TestAttentionBackfillMarksJoinPendingBlocker exercises the
// backfillAttentionSession switch branch for status = "join_pending",
// which is otherwise unreached by the existing "queued" and
// "outcome_unknown" attachment fixtures: it must classify the blocker as
// AttentionBlockerQueued while leaving Reason and BlockingSessionID unset
// (unlike the "queued" status, which populates both).
func TestAttentionBackfillMarksJoinPendingBlocker(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_join_pending_bootstrap'), ('ses_join_pending_target')`); err != nil {
		t.Fatalf("insert join-pending sessions: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attachments (attach_id, bootstrap_session_id, target_session_id, status, delivery_state, delivery_version, expires_at, target_credential_lineage_ref)
VALUES ('att_join_pending', 'ses_join_pending_bootstrap', 'ses_join_pending_target', 'join_pending', 'pending', 0, clock_timestamp() + interval '10 seconds', 'lineage_join_pending')`); err != nil {
		t.Fatalf("insert join-pending attachment: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Processed != 1 {
		t.Fatalf("join-pending backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_join_pending_target"})
	if err != nil || len(snapshot) != 1 || snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerQueued ||
		snapshot[0].Blocker.Reason != nil || snapshot[0].Blocker.BlockingSessionID != nil {
		t.Fatalf("join-pending blocker summary = %+v, %v", snapshot, err)
	}
}

// TestAttentionBackfillMarksReauthorizationRequiredBlocker exercises the
// backfillAttentionSession switch branch for status =
// "reauthorization_required" with delivery_state = "pending" (not
// "outcome_unknown", which would instead hit the earlier outcome-unknown
// short-circuit and never reach this switch case).
func TestAttentionBackfillMarksReauthorizationRequiredBlocker(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_reauth_bootstrap'), ('ses_reauth_target')`); err != nil {
		t.Fatalf("insert reauthorization sessions: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attachments (attach_id, bootstrap_session_id, target_session_id, status, delivery_state, delivery_version, target_credential_lineage_ref)
VALUES ('att_reauth', 'ses_reauth_bootstrap', 'ses_reauth_target', 'reauthorization_required', 'pending', 0, 'lineage_reauth')`); err != nil {
		t.Fatalf("insert reauthorization attachment: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Processed != 1 {
		t.Fatalf("reauthorization backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_reauth_target"})
	if err != nil || len(snapshot) != 1 || snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerReauthorizationRequired {
		t.Fatalf("reauthorization blocker summary = %+v, %v", snapshot, err)
	}
}

// TestAttentionBackfillMarksCanceledBlocker exercises the
// backfillAttentionSession switch branch for status = "canceled" (with
// delivery_state = "pending" so the outcome-unknown short-circuit is not
// hit), which must classify the blocker as AttentionBlockerNewRunRequired.
func TestAttentionBackfillMarksCanceledBlocker(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_canceled_bootstrap'), ('ses_canceled_target')`); err != nil {
		t.Fatalf("insert canceled sessions: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attachments (attach_id, bootstrap_session_id, target_session_id, status, delivery_state, delivery_version, canceled_at, target_credential_lineage_ref)
VALUES ('att_canceled', 'ses_canceled_bootstrap', 'ses_canceled_target', 'canceled', 'pending', 0, statement_timestamp(), 'lineage_canceled')`); err != nil {
		t.Fatalf("insert canceled attachment: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Processed != 1 {
		t.Fatalf("canceled backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_canceled_target"})
	if err != nil || len(snapshot) != 1 || snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerNewRunRequired {
		t.Fatalf("canceled blocker summary = %+v, %v", snapshot, err)
	}
}

// TestAttentionBackfillCheckpointStoreRejectsRelativePath exercises
// checkpointDir's "must be absolute" rejection, reached from both Load and
// Save before any filesystem access happens.
func TestAttentionBackfillCheckpointStoreRejectsRelativePath(t *testing.T) {
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: "relative/checkpoint.json"}
	if _, err := storeFile.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative checkpoint path Load = %v, want 'must be absolute' error", err)
	}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_relative"}); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative checkpoint path Save = %v, want 'must be absolute' error", err)
	}
}

// TestAttentionBackfillCheckpointSaveRejectsInvalidCursor exercises Save's
// own validAttentionCheckpoint rejection (distinct from Load's equivalent
// check, which is covered by TestAttentionBackfillCheckpointStoreRejectsUntrustedInput):
// an AfterSessionID containing a NUL byte must never reach the filesystem.
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

// TestAttentionBackfillCheckpointStoreLoadsMissingFileAsZero exercises the
// real "no checkpoint saved yet" semantic: Load on a well-formed, trusted,
// but nonexistent checkpoint file must return the zero checkpoint and no
// error (not fail closed), so that a fresh backfill run starts from the
// beginning of the session ID keyspace.
func TestAttentionBackfillCheckpointStoreLoadsMissingFileAsZero(t *testing.T) {
	path := t.TempDir() + "/does-not-exist.json"
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: path}
	got, err := storeFile.Load(context.Background())
	if err != nil {
		t.Fatalf("load missing checkpoint: %v", err)
	}
	if got != (postgres.AttentionBackfillCheckpoint{}) {
		t.Fatalf("load missing checkpoint = %+v, want zero value", got)
	}
}

// TestAttentionBackfillCheckpointStoreRejectsWritableDirectory exercises
// checkpointDir's directory-trust check: a checkpoint directory that is
// group- or world-writable must be rejected fail-closed even though the
// checkpoint file itself is trustworthy, since another principal could
// replace the file out from under the reader.
func TestAttentionBackfillCheckpointStoreRejectsWritableDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	storeFile := postgres.FileAttentionBackfillCheckpointStore{Path: path}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_dir_trust"}); err != nil {
		t.Fatalf("save checkpoint before loosening directory permissions: %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod checkpoint directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := storeFile.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "not trusted") {
		t.Fatalf("load with world-writable checkpoint directory = %v, want 'not trusted' error", err)
	}
	if err := storeFile.Save(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_dir_trust_2"}); err == nil || !strings.Contains(err.Error(), "not trusted") {
		t.Fatalf("save with world-writable checkpoint directory = %v, want 'not trusted' error", err)
	}
}

// TestAttentionBackfillCheckpointStoreRejectsWorldReadableFile exercises
// Load's file-trust check: a checkpoint file that is readable by group or
// other (permission bits outside 0o077 mask, i.e. anything beyond
// owner-only) must be rejected fail-closed even though its directory and
// content are otherwise trustworthy.
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
		t.Fatalf("load world-readable checkpoint file = %v, want 'not trusted' error", err)
	}
}
