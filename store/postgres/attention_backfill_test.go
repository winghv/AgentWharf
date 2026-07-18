package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

func TestAttentionBackfillBatchCheckpointsAcrossReopen(t *testing.T) {
	dsn, schemaName, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_1", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_2", historicalAttentionEvent{1, "session.state", `{"state":"working"}`, base.Add(time.Second)})
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_3", historicalAttentionEvent{1, "session.state", `{"state":"waiting_permission"}`, base.Add(2 * time.Second)})

	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 2)
	if err != nil {
		t.Fatalf("first attention backfill batch: %v", err)
	}
	if result.Processed != 2 || result.Done || result.Checkpoint.AfterSessionID != "ses_backfill_2" {
		t.Fatalf("first attention backfill result = %+v", result)
	}
	assertAttentionBackfillSnapshot(t, postgres.New(pool), []string{"ses_backfill_1", "ses_backfill_2"}, 2, store.AttentionProjectionComplete)
	if snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_3"}); err != nil || len(snapshot) != 0 {
		t.Fatalf("unprocessed attention snapshot = %+v, %v", snapshot, err)
	}

	pool.Close()
	pool = openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	result, err = postgres.New(pool).BackfillAttentionBatch(context.Background(), result.Checkpoint, 2)
	if err != nil {
		t.Fatalf("resumed attention backfill batch: %v", err)
	}
	if result.Processed != 1 || !result.Done || result.Checkpoint.AfterSessionID != "ses_backfill_3" {
		t.Fatalf("resumed attention backfill result = %+v", result)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_1", "ses_backfill_2", "ses_backfill_3"})
	if err != nil || len(snapshot) != 3 || snapshot[0].LastDurableEventAt == nil {
		t.Fatalf("resumed attention snapshot = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillRejectsUnknownCheckpointCursor(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "zzzz"}, 1); err == nil {
		t.Fatal("unknown checkpoint cursor unexpectedly accepted")
	}
}

func TestAttentionBackfillRejectsKnownHighCheckpointCursor(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_checkpoint_a", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	insertHistoricalAttentionEvents(t, pool, "ses_checkpoint_z", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base.Add(time.Second)})
	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{AfterSessionID: "ses_checkpoint_z"}, 1); err == nil {
		t.Fatal("known high checkpoint cursor unexpectedly skipped pending history")
	}
}

func TestRunAttentionBackfillPersistsMaintenanceCheckpoint(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_runner_1", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	insertHistoricalAttentionEvents(t, pool, "ses_runner_2", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base.Add(time.Second)})
	checkpoints := &memoryAttentionBackfillCheckpointStore{}
	result, err := postgres.New(pool).RunAttentionBackfill(context.Background(), checkpoints, 1)
	if err != nil || !result.Done || checkpoints.saves < 2 || checkpoints.value.AfterSessionID != "ses_runner_2" {
		t.Fatalf("maintenance backfill = %+v, saves=%d, err=%v", result, checkpoints.saves, err)
	}
}

type memoryAttentionBackfillCheckpointStore struct {
	value postgres.AttentionBackfillCheckpoint
	saves int
}

func (s *memoryAttentionBackfillCheckpointStore) Load(context.Context) (postgres.AttentionBackfillCheckpoint, error) {
	return s.value, nil
}

func (s *memoryAttentionBackfillCheckpointStore) Save(_ context.Context, value postgres.AttentionBackfillCheckpoint) error {
	s.value, s.saves = value, s.saves+1
	return nil
}

func TestAttentionBackfillPagesHistoryAndRebuildsLedger(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	events := make([]historicalAttentionEvent, 300)
	for index := range events {
		eventType, payload := "session.message", `{"role":"agent"}`
		if index == 0 {
			eventType, payload = "session.state", `{"state":"ready"}`
		}
		events[index] = historicalAttentionEvent{int64(index + 1), eventType, payload, base.Add(time.Duration(index) * time.Millisecond)}
	}
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_paged", events...)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_command", historicalAttentionEvent{1, "session.message", `{"role":"user"}`, base})
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_pending_commands (session_id, cmd_id, type, event_seq, status, expires_at)
VALUES ('ses_backfill_command', 'cmd_backfill', 'session.send', 1, 'outcome_unknown', clock_timestamp() + interval '10 seconds')
`); err != nil {
		t.Fatalf("insert historical command: %v", err)
	}

	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 2)
	if err != nil || result.Processed != 2 || !result.Done {
		t.Fatalf("paged attention backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_command", "ses_backfill_paged"})
	if err != nil || len(snapshot) != 2 {
		t.Fatalf("paged attention snapshot = %+v, %v", snapshot, err)
	}
	if snapshot[0].SummaryVersion != 2 || snapshot[0].LastClientCommandAt == nil || snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerOutcomeUnknown {
		t.Fatalf("rebuilt command attention = %+v", snapshot[0])
	}
	if snapshot[1].LatestSeq != 300 || snapshot[1].State != "ready" || snapshot[1].StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("paged durable attention = %+v", snapshot[1])
	}
}

func TestAttentionBackfillMarksGapAndCorruptionIncomplete(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_gap",
		historicalAttentionEvent{1, "session.state", `{"state":"starting"}`, base},
		historicalAttentionEvent{2, "session.state", `{"state":"ready"}`, base.Add(time.Second)})
	if _, err := pool.Exec(context.Background(), `DELETE FROM session_events WHERE session_id = 'ses_backfill_gap' AND seq = 1`); err != nil {
		t.Fatalf("delete retained event: %v", err)
	}
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_corrupt", historicalAttentionEvent{1, "session.state", `{"state":"unknown"}`, base})

	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 2); err != nil {
		t.Fatalf("incomplete attention backfill: %v", err)
	}
	assertAttentionBackfillSnapshot(t, postgres.New(pool), []string{"ses_backfill_corrupt", "ses_backfill_gap"}, 2, store.AttentionProjectionIncomplete)
}

func TestAttentionBackfillSkipsCompleteSummary(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_existing", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	storeClock := base.Add(30 * time.Minute)
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attention_summaries (
    session_id, latest_seq, state, last_durable_event_at, projection_state
) VALUES ('ses_backfill_existing', 1, 'busy', $1, 'complete')
`, storeClock); err != nil {
		t.Fatalf("insert existing attention summary: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Processed != 0 || !result.Done {
		t.Fatalf("complete-summary attention backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_existing"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != 1 || snapshot[0].State != "busy" ||
		snapshot[0].LastDurableEventAt == nil || !snapshot[0].LastDurableEventAt.Equal(storeClock) {
		t.Fatalf("complete attention summary changed = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillMarksHighWaterMismatchIncomplete(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	if _, err := pool.Exec(context.Background(), `
CREATE FUNCTION reject_backfill_latest_seq_regression()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.latest_seq < OLD.latest_seq THEN
        RAISE EXCEPTION 'session attention summary latest_seq must not regress';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER session_attention_summary_latest_seq_guard
BEFORE UPDATE OF latest_seq ON session_attention_summaries
FOR EACH ROW EXECUTE FUNCTION reject_backfill_latest_seq_regression();
`); err != nil {
		t.Fatalf("install summary monotonicity guard: %v", err)
	}
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_high_water", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attention_summaries (session_id, latest_seq, state, projection_state)
VALUES ('ses_backfill_high_water', 5, 'busy', 'incomplete')
`); err != nil {
		t.Fatalf("insert high-water attention summary: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Processed != 1 || result.Incomplete != 1 {
		t.Fatalf("high-water attention backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_high_water"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != 5 || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("high-water attention summary = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillMarksMissingSummaryHighWaterGap(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_missing_high_water", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	if _, err := pool.Exec(context.Background(), `UPDATE session_event_streams SET latest_seq = 2 WHERE session_id = 'ses_backfill_missing_high_water'`); err != nil {
		t.Fatalf("create missing high-water fixture: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Incomplete != 1 {
		t.Fatalf("missing-summary high-water backfill = %+v, %v", result, err)
	}
	assertAttentionBackfillSnapshot(t, postgres.New(pool), []string{"ses_backfill_missing_high_water"}, 1, store.AttentionProjectionIncomplete)
}

func TestAttentionBackfillClearsStaleLedger(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_stale_ledger", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attention_summaries (session_id, latest_seq, state, blocker_kind, blocker_reason, last_client_command_at, summary_version, projection_state)
VALUES ('ses_backfill_stale_ledger', 1, 'busy', 'queued', 'old', clock_timestamp(), 7, 'incomplete')
`); err != nil {
		t.Fatalf("insert stale ledger: %v", err)
	}
	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1); err != nil {
		t.Fatalf("stale-ledger backfill: %v", err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_stale_ledger"})
	if err != nil || len(snapshot) != 1 || snapshot[0].Blocker != nil || snapshot[0].LastClientCommandAt != nil || snapshot[0].SummaryVersion != 0 {
		t.Fatalf("stale ledger remained = %+v, %v", snapshot, err)
	}
}

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

func TestAttentionBackfillIncludesAttachmentOnlyTargetLedger(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_attachment_only_bootstrap'), ('ses_attachment_only_target'), ('ses_attachment_only_blocker')`); err != nil {
		t.Fatalf("insert attachment-only sessions: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attachments (attach_id, bootstrap_session_id, target_session_id, status, delivery_state, delivery_version, queue_reason, expires_at, blocking_session_id, target_credential_lineage_ref)
VALUES ('att_attachment_only', 'ses_attachment_only_bootstrap', 'ses_attachment_only_target', 'queued', 'pending', 0, 'capacity', clock_timestamp() + interval '10 seconds', 'ses_attachment_only_blocker', 'lineage_attachment_only')`); err != nil {
		t.Fatalf("insert attachment-only row: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Processed != 1 || result.Incomplete != 1 {
		t.Fatalf("attachment-only backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_attachment_only_target"})
	if err != nil || len(snapshot) != 1 || snapshot[0].SummaryVersion != 1 || snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerQueued || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("attachment-only summary = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillRestoresAttachmentLedgerVersion(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_attachment_version", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_attachment_version_bootstrap')`); err != nil {
		t.Fatalf("insert attachment version bootstrap: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attachments (attach_id, bootstrap_session_id, target_session_id, status, delivery_state, delivery_version, target_credential_lineage_ref)
VALUES ('att_attachment_version', 'ses_attachment_version_bootstrap', 'ses_attachment_version', 'start_received', 'completed', 2, 'lineage_attachment_version')`); err != nil {
		t.Fatalf("insert attachment version row: %v", err)
	}
	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1); err != nil {
		t.Fatalf("attachment version backfill: %v", err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_attachment_version"})
	if err != nil || len(snapshot) != 1 || snapshot[0].SummaryVersion != 3 || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("attachment version summary = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillRestoresCurrentAttachmentBlocker(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_attachment", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_backfill_bootstrap'), ('ses_backfill_blocker')`); err != nil {
		t.Fatalf("insert attachment sessions: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attachments (
    attach_id, bootstrap_session_id, target_session_id, status, delivery_state,
    delivery_version, queue_reason, expires_at, blocking_session_id, target_credential_lineage_ref
) VALUES ('att_backfill', 'ses_backfill_bootstrap', 'ses_backfill_attachment', 'queued', 'pending', 1,
          'capacity', clock_timestamp() + interval '10 seconds', 'ses_backfill_blocker', 'lineage_backfill')
`); err != nil {
		t.Fatalf("insert attachment blocker: %v", err)
	}
	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1); err != nil {
		t.Fatalf("attachment attention backfill: %v", err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_attachment"})
	if err != nil || len(snapshot) != 1 || snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerQueued ||
		snapshot[0].Blocker.Reason == nil || *snapshot[0].Blocker.Reason != "capacity" || snapshot[0].Blocker.BlockingSessionID == nil ||
		*snapshot[0].Blocker.BlockingSessionID != "ses_backfill_blocker" || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("attachment blocker summary = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillPrioritizesAttachmentOutcomeUnknown(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_attachment_unknown", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_backfill_unknown_bootstrap')`); err != nil {
		t.Fatalf("insert unknown attachment session: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attachments (
    attach_id, bootstrap_session_id, target_session_id, status, delivery_state,
    delivery_version, target_credential_lineage_ref
) VALUES ('att_backfill_unknown', 'ses_backfill_unknown_bootstrap', 'ses_backfill_attachment_unknown', 'start_received', 'outcome_unknown', 1, 'lineage_unknown')
`); err != nil {
		t.Fatalf("insert unknown attachment: %v", err)
	}
	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1); err != nil {
		t.Fatalf("unknown attachment backfill: %v", err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_attachment_unknown"})
	if err != nil || len(snapshot) != 1 || snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerOutcomeUnknown {
		t.Fatalf("unknown attachment blocker = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillRollsBackFailedProjection(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_rollback", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	originalActivity := base.Add(-time.Minute)
	if _, err := pool.Exec(context.Background(), `
INSERT INTO session_attention_summaries (
    session_id, latest_seq, state, last_durable_event_at, projection_state
) VALUES ('ses_backfill_rollback', 1, 'busy', $1, 'incomplete')
`, originalActivity); err != nil {
		t.Fatalf("insert backfill rollback summary: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
CREATE FUNCTION reject_backfill_ready_projection()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'ready' THEN
        RAISE EXCEPTION 'reject ready backfill projection';
    END IF;
    RETURN NEW;
END;
$$
`); err != nil {
		t.Fatalf("create backfill rollback trigger function: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
CREATE TRIGGER reject_backfill_ready_projection
BEFORE UPDATE ON session_attention_summaries
FOR EACH ROW EXECUTE FUNCTION reject_backfill_ready_projection()
`); err != nil {
		t.Fatalf("create backfill rollback trigger: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err == nil || result.Processed != 0 || result.Checkpoint.AfterSessionID != "" {
		t.Fatalf("failed attention backfill = %+v, %v", result, err)
	}
	snapshot, snapshotErr := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_rollback"})
	if snapshotErr != nil || len(snapshot) != 1 || snapshot[0].State != "busy" || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete ||
		snapshot[0].LastDurableEventAt == nil || !snapshot[0].LastDurableEventAt.Equal(originalActivity) {
		t.Fatalf("rolled-back attention summary = %+v, %v", snapshot, snapshotErr)
	}
}

func TestAttentionBackfillMarksNonStatePrefixIncomplete(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_non_state_first",
		historicalAttentionEvent{1, "session.message", `{"role":"agent"}`, base},
		historicalAttentionEvent{2, "session.state", `{"state":"ready"}`, base.Add(time.Second)})
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Incomplete != 1 {
		t.Fatalf("non-state prefix backfill = %+v, %v", result, err)
	}
	assertAttentionBackfillSnapshot(t, postgres.New(pool), []string{"ses_backfill_non_state_first"}, 1, store.AttentionProjectionIncomplete)
}

func TestAttentionBackfillCreatesSummaryForDeletedHistory(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_deleted", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})
	if _, err := pool.Exec(context.Background(), `DELETE FROM session_events WHERE session_id = 'ses_backfill_deleted'`); err != nil {
		t.Fatalf("delete historical events: %v", err)
	}
	result, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
	if err != nil || result.Incomplete != 1 {
		t.Fatalf("deleted-history backfill = %+v, %v", result, err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_deleted"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != 1 || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("deleted-history attention summary = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillSerializesConcurrentAppend(t *testing.T) {
	tracer := newAttentionBackfillTracer()
	_, _, pool := newAttentionBackfillPool(t, tracer)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_concurrent", historicalAttentionEvent{1, "session.state", `{"state":"ready"}`, base})

	backfillErr := make(chan error, 1)
	go func() {
		_, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1)
		backfillErr <- err
	}()
	select {
	case <-tracer.pageStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("attention backfill did not reach the event page")
	}
	appendErr := make(chan error, 1)
	go func() {
		_, err := postgres.New(pool).Append(context.Background(), "ses_backfill_concurrent", []store.PendingEvent{{
			Type: "session.state", Time: base.Add(time.Second), Payload: []byte(`{"state":"working"}`),
		}})
		appendErr <- err
	}()
	select {
	case <-tracer.appendLockStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent append did not wait on the backfill stream lock")
	}
	close(tracer.resume)
	if err := <-backfillErr; err != nil {
		t.Fatalf("concurrent attention backfill: %v", err)
	}
	if err := <-appendErr; err != nil {
		t.Fatalf("append after attention backfill: %v", err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_concurrent"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != 2 || snapshot[0].State != "busy" || snapshot[0].StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("concurrent attention result = %+v, %v", snapshot, err)
	}
}

func TestAttentionBackfillUsesStoreClockForDurableActivity(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	eventTime := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	insertHistoricalAttentionEvents(t, pool, "ses_backfill_store_clock", historicalAttentionEvent{
		1, "session.state", `{"state":"ready"}`, eventTime,
	})
	var storeTime time.Time
	if err := pool.QueryRow(context.Background(), `
SELECT updated_at FROM session_event_streams WHERE session_id = 'ses_backfill_store_clock'
`).Scan(&storeTime); err != nil {
		t.Fatalf("read historical Store clock: %v", err)
	}
	if storeTime.Equal(eventTime) {
		t.Fatalf("fixture did not distinguish Store clock from event source time: %s", storeTime)
	}
	if _, err := postgres.New(pool).BackfillAttentionBatch(context.Background(), postgres.AttentionBackfillCheckpoint{}, 1); err != nil {
		t.Fatalf("Store-clock attention backfill: %v", err)
	}
	snapshot, err := postgres.New(pool).AttentionSnapshot(context.Background(), []string{"ses_backfill_store_clock"})
	if err != nil || len(snapshot) != 1 {
		t.Fatalf("Store-clock attention snapshot = %+v, %v", snapshot, err)
	}
	if snapshot[0].LastDurableEventAt == nil || !snapshot[0].LastDurableEventAt.Equal(storeTime) {
		t.Fatalf("historical durable activity = %+v, want Store clock %s", snapshot[0], storeTime)
	}
}

type historicalAttentionEvent struct {
	seq       int64
	eventType string
	payload   string
	createdAt time.Time
}

func newAttentionBackfillPool(t *testing.T, tracer pgx.QueryTracer) (string, string, *pgxpool.Pool) {
	t.Helper()
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_backfill_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, tracer)
	resetSchema(t, pool)
	return dsn, schemaName, pool
}

func insertHistoricalAttentionEvents(t *testing.T, pool *pgxpool.Pool, sessionID string, events ...historicalAttentionEvent) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ($1) ON CONFLICT DO NOTHING`, sessionID); err != nil {
		t.Fatalf("insert historical session: %v", err)
	}
	for _, event := range events {
		if _, err := pool.Exec(context.Background(), `
INSERT INTO session_events (session_id, seq, type, payload, created_at)
VALUES ($1, $2, $3, $4::jsonb, $5)
`, sessionID, event.seq, event.eventType, event.payload, event.createdAt); err != nil {
			t.Fatalf("insert historical event %s/%d: %v", sessionID, event.seq, err)
		}
	}
}

func assertAttentionBackfillSnapshot(t *testing.T, attention *postgres.Store, sessionIDs []string, want int, projection string) {
	t.Helper()
	snapshot, err := attention.AttentionSnapshot(context.Background(), sessionIDs)
	if err != nil || len(snapshot) != want {
		t.Fatalf("attention backfill snapshot = %+v, %v", snapshot, err)
	}
	for _, summary := range snapshot {
		if summary.StateOfProjection != projection {
			t.Fatalf("attention backfill projection = %+v, want %s", summary, projection)
		}
	}
}

type attentionBackfillTracer struct {
	pageStarted       chan struct{}
	appendLockStarted chan struct{}
	resume            chan struct{}
	lockQueries       atomic.Int64
	pageOnce          sync.Once
	appendOnce        sync.Once
}

func newAttentionBackfillTracer() *attentionBackfillTracer {
	return &attentionBackfillTracer{
		pageStarted: make(chan struct{}), appendLockStarted: make(chan struct{}), resume: make(chan struct{}),
	}
}

func (tracer *attentionBackfillTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "pg_advisory_xact_lock") && tracer.lockQueries.Add(1) == 2 {
		tracer.appendOnce.Do(func() { close(tracer.appendLockStarted) })
	}
	if strings.Contains(data.SQL, "FROM session_events") && strings.Contains(data.SQL, "seq >") && strings.Contains(data.SQL, "LIMIT") {
		tracer.pageOnce.Do(func() { close(tracer.pageStarted) })
		<-tracer.resume
	}
	return ctx
}

func (*attentionBackfillTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
