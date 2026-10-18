package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

// The append path projects every event it writes, so a gap means a summary missed
// events -- and the upsert then latches it incomplete for good. This test pins
// the repair that already exists: rebuilding from the durable stream, which
// resets the summary to complete and folds in every event.
func TestAttentionBackfillRepairsASummaryTheAppendPathLatchedIncomplete(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	attention := postgres.New(pool)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	events := []historicalAttentionEvent{
		{1, "session.state", `{"state":"starting"}`, base},
		{2, "session.message", `{"role":"user"}`, base.Add(time.Second)},
		{3, "session.state", `{"state":"ready"}`, base.Add(2 * time.Second)},
		{4, "session.message", `{"role":"agent"}`, base.Add(3 * time.Second)},
	}
	insertHistoricalAttentionEvents(t, pool, "ses_latched", events...)
	if _, err := pool.Exec(ctx, `
INSERT INTO session_pending_commands (session_id, cmd_id, type, event_seq, status, expires_at)
VALUES ('ses_latched', 'cmd_latched', 'session.send', 2, 'outcome_unknown', clock_timestamp() + interval '10 seconds')`); err != nil {
		t.Fatalf("insert command: %v", err)
	}

	// Reproduce the latch the way production has it: the summary is at the stream
	// head with the right content, but its flag says incomplete, and the upsert's
	// arm ordering means no later event can clear it.
	if _, err := pool.Exec(ctx, `
INSERT INTO session_attention_summaries (session_id, latest_seq, state, latest_change_seq, last_durable_event_at, projection_state)
VALUES ('ses_latched', 4, 'ready', 3, $1, 'incomplete')`, base.Add(3*time.Second)); err != nil {
		t.Fatalf("latch the summary: %v", err)
	}
	assertAttentionBackfillSnapshot(t, attention, []string{"ses_latched"}, 1, store.AttentionProjectionIncomplete)

	if _, err := attention.BackfillAttentionBatch(ctx, postgres.AttentionBackfillCheckpoint{}, 10); err != nil {
		t.Fatalf("backfill latched summary: %v", err)
	}

	snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_latched"})
	if err != nil || len(snapshot) != 1 {
		t.Fatalf("snapshot = %+v, %v", snapshot, err)
	}
	rebuilt := snapshot[0]
	if rebuilt.StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("rebuilt projection = %q, want complete: the latch outlived a rebuild that read every event", rebuilt.StateOfProjection)
	}
	if rebuilt.LatestSeq != 4 || rebuilt.State != "ready" || rebuilt.LastClientCommandAt == nil {
		t.Fatalf("rebuilt summary = %+v, want the stream head with its client activity restored", rebuilt)
	}
}
