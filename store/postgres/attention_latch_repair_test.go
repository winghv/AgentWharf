package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

// The production shape of a latched summary, and what a rebuild can now do
// with it.
//
// Every Own Machine stream starts with an adapter capability proposal, because
// the adapter publishes its capabilities before it publishes any state. The
// backfill used to treat that first state-less event as evidence that a state had
// been lost, which meant a rebuild of a perfectly readable stream came out
// incomplete -- and because the Control Plane refuses to rebase a stale projection
// from an incomplete summary, every Own Machine projection stayed frozen.
//
// The first version of this test seeded `session.state` first, so the rebuild came
// out complete and a repair looked sufficient. No production Own Machine stream is
// shaped that way.
func TestAttentionRebuildClearsTheLatchOnAProposalFirstStream(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	attention := postgres.New(pool)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	events := []historicalAttentionEvent{
		{1, "session.run.capabilities", `{"interrupt_supported":true}`, base},
		{2, "session.message", `{"role":"user"}`, base.Add(time.Second)},
		{3, "session.state", `{"state":"ready"}`, base.Add(2 * time.Second)},
		{4, "session.message", `{"role":"agent"}`, base.Add(3 * time.Second)},
	}
	insertHistoricalAttentionEvents(t, pool, "ses_proposal_first", events...)
	if _, err := pool.Exec(ctx, `
INSERT INTO session_pending_commands (session_id, cmd_id, type, event_seq, status, expires_at)
VALUES ('ses_proposal_first', 'cmd_proposal_first', 'session.send', 2, 'outcome_unknown', clock_timestamp() + interval '10 seconds')`); err != nil {
		t.Fatalf("insert command: %v", err)
	}

	// The latch as production has it: right content, wrong flag.
	if _, err := pool.Exec(ctx, `
INSERT INTO session_attention_summaries (session_id, latest_seq, state, latest_change_seq, last_durable_event_at, projection_state)
VALUES ('ses_proposal_first', 4, 'ready', 3, $1, 'incomplete')`, base.Add(3*time.Second)); err != nil {
		t.Fatalf("latch the summary: %v", err)
	}
	assertAttentionBackfillSnapshot(t, attention, []string{"ses_proposal_first"}, 1, store.AttentionProjectionIncomplete)

	if _, err := attention.BackfillAttentionBatch(ctx, postgres.AttentionBackfillCheckpoint{}, 10); err != nil {
		t.Fatalf("backfill latched summary: %v", err)
	}

	snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_proposal_first"})
	if err != nil || len(snapshot) != 1 {
		t.Fatalf("snapshot = %+v, %v", snapshot, err)
	}
	rebuilt := snapshot[0]
	if rebuilt.LatestSeq != 4 || rebuilt.State != "ready" || rebuilt.LastClientCommandAt == nil {
		t.Fatalf("rebuilt summary = %+v, want the stream head with its client activity restored", rebuilt)
	}
	// The rebuild read every event, so it can say what the state was: this is the
	// assertion the previous fixture made impossible to reach.
	if rebuilt.StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("rebuilt projection = %q, want complete: the rebuild read every event and a leading proposal lost nothing", rebuilt.StateOfProjection)
	}
}

// The latch is still one-way for summaries that are not rebuilt: no number of
// later contiguous, state-bearing events can clear it. That is what freezes the
// Control Plane's activity projection, and therefore the Run delivery decision
// that reads `last_client_command_at` from it. Fixing this means repairing, not
// waiting.
func TestAttentionUpsertCannotClearTheLatchWithLaterContiguousEvents(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	attention := postgres.New(pool)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	insertHistoricalAttentionEvents(t, pool, "ses_latch_order",
		historicalAttentionEvent{1, "session.run.capabilities", `{"interrupt_supported":true}`, base},
		historicalAttentionEvent{2, "session.message", `{"role":"user"}`, base.Add(time.Second)})
	// Latch it the way the append path does, so this is not a hand-edited flag.
	if _, err := pool.Exec(ctx, `
INSERT INTO session_attention_summaries (session_id, latest_seq, state, last_durable_event_at, projection_state)
VALUES ('ses_latch_order', 2, 'starting', $1, 'incomplete')`, base); err != nil {
		t.Fatalf("seed latched summary: %v", err)
	}

	for index, payload := range []string{`{"state":"ready"}`, `{"state":"busy"}`, `{"state":"ready"}`} {
		if _, err := attention.Append(ctx, "ses_latch_order", []store.PendingEvent{
			{Type: "session.state", Time: base.Add(time.Duration(10+index) * time.Second), Payload: []byte(payload)},
		}); err != nil {
			t.Fatalf("append event %d: %v", index, err)
		}
	}

	snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_latch_order"})
	if err != nil || len(snapshot) != 1 {
		t.Fatalf("snapshot = %+v, %v", snapshot, err)
	}
	if snapshot[0].LatestSeq != 5 || snapshot[0].State != "ready" {
		t.Fatalf("summary stopped tracking the stream: %+v", snapshot[0])
	}
	if snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("projection = %q, want it still incomplete: three contiguous state-bearing events cannot clear the latch", snapshot[0].StateOfProjection)
	}
}
