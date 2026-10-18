package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

// The production shape of a latched summary, not a convenient one.
//
// Every Own Machine stream starts with an adapter capability proposal, because
// the adapter publishes its capabilities before it publishes any state. The
// backfill marks a stream incomplete when its first durable event carries no
// state, so such a stream is marked incomplete even by a rebuild that reads every
// event correctly.
//
// The earlier version of this test seeded `session.state` first, which made the
// rebuild come out complete and made a repair look sufficient. No production
// Own Machine stream is shaped that way, and an operator command was shipped on
// the strength of that fixture. This one pins the real ordering and therefore the
// real outcome.
func TestAttentionBackfillRebuildsContentButCannotClearTheLatchOnAProposalFirstStream(t *testing.T) {
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

	// The rebuild does its job: every event is read, the stream head and the
	// state are correct, and client activity is restored.
	if rebuilt.LatestSeq != 4 || rebuilt.State != "ready" || rebuilt.LastClientCommandAt == nil {
		t.Fatalf("rebuilt content = %+v, want the stream head with its client activity restored", rebuilt)
	}
	// And it still does not clear the latch, because the first event of the
	// stream carries no state. This is the assertion that would have caught the
	// unrealistic fixture: with a state-first stream it would read `complete` and
	// fail here.
	if rebuilt.StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("rebuilt projection = %q, want incomplete: a leading proposal is not evidence of a lost state, but the backfill still counts it as one", rebuilt.StateOfProjection)
	}
}

// The latch is one-way: no number of later contiguous, state-bearing events can
// clear it. This is the rule that freezes the Control Plane's activity
// projection, and therefore the Run delivery decision that reads
// `last_client_command_at` from it.
func TestAttentionUpsertCannotClearTheLatchWithLaterContiguousEvents(t *testing.T) {
	_, _, pool := newAttentionBackfillPool(t, nil)
	attention := postgres.New(pool)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	// A proposal-first stream, so the latch arises the way it does in production
	// rather than by hand-editing the flag.
	insertHistoricalAttentionEvents(t, pool, "ses_latch_order",
		historicalAttentionEvent{1, "session.run.capabilities", `{"interrupt_supported":true}`, base},
		historicalAttentionEvent{2, "session.message", `{"role":"user"}`, base.Add(time.Second)})
	if _, err := attention.BackfillAttentionBatch(ctx, postgres.AttentionBackfillCheckpoint{}, 10); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	assertAttentionBackfillSnapshot(t, attention, []string{"ses_latch_order"}, 1, store.AttentionProjectionIncomplete)

	// Now append more events, each contiguous and carrying a state, through the
	// normal append path.
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
