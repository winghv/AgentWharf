package postgres_test

import (
	"context"
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
