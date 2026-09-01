package postgres_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
	"github.com/winghv/agentwharf/store/storetest"
)

var schemaSeq atomic.Uint64

func TestAttentionSummaryStoreContract(t *testing.T) {
	var _ store.AttentionSummaryStore = (*postgres.Store)(nil)
}

func TestSessionAdmissionTruthIsBoundedAndFailsClosed(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_admission_truth_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
INSERT INTO agent_sessions (id, provider, status, started_at)
VALUES ('ses_live', 'claude-code', 'ready', clock_timestamp()),
	       ('ses_preallocated_target', 'claude-code', 'starting', clock_timestamp()),
	       ('ses_terminal', 'claude-code', 'ended', clock_timestamp())`); err != nil {
		t.Fatalf("seed admission sessions: %v", err)
	}
	events := postgres.New(pool)
	for _, test := range []struct {
		sessionID string
		want      store.SessionAdmissionTruth
	}{
		{sessionID: "ses_live", want: store.SessionAdmissionTruth{SessionID: "ses_live", Provider: "claude-code", Exists: true, Complete: true, Live: true}},
		// A Control Plane preallocation establishes the durable Task/Run relation,
		// but is not Hub Store truth until attachment commits it.
		{sessionID: "ses_preallocated_target", want: store.SessionAdmissionTruth{SessionID: "ses_preallocated_target"}},
		{sessionID: "ses_terminal", want: store.SessionAdmissionTruth{SessionID: "ses_terminal", Provider: "claude-code", Exists: true, Complete: true, Terminal: true}},
		{sessionID: "ses_missing", want: store.SessionAdmissionTruth{SessionID: "ses_missing"}},
	} {
		got, err := events.SessionAdmissionTruth(ctx, test.sessionID)
		if err != nil || got != test.want {
			t.Fatalf("SessionAdmissionTruth(%q) = %+v, %v; want %+v, nil", test.sessionID, got, err, test.want)
		}
	}
	starting, err := events.AdapterSessionAdmissionTruth(ctx, "ses_preallocated_target")
	wantStarting := store.SessionAdmissionTruth{SessionID: "ses_preallocated_target", Provider: "claude-code", Exists: true, Complete: true, Live: true}
	if err != nil || starting != wantStarting {
		t.Fatalf("AdapterSessionAdmissionTruth(starting) = %+v, %v; want %+v, nil", starting, err, wantStarting)
	}
	if _, err := events.SessionAdmissionTruth(ctx, ""); err == nil {
		t.Fatal("empty session ID was accepted")
	}
}

func TestAppendProjectsCanonicalAgentSessionStates(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_session_state_projection_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	events := postgres.New(pool)
	ctx := context.Background()

	for _, test := range []struct {
		name       string
		eventType  string
		payload    string
		wantStatus string
		terminal   bool
	}{
		{name: "ready", eventType: "session.state", payload: `{"state":"ready"}`, wantStatus: "ready"},
		{name: "working", eventType: "session.state", payload: `{"state":"working"}`, wantStatus: "busy"},
		{name: "waiting permission", eventType: "session.state", payload: `{"state":"waiting_permission"}`, wantStatus: "waiting_permission"},
		{name: "recovering", eventType: "session.state", payload: `{"state":"recovering"}`, wantStatus: "recovering"},
		{name: "ended", eventType: "session.state", payload: `{"state":"ended"}`, wantStatus: "ended", terminal: true},
		{name: "error state", eventType: "session.state", payload: `{"state":"error"}`, wantStatus: "error", terminal: true},
		{name: "error event", eventType: "session.error", payload: `{"reason":"provider_failed"}`, wantStatus: "error", terminal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sessionID := "ses_projection_" + strings.ReplaceAll(test.name, " ", "_")
			if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id, provider, status, started_at) VALUES ($1, 'claude-code', 'starting', clock_timestamp())`, sessionID); err != nil {
				t.Fatal(err)
			}
			if _, err := events.Append(ctx, sessionID, []store.PendingEvent{{Type: test.eventType, Time: time.Now(), Payload: []byte(test.payload)}}); err != nil {
				t.Fatal(err)
			}
			var status string
			var endedAt *time.Time
			if err := pool.QueryRow(ctx, `SELECT status, ended_at FROM agent_sessions WHERE id=$1`, sessionID).Scan(&status, &endedAt); err != nil {
				t.Fatal(err)
			}
			if status != test.wantStatus || (endedAt != nil) != test.terminal {
				t.Fatalf("projected session = status %q ended_at %v, want %s terminal=%t", status, endedAt, test.wantStatus, test.terminal)
			}
		})
	}

	const invalidSessionID = "ses_projection_invalid"
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id, provider, status, started_at) VALUES ($1, 'claude-code', 'starting', clock_timestamp())`, invalidSessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(ctx, invalidSessionID, []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"unknown"}`)}}); err != nil {
		t.Fatal(err)
	}
	var invalidStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM agent_sessions WHERE id=$1`, invalidSessionID).Scan(&invalidStatus); err != nil {
		t.Fatal(err)
	}
	if invalidStatus != "starting" {
		t.Fatalf("invalid state projected as %q, want starting", invalidStatus)
	}
}

func TestAppendWithCallerTransactionRollsBackWithLifecycleRows(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_event_tx_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id, provider, status, started_at) VALUES ('ses_event_tx', 'claude-code', 'starting', clock_timestamp())`); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.NewEventStoreTx(tx).Append(ctx, "ses_event_tx", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Append() with caller transaction: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT status FROM agent_sessions WHERE id='ses_event_tx'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM session_events WHERE session_id='ses_event_tx'`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if status != "starting" || eventCount != 0 {
		t.Fatalf("caller transaction rollback left status=%q events=%d, want starting/0", status, eventCount)
	}
}

func TestAttentionSummaryPageIsReadOnlyAndKeysetBounded(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_page_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	attention := postgres.New(pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id) SELECT session_id FROM unnest($1::text[]) AS session_id`, []string{"ses_page_a", "ses_page_b", "ses_page_c"}); err != nil {
		t.Fatalf("seed attention page sessions: %v", err)
	}
	for _, sessionID := range []string{"ses_page_a", "ses_page_b", "ses_page_c"} {
		if _, err := attention.Append(ctx, sessionID, []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
			t.Fatalf("seed %s: %v", sessionID, err)
		}
		if _, err := attention.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			t.Fatalf("initialize %s: %v", sessionID, err)
		}
	}
	page, err := attention.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 2})
	if err != nil || page.SnapshotAt.IsZero() || len(page.Summaries) != 2 || page.Summaries[0].SessionID != "ses_page_a" || page.NextAfterSessionID == nil || *page.NextAfterSessionID != "ses_page_b" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	next, err := attention.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{AfterSessionID: *page.NextAfterSessionID, Limit: 2})
	if err != nil || len(next.Summaries) != 1 || next.Summaries[0].SessionID != "ses_page_c" || next.NextAfterSessionID != nil {
		t.Fatalf("second page = %+v, %v", next, err)
	}
}

func TestAttentionSummaryPageRequiresCurrentAdapterAuthority(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_authority_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	attention := postgres.New(pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id) SELECT session_id FROM unnest($1::text[]) AS session_id`, []string{"ses_active", "ses_expired", "ses_revoked", "ses_terminal", "ses_missing"}); err != nil {
		t.Fatalf("seed attention authority sessions: %v", err)
	}
	for _, sessionID := range []string{"ses_active", "ses_expired", "ses_revoked", "ses_terminal", "ses_missing"} {
		if _, err := attention.Append(ctx, sessionID, []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
			t.Fatalf("seed %s: %v", sessionID, err)
		}
		if sessionID != "ses_missing" {
			if _, err := attention.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
				t.Fatalf("initialize %s: %v", sessionID, err)
			}
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE session_adapter_connections SET created_at = clock_timestamp() - interval '2 minutes', active_credential_expires_at = clock_timestamp() - interval '1 second' WHERE session_id = 'ses_expired'; UPDATE session_adapter_connections SET revoked_at = clock_timestamp() WHERE session_id = 'ses_revoked'; UPDATE session_adapter_connections SET terminal_at = clock_timestamp() WHERE session_id = 'ses_terminal'`); err != nil {
		t.Fatal(err)
	}
	page, err := attention.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 10})
	if err != nil || len(page.Summaries) != 1 || page.Summaries[0].SessionID != "ses_active" {
		t.Fatalf("authority-fenced page = %+v, %v", page, err)
	}
}

func TestAttentionSnapshotProjectsDurableEvents(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	events := postgres.New(pool)
	before := time.Now()
	first := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	if _, err := events.Append(context.Background(), "ses_attention_1", []store.PendingEvent{
		{Type: "session.state", Time: first, Payload: []byte(`{"state":"starting"}`)},
		{Type: "session.state", Time: first.Add(time.Millisecond), Payload: []byte(`{"state":"working"}`)},
		{Type: "session.message", Time: first.Add(2 * time.Millisecond), Payload: []byte(`{"role":"agent"}`)},
	}); err != nil {
		t.Fatalf("append attention events: %v", err)
	}

	snapshot, err := events.AttentionSnapshot(context.Background(), []string{"ses_attention_1", "ses_missing"})
	if err != nil {
		t.Fatalf("attention snapshot: %v", err)
	}
	if len(snapshot) != 1 {
		t.Fatalf("attention snapshot length = %d, want 1", len(snapshot))
	}
	summary := snapshot[0]
	if summary.SessionID != "ses_attention_1" || summary.LatestSeq != 3 || summary.State != "busy" || summary.LatestChangeSeq == nil || *summary.LatestChangeSeq != 2 || summary.LastDurableEventAt == nil || summary.LastDurableEventAt.Before(before) || summary.StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("attention summary = %+v, want complete busy durable projection", summary)
	}
}

func TestAttentionSnapshotProjectsClientCommandLedger(t *testing.T) {
	harness := newPostgresCommandHarness(t, "agentwharf_attention_command", nil)
	request := store.PendingCommandRequest{CommandID: "cmd_attention", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := harness.CommitPendingCommand(context.Background(), "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
		store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)}, request); err != nil {
		t.Fatalf("commit pending command: %v", err)
	}
	snapshot, err := harness.AttentionSnapshot(context.Background(), []string{"ses_command_1"})
	if err != nil || len(snapshot) != 1 {
		t.Fatalf("attention snapshot = %+v, %v", snapshot, err)
	}
	summary := snapshot[0]
	if summary.LatestSeq != 1 || summary.SummaryVersion != 1 || summary.LastClientCommandAt == nil || summary.Blocker != nil {
		t.Fatalf("command attention summary = %+v, want independent ledger version and Store-clock activity", summary)
	}
	originalActivity := *summary.LastClientCommandAt
	if _, err := harness.ClaimPendingCommand(context.Background(), "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, request.CommandID); err != nil {
		t.Fatalf("claim pending command: %v", err)
	}
	if _, err := harness.ResolvePendingCommand(context.Background(), "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, request.CommandID, store.PendingCommandCompleted); err != nil {
		t.Fatalf("resolve pending command: %v", err)
	}
	snapshot, err = harness.AttentionSnapshot(context.Background(), []string{"ses_command_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].SummaryVersion != summary.SummaryVersion || snapshot[0].LastClientCommandAt == nil || !snapshot[0].LastClientCommandAt.Equal(originalActivity) {
		t.Fatalf("receipt changed original command activity = %+v, %v", snapshot, err)
	}
}

func TestAttentionSnapshotPreservesClientActivityForNonClientCallbacks(t *testing.T) {
	harness := newPostgresCommandHarness(t, "agentwharf_attention_callbacks", nil)
	ctx := context.Background()
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	firstRequest := store.PendingCommandRequest{CommandID: "cmd_attention_callback_first", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := harness.CommitPendingCommand(ctx, "ses_command_1", authority,
		store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)}, firstRequest); err != nil {
		t.Fatalf("commit first pending command: %v", err)
	}
	snapshot, err := harness.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LastClientCommandAt == nil {
		t.Fatalf("first command attention snapshot = %+v, %v", snapshot, err)
	}
	originalActivity := *snapshot[0].LastClientCommandAt
	originalVersion := snapshot[0].SummaryVersion

	if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, "ses_attention_callback_bootstrap"); err != nil {
		t.Fatalf("seed attachment bootstrap session: %v", err)
	}
	attachmentRequest := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_attention_callback", BootstrapSessionID: "ses_attention_callback_bootstrap",
		TargetSessionID: "ses_command_1", TargetCredentialLineageRef: "lineage_attention_callback",
	}, ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := harness.CreateAttachment(ctx, attachmentRequest); err != nil {
		t.Fatalf("create attachment callback: %v", err)
	}
	operation := "start"
	if _, err := harness.UpdateAttachment(ctx, attachmentRequest.Identity.AttachID, 0, store.AttachmentUpdate{
		Status: store.AttachmentReauthorizationRequired, DeliveryState: store.AttachmentDeliveryOutcomeUnknown,
		Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerOutcomeUnknown, Operation: &operation},
	}); err != nil {
		t.Fatalf("record attachment outcome unknown: %v", err)
	}
	snapshot, err = harness.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].SummaryVersion != originalVersion+2 || snapshot[0].LastClientCommandAt == nil || !snapshot[0].LastClientCommandAt.Equal(originalActivity) {
		t.Fatalf("attachment callbacks changed client activity = %+v, %v", snapshot, err)
	}

	secondRequest := store.PendingCommandRequest{CommandID: "cmd_attention_callback_unknown", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := harness.CommitPendingCommand(ctx, "ses_command_1", authority,
		store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)}, secondRequest); err != nil {
		t.Fatalf("commit outcome-unknown pending command: %v", err)
	}
	snapshot, err = harness.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LastClientCommandAt == nil {
		t.Fatalf("outcome-unknown command attention snapshot = %+v, %v", snapshot, err)
	}
	originalActivity = *snapshot[0].LastClientCommandAt
	originalVersion = snapshot[0].SummaryVersion
	if _, err := harness.ClaimPendingCommand(ctx, "ses_command_1", authority, secondRequest.CommandID); err != nil {
		t.Fatalf("claim outcome-unknown pending command: %v", err)
	}
	if _, err := harness.ResolvePendingCommand(ctx, "ses_command_1", authority, secondRequest.CommandID, store.PendingCommandOutcomeUnknown); err != nil {
		t.Fatalf("resolve outcome-unknown pending command: %v", err)
	}
	snapshot, err = harness.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].SummaryVersion != originalVersion+1 || snapshot[0].LastClientCommandAt == nil || !snapshot[0].LastClientCommandAt.Equal(originalActivity) {
		t.Fatalf("outcome-unknown callback changed client activity = %+v, %v", snapshot, err)
	}
}

func TestAttentionProjectionRollsBackAndSurvivesReopen(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_restart_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	resetSchema(t, pool)
	attention := postgres.New(pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_attention_projection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'attention projection failpoint'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER reject_attention_projection BEFORE INSERT ON session_attention_summaries FOR EACH ROW EXECUTE FUNCTION reject_attention_projection()`); err != nil {
		t.Fatal(err)
	}
	if _, err := attention.Append(ctx, "ses_attention_rollback", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err == nil {
		t.Fatal("append unexpectedly committed after attention projection failure")
	}
	if latest, err := attention.LatestSeq(ctx, "ses_attention_rollback"); err != nil || latest != 0 {
		t.Fatalf("rolled-back latest sequence = %d, %v", latest, err)
	}
	if snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_attention_rollback"}); err != nil || len(snapshot) != 0 {
		t.Fatalf("rolled-back attention snapshot = %+v, %v", snapshot, err)
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER reject_attention_projection ON session_attention_summaries`); err != nil {
		t.Fatal(err)
	}
	if _, err := attention.Append(ctx, "ses_attention_rollback", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatalf("append after rollback: %v", err)
	}
	pool.Close()
	pool = openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	attention = postgres.New(pool)
	snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_attention_rollback"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != 1 || snapshot[0].State != "ready" {
		t.Fatalf("reopened attention snapshot = %+v, %v", snapshot, err)
	}
}

func TestAttentionSnapshotConcurrentAppend(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_concurrent_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	appendLockStarted := newQueryStartSignal("pg_advisory_xact_lock")
	pool := openPool(t, dsn, schemaName, appendLockStarted)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	attention := postgres.New(pool)
	ctx := context.Background()
	const sessionID = "ses_attention_concurrent"
	done := make(chan struct{})
	scanErr := make(chan error, 1)
	scanStarted := make(chan struct{})
	scanDuringAppend := make(chan struct{})
	scannerDone := make(chan struct{})
	var scannerStarted sync.Once
	var scannerOverlap sync.Once
	var appendWaiting atomic.Bool
	go func() {
		defer close(scannerDone)
		for {
			if _, err := attention.AttentionSnapshot(ctx, []string{sessionID}); err != nil {
				scanErr <- err
				return
			}
			scannerStarted.Do(func() { close(scanStarted) })
			if appendWaiting.Load() {
				scannerOverlap.Do(func() { close(scanDuringAppend) })
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("attention scanner did not complete its startup snapshot")
	}
	blocker := openPool(t, dsn, schemaName, nil)
	t.Cleanup(blocker.Close)
	blockerTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatalf("begin append stream blocker: %v", err)
	}
	defer func() { _ = blockerTx.Rollback(ctx) }()
	if _, err := blockerTx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, commandAdvisoryLockKey(sessionID)); err != nil {
		t.Fatalf("lock append event stream: %v", err)
	}
	appendResult := make(chan error, 1)
	go func() {
		_, appendErr := attention.Append(ctx, sessionID, []store.PendingEvent{{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"agent"}`)}})
		appendResult <- appendErr
	}()
	select {
	case <-appendLockStarted.started:
	case <-time.After(2 * time.Second):
		t.Fatal("append did not reach the locked event stream")
	}
	appendWaiting.Store(true)
	select {
	case <-scanDuringAppend:
	case <-time.After(2 * time.Second):
		t.Fatal("attention scanner did not complete a snapshot while append was blocked")
	}
	if err := blockerTx.Commit(ctx); err != nil {
		t.Fatalf("release append event stream: %v", err)
	}
	if err := <-appendResult; err != nil {
		t.Fatalf("concurrent append: %v", err)
	}
	close(done)
	<-scannerDone
	select {
	case err := <-scanErr:
		t.Fatalf("concurrent attention snapshot: %v", err)
	default:
	}
	snapshot, err := attention.AttentionSnapshot(ctx, []string{sessionID})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != 1 || snapshot[0].LastDurableEventAt == nil || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("concurrent attention snapshot = %+v, %v", snapshot, err)
	}
}

func TestEventStoreContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T) store.EventStore {
		t.Helper()

		dsn := testDSN(t)
		schemaName := fmt.Sprintf("agentwharf_store_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
		setupSchema(t, dsn, schemaName)
		t.Cleanup(func() {
			dropSchema(t, dsn, schemaName)
		})

		pool := openPool(t, dsn, schemaName, nil)
		t.Cleanup(func() {
			pool.Close()
		})
		resetSchema(t, pool)
		return postgres.New(pool)
	})
}

func TestHistoryStoreContract(t *testing.T) {
	storetest.HistoryContract(t, storetest.HistoryHarness{
		Open: func(t *testing.T) store.HistoryStore {
			t.Helper()

			dsn := testDSN(t)
			schemaName := fmt.Sprintf("agentwharf_history_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
			setupSchema(t, dsn, schemaName)
			harness := &postgresHistoryHarness{dsn: dsn, schemaName: schemaName}
			harness.reopen(t)
			resetSchema(t, harness.pool)
			t.Cleanup(func() {
				harness.pool.Close()
				dropSchema(t, dsn, schemaName)
			})
			return harness
		},
		Reopen: func(t *testing.T, current store.HistoryStore) store.HistoryStore {
			t.Helper()

			harness := current.(*postgresHistoryHarness)
			harness.pool.Close()
			harness.reopen(t)
			return harness
		},
		PruneBefore: func(t *testing.T, current store.HistoryStore, sessionID string, beforeSeq int64) {
			t.Helper()

			harness := current.(*postgresHistoryHarness)
			if _, err := harness.pool.Exec(context.Background(),
				"DELETE FROM session_events WHERE session_id = $1 AND seq < $2", sessionID, beforeSeq); err != nil {
				t.Fatalf("prune retained history: %v", err)
			}
		},
	})
}

func TestPendingCommandStoreContract(t *testing.T) {
	storetest.PendingCommandContract(t, storetest.PendingCommandHarness{
		Open: func(t *testing.T) store.CommandLedgerStore {
			t.Helper()
			return newPostgresCommandHarness(t, "agentwharf_command", nil)
		},
		Reopen: func(t *testing.T, current store.CommandLedgerStore) store.CommandLedgerStore {
			t.Helper()
			harness := current.(*postgresCommandHarness)
			harness.pool.Close()
			harness.reopen(t)
			return harness
		},
		Authority: func(t *testing.T, _ store.CommandLedgerStore) store.CommandAuthority {
			t.Helper()
			return store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
		},
		Invalidate: func(t *testing.T, current store.CommandLedgerStore, kind storetest.CommandAuthorityFailure) {
			t.Helper()
			harness := current.(*postgresCommandHarness)
			var statement string
			switch kind {
			case storetest.CommandAuthoritySuperseded:
				statement = "UPDATE session_adapter_connections SET connection_epoch = 2"
			case storetest.CommandAuthorityRevoked:
				statement = "UPDATE session_adapter_connections SET revoked_at = statement_timestamp()"
			case storetest.CommandAuthorityExpired:
				statement = "UPDATE session_adapter_connections SET created_at = statement_timestamp() - interval '2 minutes', active_credential_expires_at = statement_timestamp() - interval '1 minute'"
			case storetest.CommandAuthorityTerminal:
				statement = "UPDATE session_adapter_connections SET terminal_at = statement_timestamp()"
			default:
				t.Fatalf("unknown command authority failure %q", kind)
			}
			if _, err := harness.pool.Exec(context.Background(), statement); err != nil {
				t.Fatalf("invalidate command authority %s: %v", kind, err)
			}
		},
	})
}

func TestSettingsCommandStoreContract(t *testing.T) {
	storetest.SettingsCommandContract(t, storetest.SettingsCommandHarness{
		Open: func(t *testing.T) store.SettingsCommandStore {
			t.Helper()
			return newPostgresCommandHarness(t, "agentwharf_settings", nil)
		},
		Reopen: func(t *testing.T, current store.SettingsCommandStore) store.SettingsCommandStore {
			t.Helper()
			harness := current.(*postgresCommandHarness)
			harness.pool.Close()
			harness.reopen(t)
			return harness
		},
		ExpireOperationDeadline: func(t *testing.T, current store.SettingsCommandStore, sessionID, commandID string) {
			t.Helper()
			harness := current.(*postgresCommandHarness)
			if _, err := harness.pool.Exec(context.Background(), `UPDATE session_settings_commands SET created_at=created_at - interval '2 milliseconds', delivery_deadline=delivery_deadline - interval '2 milliseconds', operation_deadline=created_at - interval '1 millisecond' WHERE session_id=$1 AND cmd_id=$2`, sessionID, commandID); err != nil {
				t.Fatalf("expire settings operation deadline: %v", err)
			}
		},
		ExpireFileReferenceDeliveryDeadline: func(t *testing.T, current store.SettingsCommandStore, sessionID, commandID string) {
			t.Helper()
			harness := current.(*postgresCommandHarness)
			if _, err := harness.pool.Exec(context.Background(), `UPDATE session_file_reference_commands SET created_at=created_at - interval '2 milliseconds', delivery_deadline=created_at - interval '1 millisecond' WHERE session_id=$1 AND cmd_id=$2`, sessionID, commandID); err != nil {
				t.Fatalf("expire file-reference delivery deadline: %v", err)
			}
		},
		RevokeWriter: func(t *testing.T, current store.SettingsCommandStore, sessionID string) {
			t.Helper()
			harness := current.(*postgresCommandHarness)
			if _, err := harness.pool.Exec(context.Background(), `UPDATE session_adapter_connections SET revoked_at=statement_timestamp() WHERE session_id=$1`, sessionID); err != nil {
				t.Fatalf("revoke settings writer: %v", err)
			}
		},
	})
}

func TestPendingCommandListRejectsCorruptEventPayload(t *testing.T) {
	harness := newPostgresCommandHarness(t, "agentwharf_command_corrupt", nil)
	ctx := context.Background()
	if _, err := harness.pool.Exec(ctx, `
INSERT INTO agent_sessions (id) VALUES ('ses_command_corrupt');
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at
) VALUES ('ses_command_corrupt', 1, 1, 1, 1, statement_timestamp() + interval '1 hour');
INSERT INTO session_events (session_id, seq, type, payload)
VALUES ('ses_command_corrupt', 1, 'session.message', '{"role":"agent","content":"must reject"}');
INSERT INTO session_pending_commands (session_id, cmd_id, type, event_seq, status, expires_at)
VALUES ('ses_command_corrupt', 'cmd_corrupt', 'session.send', 1, 'pending', statement_timestamp() + interval '10 seconds');
`); err != nil {
		t.Fatalf("seed corrupt pending command: %v", err)
	}
	if _, err := harness.ListPendingCommands(ctx, "ses_command_corrupt", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}); err == nil {
		t.Fatal("ListPendingCommands() accepted corrupt non-user event payload")
	}
}

func TestAttachmentStoreContract(t *testing.T) {
	storetest.AttachmentContract(t, storetest.AttachmentHarness{
		Open: func(t *testing.T) store.AttachmentStore {
			t.Helper()
			return newPostgresAttachmentHarness(t)
		},
		Reopen: func(t *testing.T, current store.AttachmentStore) store.AttachmentStore {
			t.Helper()
			harness := current.(*postgresAttachmentHarness)
			harness.pool.Close()
			harness.reopen(t)
			return harness
		},
	})
}

func TestAttachAttemptStoreContract(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attempt_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_bootstrap'), ('ses_target')`); err != nil {
		t.Fatal(err)
	}
	storetest.AttachAttemptContract(t, storetest.AttachAttemptHarness{
		Open:   func(*testing.T) store.AttachAttemptStore { return postgres.New(pool) },
		Reopen: func(*testing.T, store.AttachAttemptStore) store.AttachAttemptStore { return postgres.New(pool) },
	})
}

func TestWarmAttachStoreContract(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_warm_attach_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ('ses_bootstrap'), ('ses_target')`); err != nil {
		t.Fatal(err)
	}
	warm := postgres.New(pool)
	if _, err := warm.Append(ctx, "ses_target", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatalf("seed warm target state: %v", err)
	}
	if _, err := warm.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_bootstrap", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("initialize warm bootstrap: %v", err)
	}
	if _, err := warm.AcceptAdapterHello(ctx, "ses_bootstrap", store.AdapterHello{CredentialGeneration: 1}); err != nil {
		t.Fatalf("accept warm bootstrap hello: %v", err)
	}
	storetest.WarmAttachContract(t, storetest.WarmAttachHarness{
		Open: func(*testing.T) store.WarmAttachStore { return postgres.New(pool) },
		Fail: func(t *testing.T, _ store.WarmAttachStore, failure storetest.WarmAttachFailure) {
			clearWarmAttachFailpoint(t, pool)
			table := map[storetest.WarmAttachFailure]string{
				storetest.WarmAttachFailureAttempt:    "session_attach_attempts",
				storetest.WarmAttachFailureAttachment: "session_attachments",
				storetest.WarmAttachFailureOutbox:     "session_pending_commands",
				storetest.WarmAttachFailureSummary:    "session_attention_summaries",
			}[failure]
			if table == "" {
				t.Fatalf("unknown warm attach failure %q", failure)
			}
			if _, err := pool.Exec(context.Background(), fmt.Sprintf(`CREATE FUNCTION warm_attach_failpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'warm attach %s failpoint'; END $$; CREATE TRIGGER warm_attach_failpoint BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION warm_attach_failpoint()`, failure, table)); err != nil {
				t.Fatalf("install warm attach %s failpoint: %v", failure, err)
			}
		},
		Expire: func(t *testing.T, _ store.WarmAttachStore) {
			if _, err := pool.Exec(context.Background(), `UPDATE session_attachments SET created_at = clock_timestamp() - interval '2 minutes', expires_at = clock_timestamp() - interval '1 second' WHERE attach_id = 'att_warm'`); err != nil {
				t.Fatalf("expire warm attachment fixture: %v", err)
			}
		},
		Absent: func(t *testing.T, _ store.WarmAttachStore, _ store.WarmAttachRequest) {
			clearWarmAttachFailpoint(t, pool)
			var attempts, attachments, commands, references int
			if err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM session_attach_attempts WHERE attach_id = 'att_warm'), (SELECT count(*) FROM session_attachments WHERE attach_id = 'att_warm'), (SELECT count(*) FROM session_pending_commands WHERE cmd_id = 'cmd_warm'), (SELECT count(*) FROM session_events WHERE session_id = 'ses_target' AND payload->>'reference_id' = 'ref_warm')`).Scan(&attempts, &attachments, &commands, &references); err != nil {
				t.Fatalf("inspect rolled-back warm attach: %v", err)
			}
			if attempts+attachments+commands+references != 0 {
				t.Fatalf("warm attach rollback left durable rows attempts=%d attachments=%d commands=%d references=%d", attempts, attachments, commands, references)
			}
			var connections int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM session_adapter_connections WHERE session_id = 'ses_target'`).Scan(&connections); err != nil || connections != 0 {
				t.Fatalf("warm attach rollback left target credential connections=%d err=%v", connections, err)
			}
			var latest int64
			var blocker *string
			if err := pool.QueryRow(context.Background(), `SELECT latest_seq, blocker_kind FROM session_attention_summaries WHERE session_id = 'ses_target'`).Scan(&latest, &blocker); err != nil || latest != 1 || blocker != nil {
				t.Fatalf("warm attach rollback changed target summary latest=%d blocker=%v err=%v", latest, blocker, err)
			}
		},
	})
}

func clearWarmAttachFailpoint(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, table := range []string{"session_attach_attempts", "session_attachments", "session_pending_commands", "session_attention_summaries"} {
		if _, err := pool.Exec(context.Background(), "DROP TRIGGER IF EXISTS warm_attach_failpoint ON "+table); err != nil {
			t.Fatalf("drop warm attach failpoint on %s: %v", table, err)
		}
	}
	if _, err := pool.Exec(context.Background(), "DROP FUNCTION IF EXISTS warm_attach_failpoint()"); err != nil {
		t.Fatalf("drop warm attach failpoint function: %v", err)
	}
}

func TestWarmAttachRejectsFencedAdmissionAndTerminalTarget(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, warm *postgres.Store, pool *pgxpool.Pool, request *store.WarmAttachRequest)
	}{
		{name: "wrong_credential_generation", mutate: func(_ *testing.T, _ *postgres.Store, _ *pgxpool.Pool, request *store.WarmAttachRequest) {
			request.BootstrapAdmission.CredentialGeneration++
		}},
		{name: "grant_does_not_advance_fence", mutate: func(_ *testing.T, _ *postgres.Store, _ *pgxpool.Pool, request *store.WarmAttachRequest) {
			request.BootstrapAdmission.GrantFence = request.BootstrapAdmission.AcceptedFence
		}},
		{name: "stale_hello_epoch", mutate: func(t *testing.T, warm *postgres.Store, _ *pgxpool.Pool, _ *store.WarmAttachRequest) {
			if _, err := warm.AcceptAdapterHello(context.Background(), "ses_bootstrap", store.AdapterHello{CredentialGeneration: 1}); err != nil {
				t.Fatalf("advance bootstrap hello: %v", err)
			}
		}},
		{name: "revoked_bootstrap", mutate: func(t *testing.T, _ *postgres.Store, pool *pgxpool.Pool, _ *store.WarmAttachRequest) {
			if _, err := pool.Exec(context.Background(), `UPDATE session_adapter_connections SET revoked_at = clock_timestamp() WHERE session_id = 'ses_bootstrap'`); err != nil {
				t.Fatalf("revoke bootstrap: %v", err)
			}
		}},
		{name: "terminal_target", mutate: func(t *testing.T, warm *postgres.Store, _ *pgxpool.Pool, _ *store.WarmAttachRequest) {
			if _, err := warm.Append(context.Background(), "ses_target", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
				t.Fatalf("end target: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool, warm := newWarmAttachStore(t)
			request := warmAttachRequestForPostgres()
			test.mutate(t, warm, pool, &request)
			if _, err := warm.CommitWarmAttach(context.Background(), request); err == nil {
				t.Fatal("fenced warm attach committed")
			}
			assertWarmAttachRowsAbsent(t, pool)
		})
	}
}

func TestWarmAttachReplayIsReferenceOnly(t *testing.T) {
	pool, warm := newWarmAttachStore(t)
	request := warmAttachRequestForPostgres()
	commit, err := warm.CommitWarmAttach(context.Background(), request)
	if err != nil {
		t.Fatalf("commit warm attach: %v", err)
	}
	target, err := warm.AdapterConnection(context.Background(), request.Attachment.Identity.TargetSessionID)
	if err != nil || target.ConnectionEpoch != 0 || target.AcceptedFence != 0 || target.ActiveCredentialGeneration != request.TargetActivation.Generation || target.CredentialGenerationHighWatermark != request.TargetActivation.Generation || !target.ActiveCredentialExpiresAt.Equal(request.TargetActivation.ExpiresAt) || target.RevokedAt != nil || target.TerminalAt != nil {
		t.Fatalf("committed target activation = %+v, %v", target, err)
	}
	if err := warm.ValidateWarmAttachTargetActivation(context.Background(), request.Attachment.Identity.TargetSessionID, request.TargetActivation); err != nil {
		t.Fatalf("validate current warm target activation: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE session_adapter_connections SET active_credential_expires_at = clock_timestamp() - interval '1 millisecond' WHERE session_id = $1`, request.Attachment.Identity.TargetSessionID); err != nil {
		t.Fatalf("expire target activation: %v", err)
	}
	if err := warm.ValidateWarmAttachTargetActivation(context.Background(), request.Attachment.Identity.TargetSessionID, request.TargetActivation); err == nil {
		t.Fatal("expired target activation passed Store-clock recheck")
	}
	var replayed int
	err = warm.Replay(context.Background(), request.Attachment.Identity.TargetSessionID, 1, func(event store.Event) error {
		replayed++
		if event.Seq != commit.Outbox.EventSeq || event.Type != "session.message" ||
			!strings.Contains(string(event.Payload), `"reference_id":"ref_auth"`) {
			return fmt.Errorf("unexpected warm attach replay event: %+v", event)
		}
		for _, forbidden := range []string{"content", "grant", "bearer", "credential"} {
			if strings.Contains(strings.ToLower(string(event.Payload)), forbidden) {
				return fmt.Errorf("warm attach replay leaked %s", forbidden)
			}
		}
		return nil
	})
	if err != nil || replayed != 1 {
		t.Fatalf("replay warm attach = events:%d err:%v", replayed, err)
	}
}

func TestWarmAttachReceivedReceiptExpiresFailClosed(t *testing.T) {
	pool, warm := newWarmAttachStore(t)
	request := warmAttachRequestForPostgres()
	committed, err := warm.CommitWarmAttach(context.Background(), request)
	if err != nil {
		t.Fatalf("commit warm attach: %v", err)
	}
	claimed, err := warm.UpdateAttachment(context.Background(), committed.Attachment.Identity.AttachID, committed.Attachment.DeliveryVersion, store.AttachmentUpdate{
		Status: store.AttachmentJoinPending, DeliveryState: store.AttachmentDeliveryReceived, ExpiresAt: committed.Attachment.ExpiresAt,
	})
	if err != nil || claimed.Attachment.DeliveryState != store.AttachmentDeliveryReceived {
		t.Fatalf("claim credential receipt = %+v, %v", claimed, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE session_attachments SET created_at = clock_timestamp() - interval '2 minutes', expires_at = clock_timestamp() - interval '1 second' WHERE attach_id = $1`, claimed.Attachment.Identity.AttachID); err != nil {
		t.Fatalf("expire claimed credential receipt: %v", err)
	}
	expired, err := warm.ExpireWarmAttach(context.Background(), claimed.Attachment.Identity.AttachID, claimed.Attachment.DeliveryVersion)
	if err != nil || expired.Attachment.Status != store.AttachmentReauthorizationRequired || expired.Attachment.DeliveryState != store.AttachmentDeliveryOutcomeUnknown || expired.Summary.Blocker == nil || expired.Summary.Blocker.Kind != store.AttentionBlockerOutcomeUnknown || expired.Summary.Blocker.Operation == nil || *expired.Summary.Blocker.Operation != "credential_handoff" {
		t.Fatalf("expire claimed credential receipt = %+v, %v", expired, err)
	}
}

func newWarmAttachStore(t *testing.T) (*pgxpool.Pool, *postgres.Store) {
	t.Helper()
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_warm_attach_reject_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	pool := openPool(t, dsn, schemaName, nil)
	resetSchema(t, pool)
	warm := postgres.New(pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ('ses_bootstrap'), ('ses_target')`); err != nil {
		t.Fatal(err)
	}
	if _, err := warm.Append(ctx, "ses_target", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatalf("seed warm target state: %v", err)
	}
	if _, err := warm.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_bootstrap", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("initialize warm bootstrap: %v", err)
	}
	if _, err := warm.AcceptAdapterHello(ctx, "ses_bootstrap", store.AdapterHello{CredentialGeneration: 1}); err != nil {
		t.Fatalf("accept warm bootstrap hello: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropSchema(t, dsn, schemaName)
	})
	return pool, warm
}

func warmAttachRequestForPostgres() store.WarmAttachRequest {
	grantExpiresAt := time.Now().Add(3 * time.Minute)
	deliveryExpiresAt := time.Now().Add(20 * time.Second).UTC().Truncate(time.Microsecond)
	issuedGeneration := int64(1)
	return store.WarmAttachRequest{
		Attempt: store.AttachAttemptRequest{
			Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{9}, AttachID: "att_auth", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
			Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{8}, KeyVersion: 1},
			ExpiresAt:   grantExpiresAt, Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: &issuedGeneration,
		},
		Attachment:         store.AttachmentCreate{Identity: store.AttachmentIdentity{AttachID: "att_auth", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", TargetCredentialLineageRef: "lineage_target"}, ExpiresAt: deliveryExpiresAt},
		TargetActivation:   store.WarmAttachTargetActivation{Generation: issuedGeneration, ExpiresAt: deliveryExpiresAt},
		BootstrapAdmission: store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: 1, AcceptedFence: 1, GrantFence: 2},
		FirstDelivery:      store.WarmAttachFirstDelivery{CommandID: "cmd_auth", ReferenceID: "ref_auth", ReferenceDigest: [32]byte{7}, ExpiresAt: deliveryExpiresAt},
	}
}

func assertWarmAttachRowsAbsent(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var attempts, attachments, commands, references int
	if err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM session_attach_attempts WHERE attach_id = 'att_auth'), (SELECT count(*) FROM session_attachments WHERE attach_id = 'att_auth'), (SELECT count(*) FROM session_pending_commands WHERE cmd_id = 'cmd_auth'), (SELECT count(*) FROM session_events WHERE session_id = 'ses_target' AND payload->>'reference_id' = 'ref_auth')`).Scan(&attempts, &attachments, &commands, &references); err != nil {
		t.Fatalf("inspect fenced warm attach: %v", err)
	}
	if attempts+attachments+commands+references != 0 {
		t.Fatalf("fenced warm attach left durable rows attempts=%d attachments=%d commands=%d references=%d", attempts, attachments, commands, references)
	}
	var connections int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM session_adapter_connections WHERE session_id = 'ses_target'`).Scan(&connections); err != nil || connections != 0 {
		t.Fatalf("fenced warm attach left target credential connections=%d err=%v", connections, err)
	}
}

func TestAttachAttemptFailureLeavesNoRow(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attempt_rollback_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_bootstrap')`); err != nil {
		t.Fatal(err)
	}
	generation := int64(1)
	request := store.AttachAttemptRequest{
		Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{1}, AttachID: "att_rollback", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
		Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{2}, KeyVersion: 1},
		ExpiresAt:   time.Now().Add(time.Minute), Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: &generation,
	}
	attempts := postgres.New(pool)
	if _, err := attempts.CommitAttachAttempt(context.Background(), request); err == nil {
		t.Fatal("missing target attempt unexpectedly committed")
	}
	if _, err := attempts.AttachAttempt(context.Background(), request.Identity.JTIHash); err == nil {
		t.Fatal("failed attempt remained durable")
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_target')`); err != nil {
		t.Fatal(err)
	}
	if result, err := attempts.CommitAttachAttempt(context.Background(), request); err != nil || result.Duplicate {
		t.Fatalf("retry after rollback = %+v, %v", result, err)
	}
}

func TestAttachmentCreateRollback(t *testing.T) {
	harness := newPostgresAttachmentHarness(t)
	request := store.AttachmentCreate{
		Identity: store.AttachmentIdentity{
			AttachID:                   "attach_rollback",
			BootstrapSessionID:         "ses_bootstrap_lookup",
			TargetSessionID:            "ses_target_rollback",
			TargetCredentialLineageRef: "lineage_attach_rollback",
		},
		ExpiresAt: time.Now().Add(20 * time.Second),
	}
	if _, err := harness.CreateAttachment(context.Background(), request); err == nil {
		t.Fatal("CreateAttachment() without target FK unexpectedly succeeded")
	}
	if _, err := harness.Attachment(context.Background(), request.Identity.AttachID); err == nil {
		t.Fatal("failed attachment create left durable state")
	}
	if _, err := harness.pool.Exec(context.Background(), "INSERT INTO agent_sessions (id) VALUES ($1)", request.Identity.TargetSessionID); err != nil {
		t.Fatalf("seed rollback target session: %v", err)
	}
	if commit, err := harness.CreateAttachment(context.Background(), request); err != nil || commit.Noop {
		t.Fatalf("CreateAttachment() after rollback = %+v, %v; want new attachment", commit, err)
	}
}

func TestAttachmentExpiredRetryAndStart(t *testing.T) {
	harness := newPostgresAttachmentHarness(t)
	request := store.AttachmentCreate{
		Identity: store.AttachmentIdentity{
			AttachID: "attach_expired_retry", BootstrapSessionID: "ses_bootstrap_expired_retry",
			TargetSessionID: "ses_target_expired_retry", TargetCredentialLineageRef: "lineage_expired_retry",
		},
		ExpiresAt: time.Now().Add(250 * time.Millisecond),
	}
	created, err := harness.CreateAttachment(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	time.Sleep(time.Until(request.ExpiresAt) + 100*time.Millisecond)
	retry, err := harness.CreateAttachment(context.Background(), request)
	if err != nil || !retry.Noop || retry.Attachment.Identity != created.Attachment.Identity {
		t.Fatalf("expired exact retry = %+v, %v; want durable no-op", retry, err)
	}
	started := store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived}
	if _, err := harness.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, started); err == nil {
		t.Fatal("expired attachment unexpectedly recorded start receipt")
	}
	stored, err := harness.Attachment(context.Background(), request.Identity.AttachID)
	if err != nil || stored.Status != store.AttachmentJoinPending || stored.DeliveryVersion != 0 {
		t.Fatalf("expired attachment after rejected start = %+v, %v", stored, err)
	}
}

func TestAttachmentOutcomeOperationIsBounded(t *testing.T) {
	harness := newPostgresAttachmentHarness(t)
	request := store.AttachmentCreate{
		Identity: store.AttachmentIdentity{
			AttachID: "attach_operation", BootstrapSessionID: "ses_bootstrap_operation",
			TargetSessionID: "ses_target_operation", TargetCredentialLineageRef: "lineage_operation",
		},
		ExpiresAt: time.Now().Add(20 * time.Second),
	}
	if _, err := harness.CreateAttachment(context.Background(), request); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	reauthorize := store.AttachmentUpdate{Status: store.AttachmentReauthorizationRequired, DeliveryState: store.AttachmentDeliveryPending,
		Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerReauthorizationRequired}}
	if _, err := harness.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, reauthorize); err != nil {
		t.Fatalf("reauthorization update: %v", err)
	}
	for _, operation := range []string{"other", strings.Repeat("x", 129)} {
		unknown := store.AttachmentUpdate{Status: store.AttachmentReauthorizationRequired, DeliveryState: store.AttachmentDeliveryOutcomeUnknown,
			Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerOutcomeUnknown, Operation: &operation}}
		if _, err := harness.UpdateAttachment(context.Background(), request.Identity.AttachID, 1, unknown); err == nil {
			t.Fatalf("outcome_unknown operation %q unexpectedly succeeded", operation)
		}
	}
}

func TestProposedEventStoreContract(t *testing.T) {
	storetest.ProposalContract(t, storetest.ProposalHarness{
		Open: func(t *testing.T) store.ProposedEventStore {
			t.Helper()
			return newPostgresProposalHarness(t)
		},
		Reopen: func(t *testing.T, current store.ProposedEventStore) store.ProposedEventStore {
			t.Helper()
			harness := current.(*postgresProposalHarness)
			harness.pool.Close()
			harness.reopen(t)
			return harness
		},
		Authority: func(t *testing.T, _ store.ProposedEventStore) store.CommandAuthority {
			t.Helper()
			return store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
		},
		Invalidate: func(t *testing.T, current store.ProposedEventStore, kind storetest.CommandAuthorityFailure) {
			t.Helper()
			harness := current.(*postgresProposalHarness)
			var statement string
			switch kind {
			case storetest.CommandAuthoritySuperseded:
				statement = "UPDATE session_adapter_connections SET connection_epoch = 2"
			case storetest.CommandAuthorityRevoked:
				statement = "UPDATE session_adapter_connections SET revoked_at = clock_timestamp()"
			case storetest.CommandAuthorityExpired:
				statement = "UPDATE session_adapter_connections SET created_at = clock_timestamp() - interval '2 minutes', active_credential_expires_at = clock_timestamp() - interval '1 minute'"
			case storetest.CommandAuthorityTerminal:
				statement = "UPDATE session_adapter_connections SET terminal_at = clock_timestamp()"
			default:
				t.Fatalf("unknown proposal authority failure %q", kind)
			}
			if _, err := harness.pool.Exec(context.Background(), statement); err != nil {
				t.Fatalf("invalidate proposal authority %s: %v", kind, err)
			}
		},
	})
}

func TestProposedEventRollback(t *testing.T) {
	harness := newPostgresProposalHarness(t)
	if _, err := harness.pool.Exec(context.Background(), `
CREATE FUNCTION reject_test_proposed_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.proposal_id IS NOT NULL THEN RAISE EXCEPTION 'forced proposed event failure'; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER session_events_reject_test_proposal
BEFORE INSERT ON session_events FOR EACH ROW EXECUTE FUNCTION reject_test_proposed_event();
`); err != nil {
		t.Fatalf("install proposed event failpoint: %v", err)
	}
	_, err := harness.CommitProposedEvent(context.Background(), "ses_proposal_1",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
		store.ProposedEventRequest{ProposalID: "proposal_rollback", Event: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"starting"}`)}})
	if err == nil {
		t.Fatal("CommitProposedEvent() unexpectedly succeeded through failpoint")
	}
	assertProposedEventRollback(t, harness, "ses_proposal_1")
}

func TestProposedEventProjectsAttention(t *testing.T) {
	harness := newPostgresProposalHarness(t)
	// The projection stamps last_durable_event_at with the Postgres Store clock,
	// which can lag the test host clock by milliseconds under full-suite load.
	// A one-minute tolerance keeps the "not the 1970 event time" assertion while
	// absorbing that skew instead of flaking.
	before := time.Now().Add(-time.Minute)
	receipt, err := harness.CommitProposedEvent(context.Background(), "ses_proposal_1",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
		store.ProposedEventRequest{ProposalID: "proposal_attention", Event: store.PendingEvent{Type: "session.state", Time: time.Unix(1, 0), Payload: []byte(`{"state":"working"}`)}})
	if err != nil || receipt.Seq != 1 {
		t.Fatalf("commit proposed attention event = %+v, %v", receipt, err)
	}
	snapshot, err := harness.AttentionSnapshot(context.Background(), []string{"ses_proposal_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != receipt.Seq || snapshot[0].State != "busy" || snapshot[0].LatestChangeSeq == nil || *snapshot[0].LatestChangeSeq != receipt.Seq || snapshot[0].LastDurableEventAt == nil || snapshot[0].LastDurableEventAt.Before(before) {
		t.Fatalf("proposed event attention snapshot = %+v, %v", snapshot, err)
	}
}

func TestProposedEventProjectsAgentSessionState(t *testing.T) {
	harness := newPostgresProposalHarness(t)
	receipt, err := harness.CommitProposedEvent(context.Background(), "ses_proposal_1",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
		store.ProposedEventRequest{ProposalID: "proposal_state", Event: store.PendingEvent{Type: "session.state", Time: time.Unix(1, 0), Payload: []byte(`{"state":"ready"}`)}})
	if err != nil || receipt.Seq != 1 {
		t.Fatalf("commit proposed state event = %+v, %v", receipt, err)
	}
	var status string
	if err := harness.pool.QueryRow(context.Background(), `SELECT status FROM agent_sessions WHERE id = 'ses_proposal_1'`).Scan(&status); err != nil {
		t.Fatalf("read agent session status: %v", err)
	}
	if status != "ready" {
		t.Fatalf("proposed session.state did not project: status = %q, want ready", status)
	}
}

func TestAttentionProjectionUsesStoreClockAndNeverRepairsIncomplete(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_clock_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	events := postgres.New(pool)
	before := time.Now().Add(-time.Minute)
	if _, err := events.Append(context.Background(), "ses_attention_clock", []store.PendingEvent{{Type: "session.state", Time: time.Unix(1, 0), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(context.Background(), "ses_attention_incomplete", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"unknown"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(context.Background(), "ses_attention_incomplete", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := events.AttentionSnapshot(context.Background(), []string{"ses_attention_clock", "ses_attention_incomplete"})
	if err != nil || len(snapshot) != 2 || snapshot[0].LastDurableEventAt == nil || snapshot[0].LastDurableEventAt.Before(before) || snapshot[1].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("clock/incomplete attention snapshot = %+v, %v", snapshot, err)
	}
}

func TestAttentionProjectionDoesNotFabricateHistoricalCompleteness(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_historical_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ('ses_attention_historical')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO session_events (session_id, seq, type, payload, created_at) VALUES
    ('ses_attention_historical', 1, 'session.message', '{}', clock_timestamp()),
    ('ses_attention_historical', 2, 'session.message', '{}', clock_timestamp())
`); err != nil {
		t.Fatal(err)
	}
	attention := postgres.New(pool)
	if seq, err := attention.Append(ctx, "ses_attention_historical", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil || seq != 3 {
		t.Fatalf("append historical attention event = %d, %v", seq, err)
	}
	summaries, err := attention.AttentionSnapshot(ctx, []string{"ses_attention_historical"})
	if err != nil || len(summaries) != 1 || summaries[0].LatestSeq != 3 || summaries[0].State != "ready" || summaries[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("historical attention summary = %+v, %v", summaries, err)
	}
}

func TestAttentionProjectionMarksExistingSummaryIncompleteAcrossGap(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_gap_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	attention := postgres.New(pool)
	ctx := context.Background()
	if _, err := attention.Append(ctx, "ses_attention_gap", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"starting"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO session_events (session_id, seq, type, payload, created_at) VALUES ('ses_attention_gap', 2, 'permission.request', '{}', clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	if seq, err := attention.Append(ctx, "ses_attention_gap", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil || seq != 3 {
		t.Fatalf("append across attention gap = %d, %v", seq, err)
	}
	summaries, err := attention.AttentionSnapshot(ctx, []string{"ses_attention_gap"})
	if err != nil || len(summaries) != 1 || summaries[0].LatestSeq != 3 || summaries[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("attention summary across gap = %+v, %v", summaries, err)
	}
}

func TestAttentionProjectionRecoversFromNonStateFirstEvent(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attention_recover_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	attention := postgres.New(pool)
	ctx := context.Background()
	// ACP adapters publish capabilities before the first session.state.
	if _, err := attention.Append(ctx, "ses_attention_recover", []store.PendingEvent{{Type: "session.run.capabilities", Time: time.Now(), Payload: []byte(`{"schema_version":1}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := attention.Append(ctx, "ses_attention_recover", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	summaries, err := attention.AttentionSnapshot(ctx, []string{"ses_attention_recover"})
	if err != nil || len(summaries) != 1 || summaries[0].LatestSeq != 2 || summaries[0].State != "ready" || summaries[0].StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("attention summary after non-state first event = %+v, %v", summaries, err)
	}
}

func TestAttentionProjectionTracksPermissionAndTerminalFence(t *testing.T) {
	harness := newPostgresProposalHarness(t)
	ctx := context.Background()
	if _, err := harness.Append(ctx, "ses_proposal_conflict", []store.PendingEvent{{Type: "permission.request", Time: time.Now(), Payload: []byte(`{"request_id":"pr_attention"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.Append(ctx, "ses_proposal_conflict", []store.PendingEvent{{Type: "permission.decision", Time: time.Now(), Payload: []byte(`{"request_id":"pr_attention","decision":"approve"}`)}}); err != nil {
		t.Fatal(err)
	}
	permission, err := harness.AttentionSnapshot(ctx, []string{"ses_proposal_conflict"})
	if err != nil || len(permission) != 1 || permission[0].Permission != nil {
		t.Fatalf("resolved permission attention snapshot = %+v, %v", permission, err)
	}
	if _, err := harness.CommitProposedEvent(ctx, "ses_proposal_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
		store.ProposedEventRequest{ProposalID: "proposal_terminal", Event: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
		t.Fatal(err)
	}
	terminal, err := harness.AttentionSnapshot(ctx, []string{"ses_proposal_1"})
	if err != nil || len(terminal) != 1 || terminal[0].TerminalOutcome == nil || *terminal[0].TerminalOutcome != "ended" {
		t.Fatalf("terminal attention snapshot = %+v, %v", terminal, err)
	}
	connection, err := harness.AdapterConnection(ctx, "ses_proposal_1")
	if err != nil || connection.TerminalAt == nil || connection.RevokedAt == nil {
		t.Fatalf("terminal adapter connection = %+v, %v", connection, err)
	}
	if _, err := harness.CommitProposedEvent(ctx, "ses_proposal_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
		store.ProposedEventRequest{ProposalID: "proposal_after_terminal", Event: store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"agent"}`)}}); err == nil {
		t.Fatal("terminal adapter accepted a later proposed event")
	}
}

func TestAttentionSnapshotConcurrentClientCommands(t *testing.T) {
	harness := newPostgresCommandHarness(t, "agentwharf_attention_concurrent_command", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const commits = 12
	errs := make(chan error, commits)
	done := make(chan struct{})
	scanErr := make(chan error, 1)
	go func() {
		defer close(scanErr)
		for {
			select {
			case <-done:
				return
			default:
				if _, err := harness.AttentionSnapshot(ctx, []string{"ses_command_1"}); err != nil {
					scanErr <- err
					return
				}
			}
		}
	}()
	for index := 0; index < commits; index++ {
		go func(index int) {
			_, err := harness.CommitPendingCommand(ctx, "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
				store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)},
				store.PendingCommandRequest{CommandID: fmt.Sprintf("cmd_attention_concurrent_%d", index), Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)})
			errs <- err
		}(index)
	}
	for index := 0; index < commits; index++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent command commit: %v", err)
		}
	}
	close(done)
	for err := range scanErr {
		if err != nil {
			t.Fatalf("concurrent attention snapshot: %v", err)
		}
	}
	snapshot, err := harness.AttentionSnapshot(context.Background(), []string{"ses_command_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != commits || snapshot[0].SummaryVersion != commits || snapshot[0].LastClientCommandAt == nil {
		t.Fatalf("concurrent command attention snapshot = %+v, %v", snapshot, err)
	}
}

func TestProposedEventRejectsQueuedAuthorityChange(t *testing.T) {
	updates := map[string]string{
		"epoch":      "UPDATE session_adapter_connections SET connection_epoch = 2 WHERE session_id = 'ses_proposal_1'",
		"generation": "UPDATE session_adapter_connections SET active_credential_generation = 2, credential_generation_high_watermark = 2 WHERE session_id = 'ses_proposal_1'",
		"revoked":    "UPDATE session_adapter_connections SET revoked_at = clock_timestamp() WHERE session_id = 'ses_proposal_1'",
		"expired":    "UPDATE session_adapter_connections SET created_at = clock_timestamp() - interval '2 minutes', active_credential_expires_at = clock_timestamp() - interval '1 minute' WHERE session_id = 'ses_proposal_1'",
		"terminal":   "UPDATE session_adapter_connections SET terminal_at = clock_timestamp() WHERE session_id = 'ses_proposal_1'",
	}
	for name, update := range updates {
		t.Run(name, func(t *testing.T) {
			harness := newPostgresProposalHarness(t)
			tracer := newQueryStartSignal("FROM session_adapter_connections AS authority")
			harness.pool.Close()
			harness.reopenWithTracer(t, tracer)
			blocker := openPool(t, harness.dsn, harness.schemaName, nil)
			t.Cleanup(blocker.Close)
			tx, err := blocker.Begin(context.Background())
			if err != nil {
				t.Fatalf("begin authority blocker: %v", err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err := tx.Exec(context.Background(), "SELECT 1 FROM session_adapter_connections WHERE session_id = 'ses_proposal_1' FOR UPDATE"); err != nil {
				t.Fatalf("lock proposal authority: %v", err)
			}
			result := make(chan error, 1)
			go func() {
				_, commitErr := harness.CommitProposedEvent(context.Background(), "ses_proposal_1",
					store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
					store.ProposedEventRequest{ProposalID: "proposal_queued_" + name, Event: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"starting"}`)}})
				result <- commitErr
			}()
			<-tracer.started
			if _, err := tx.Exec(context.Background(), update); err != nil {
				t.Fatalf("change queued proposal authority: %v", err)
			}
			if err := tx.Commit(context.Background()); err != nil {
				t.Fatalf("commit queued authority change: %v", err)
			}
			if err := <-result; err == nil {
				t.Fatal("queued proposal committed after authority changed")
			}
			assertProposedEventRollback(t, harness, "ses_proposal_1")
		})
	}
}

func TestProposedEventRejectsExpiryDuringStreamWait(t *testing.T) {
	harness := newPostgresProposalHarness(t)
	tracer := newQueryStartSignal("pg_advisory_xact_lock")
	harness.pool.Close()
	harness.reopenWithTracer(t, tracer)
	blocker := openPool(t, harness.dsn, harness.schemaName, nil)
	t.Cleanup(blocker.Close)
	tx, err := blocker.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin proposal stream blocker: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(context.Background(), "SELECT pg_advisory_xact_lock($1)", commandAdvisoryLockKey("ses_proposal_1")); err != nil {
		t.Fatalf("lock proposal stream: %v", err)
	}
	if _, err := harness.pool.Exec(context.Background(), "UPDATE session_adapter_connections SET active_credential_expires_at = clock_timestamp() + interval '250 milliseconds'"); err != nil {
		t.Fatalf("shorten proposal authority: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, commitErr := harness.CommitProposedEvent(context.Background(), "ses_proposal_1",
			store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
			store.ProposedEventRequest{ProposalID: "proposal_expiry_wait", Event: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"starting"}`)}})
		result <- commitErr
	}()
	<-tracer.started
	time.Sleep(300 * time.Millisecond)
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("release proposal stream: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("proposal committed after authority expired during stream wait")
	}
	assertProposedEventRollback(t, harness, "ses_proposal_1")
}

func TestAdapterConnectionStoreContract(t *testing.T) {
	storetest.ConnectionContract(t, storetest.ConnectionHarness{
		Open: func(t *testing.T) store.AdapterConnectionStore {
			t.Helper()
			return newPostgresConnectionHarness(t)
		},
		Invalidate: func(t *testing.T, current store.AdapterConnectionStore, terminal bool) {
			t.Helper()
			harness := current.(*postgresConnectionHarness)
			column := "revoked_at"
			if terminal {
				column = "terminal_at"
			}
			if _, err := harness.pool.Exec(context.Background(), "UPDATE session_adapter_connections SET "+column+" = clock_timestamp() WHERE session_id = 'ses_connection'"); err != nil {
				t.Fatalf("invalidate adapter connection: %v", err)
			}
		},
	})
}

func TestAdapterConnectionAuthorityReceiptContract(t *testing.T) {
	ctx := context.Background()
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal_%v", terminal), func(t *testing.T) {
			connections := newPostgresConnectionHarness(t)
			connection, admission, writer := issuePostgresConnectionAuthorityReceipt(t, connections, time.Now().Add(time.Minute))
			receipt, err := connections.IssueAdapterConnectionAuthorityReceipt(ctx, "ses_connection", admission, writer)
			if err != nil || receipt.SessionID != "ses_connection" || receipt.ConnectionEpoch != connection.ConnectionEpoch || receipt.CredentialGeneration != connection.ActiveCredentialGeneration || receipt.AcceptedFence != connection.AcceptedFence || receipt.WriterLeaseID != writer.LeaseID || !receipt.ExpiresAt.Equal(connection.ActiveCredentialExpiresAt) {
				t.Fatalf("connection authority receipt = %+v, %v", receipt, err)
			}
			for _, invalid := range []struct {
				name      string
				admission store.AdapterConnectionAdmission
				writer    store.SettingsWriter
			}{
				{name: "epoch", admission: store.AdapterConnectionAdmission{CredentialGeneration: admission.CredentialGeneration, ConnectionEpoch: admission.ConnectionEpoch + 1, AcceptedFence: admission.AcceptedFence, GrantFence: admission.GrantFence}, writer: writer},
				{name: "generation", admission: store.AdapterConnectionAdmission{CredentialGeneration: admission.CredentialGeneration + 1, ConnectionEpoch: admission.ConnectionEpoch, AcceptedFence: admission.AcceptedFence, GrantFence: admission.GrantFence}, writer: writer},
				{name: "lease", admission: admission, writer: store.SettingsWriter{ConnectionEpoch: writer.ConnectionEpoch, CredentialGeneration: writer.CredentialGeneration, LeaseID: "replaced-opaque-lease"}},
			} {
				if _, err := connections.IssueAdapterConnectionAuthorityReceipt(ctx, "ses_connection", invalid.admission, invalid.writer); err == nil {
					t.Fatalf("%s authority receipt succeeded", invalid.name)
				}
			}
			column := "revoked_at"
			if terminal {
				column = "terminal_at"
			}
			if _, err := connections.pool.Exec(ctx, "UPDATE session_adapter_connections SET "+column+"=clock_timestamp() WHERE session_id='ses_connection'"); err != nil {
				t.Fatal(err)
			}
			if _, err := connections.IssueAdapterConnectionAuthorityReceipt(ctx, "ses_connection", admission, writer); err == nil {
				t.Fatal("invalidated connection issued authority receipt")
			}
		})
	}
	connections := newPostgresConnectionHarness(t)
	_, admission, writer := issuePostgresConnectionAuthorityReceipt(t, connections, time.Now().Add(60*time.Millisecond))
	time.Sleep(100 * time.Millisecond)
	if _, err := connections.IssueAdapterConnectionAuthorityReceipt(ctx, "ses_connection", admission, writer); err == nil {
		t.Fatal("expired connection issued authority receipt")
	}
}

func issuePostgresConnectionAuthorityReceipt(t *testing.T, connections *postgresConnectionHarness, expiresAt time.Time) (store.AdapterConnection, store.AdapterConnectionAdmission, store.SettingsWriter) {
	t.Helper()
	if _, err := connections.InitializeAdapterConnection(context.Background(), store.AdapterConnectionInitialize{SessionID: "ses_connection", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: expiresAt}); err != nil {
		t.Fatal(err)
	}
	writer := store.SettingsWriter{LeaseID: "opaque-writer-lease"}
	connection, err := connections.AcceptAdapterHello(context.Background(), "ses_connection", store.AdapterHello{CredentialGeneration: 1, WriterLeaseID: writer.LeaseID})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := connections.AllocateAdapterGrantFence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writer.ConnectionEpoch, writer.CredentialGeneration = connection.ConnectionEpoch, connection.ActiveCredentialGeneration
	return connection, store.AdapterConnectionAdmission{CredentialGeneration: connection.ActiveCredentialGeneration, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grant}, writer
}

func TestAdapterConnectionReopenPreservesGlobalFence(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	for _, sessionID := range []string{"ses_connection", "ses_connection_other"} {
		if _, err := harness.InitializeAdapterConnection(context.Background(), store.AdapterConnectionInitialize{
			SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
		}); err != nil {
			t.Fatalf("initialize %s: %v", sessionID, err)
		}
	}
	first, err := harness.AcceptAdapterHello(context.Background(), "ses_connection", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("first global fence: %v", err)
	}
	second, err := harness.AcceptAdapterHello(context.Background(), "ses_connection_other", store.AdapterHello{CredentialGeneration: 1})
	if err != nil || second.AcceptedFence <= first.AcceptedFence {
		t.Fatalf("second global fence = %+v, %v; first=%+v", second, err, first)
	}
	harness.pool.Close()
	harness.reopen(t)
	reopened, err := harness.AdapterConnection(context.Background(), "ses_connection_other")
	if err != nil || !reflect.DeepEqual(reopened, second) {
		t.Fatalf("reopened connection = %+v, %v; want %+v", reopened, err, second)
	}
	third, err := harness.AcceptAdapterHello(context.Background(), "ses_connection", store.AdapterHello{CredentialGeneration: 1})
	if err != nil || third.AcceptedFence <= second.AcceptedFence || third.ConnectionEpoch != first.ConnectionEpoch+1 {
		t.Fatalf("post-reopen fence = %+v, %v; first=%+v second=%+v", third, err, first, second)
	}
}

func TestAdapterConnectionInitializeExactRetry(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	request := store.AdapterConnectionInitialize{
		SessionID: "ses_connection", ActiveCredentialGeneration: 1,
		ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
	}
	first, err := harness.InitializeAdapterConnection(context.Background(), request)
	if err != nil {
		t.Fatalf("first initialize: %v", err)
	}
	retry, err := harness.InitializeAdapterConnection(context.Background(), request)
	if err != nil || !reflect.DeepEqual(retry, first) {
		t.Fatalf("exact initialize retry = %+v, %v; want %+v", retry, err, first)
	}
}

func TestAdapterCredentialRecoveryAdvancesExpiredLineageAndRetriesExactly(t *testing.T) {
	ctx := context.Background()
	harness := newPostgresConnectionHarness(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_recovery", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	original, err := harness.AcceptAdapterHello(ctx, "ses_recovery", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.pool.Exec(ctx, `UPDATE session_adapter_connections
SET created_at = clock_timestamp() - interval '1 hour',
    active_credential_generation = 34,
    credential_generation_high_watermark = 35,
    active_credential_expires_at = clock_timestamp() - interval '2 seconds',
    pending_credential_generation = 35,
    pending_credential_expires_at = clock_timestamp() - interval '1 second',
    prior_recovery_credential_generation = 33,
    rotation_id = 'stale-rotation'
WHERE session_id = 'ses_recovery'`); err != nil {
		t.Fatal(err)
	}

	recovery := store.AdapterCredentialRecovery{
		RefreshBefore: now.Add(2 * time.Minute), ActiveCredentialExpiresAt: now.Add(15 * time.Minute),
	}
	recovered, err := harness.RecoverAdapterCredential(ctx, "ses_recovery", recovery)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ActiveCredentialGeneration != 36 || recovered.CredentialGenerationHighWatermark != 36 ||
		recovered.PendingCredentialGeneration != nil || recovered.PendingCredentialExpiresAt != nil || recovered.RotationID != nil ||
		recovered.PriorRecoveryGeneration == nil || *recovered.PriorRecoveryGeneration != 34 ||
		recovered.ConnectionEpoch != original.ConnectionEpoch || recovered.AcceptedFence != original.AcceptedFence ||
		!recovered.ActiveCredentialExpiresAt.Equal(recovery.ActiveCredentialExpiresAt) {
		t.Fatalf("recovered connection = %+v", recovered)
	}
	retry, err := harness.RecoverAdapterCredential(ctx, "ses_recovery", recovery)
	if err != nil || !reflect.DeepEqual(retry, recovered) {
		t.Fatalf("exact recovery retry = %+v, %v; want %+v", retry, err, recovered)
	}
	if _, err := harness.AcceptAdapterHello(ctx, "ses_recovery", store.AdapterHello{CredentialGeneration: 34}); err == nil {
		t.Fatal("fenced credential generation was accepted")
	}
	accepted, err := harness.AcceptAdapterHello(ctx, "ses_recovery", store.AdapterHello{CredentialGeneration: 36})
	if err != nil || accepted.ConnectionEpoch != original.ConnectionEpoch+1 || accepted.AcceptedFence <= original.AcceptedFence {
		t.Fatalf("recovered hello = %+v, %v", accepted, err)
	}
}

func TestAdapterCredentialRecoveryReissuesLiveGeneration(t *testing.T) {
	ctx := context.Background()
	harness := newPostgresConnectionHarness(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	initial := store.AdapterConnectionInitialize{
		SessionID: "ses_recovery_live", ActiveCredentialGeneration: 7,
		ActiveCredentialExpiresAt: now.Add(10 * time.Minute),
	}
	if _, err := harness.InitializeAdapterConnection(ctx, initial); err != nil {
		t.Fatal(err)
	}
	accepted, err := harness.AcceptAdapterHello(ctx, initial.SessionID, store.AdapterHello{CredentialGeneration: 7})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := harness.RecoverAdapterCredential(ctx, initial.SessionID, store.AdapterCredentialRecovery{
		RefreshBefore: now.Add(2 * time.Minute), ActiveCredentialExpiresAt: now.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered, accepted) {
		t.Fatalf("live recovery = %+v, want unchanged %+v", recovered, accepted)
	}
}

func TestAdapterCredentialRecoveryInitializesMissingAuthority(t *testing.T) {
	ctx := context.Background()
	harness := newPostgresConnectionHarness(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	recovery := store.AdapterCredentialRecovery{
		RefreshBefore: now.Add(2 * time.Minute), ActiveCredentialExpiresAt: now.Add(15 * time.Minute),
	}
	connection, err := harness.RecoverAdapterCredential(ctx, "ses_recovery_missing", recovery)
	if err != nil {
		t.Fatal(err)
	}
	if connection.ActiveCredentialGeneration != 1 || connection.CredentialGenerationHighWatermark != 1 ||
		connection.ConnectionEpoch != 0 || connection.AcceptedFence != 0 ||
		!connection.ActiveCredentialExpiresAt.Equal(recovery.ActiveCredentialExpiresAt) {
		t.Fatalf("initial recovery connection = %+v", connection)
	}
}

func TestAdapterConnectionPreHelloLifecycle(t *testing.T) {
	ctx := context.Background()
	harness := newPostgresConnectionHarness(t)
	refreshInit := store.AdapterConnectionInitialize{SessionID: "ses_refresh", ActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	if _, err := harness.InitializeAdapterConnection(ctx, refreshInit); err != nil {
		t.Fatal(err)
	}
	early := store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: refreshInit.ActiveCredentialExpiresAt.Add(time.Minute)}
	if _, err := harness.RefreshAdapterCredentialBeforeHello(ctx, refreshInit.SessionID, early); err == nil {
		t.Fatal("live credential refresh succeeded")
	}
	expireAdapterConnectionCredential(t, harness, refreshInit.SessionID)
	refresh := store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	refreshed, err := harness.RefreshAdapterCredentialBeforeHello(ctx, refreshInit.SessionID, refresh)
	if err != nil || !refreshed.ActiveCredentialExpiresAt.Equal(refresh.ActiveCredentialExpiresAt) {
		t.Fatalf("refresh = %+v, %v", refreshed, err)
	}
	if exact, err := harness.RefreshAdapterCredentialBeforeHello(ctx, refreshInit.SessionID, refresh); err != nil || !reflect.DeepEqual(exact, refreshed) {
		t.Fatalf("exact refresh = %+v, %v", exact, err)
	}
	for _, invalid := range []store.AdapterCredentialPreHelloRefresh{
		{ExpectedActiveCredentialGeneration: 2, ActiveCredentialExpiresAt: refresh.ActiveCredentialExpiresAt},
		{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(-time.Minute)},
		{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: refresh.ActiveCredentialExpiresAt.Add(-time.Second)},
		{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: refresh.ActiveCredentialExpiresAt.Add(time.Second)},
	} {
		if _, err := harness.RefreshAdapterCredentialBeforeHello(ctx, refreshInit.SessionID, invalid); err == nil {
			t.Fatalf("invalid refresh succeeded: %+v", invalid)
		}
	}
	if _, err := harness.AcceptAdapterHello(ctx, refreshInit.SessionID, store.AdapterHello{CredentialGeneration: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.RefreshAdapterCredentialBeforeHello(ctx, refreshInit.SessionID, refresh); err == nil {
		t.Fatal("post-hello refresh succeeded")
	}
	if _, err := harness.TerminateAdapterConnectionBeforeHello(ctx, refreshInit.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 3}); err == nil {
		t.Fatal("post-hello termination succeeded")
	}

	terminateInit := store.AdapterConnectionInitialize{SessionID: "ses_terminate", ActiveCredentialGeneration: 4, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := harness.InitializeAdapterConnection(ctx, terminateInit); err != nil {
		t.Fatal(err)
	}
	termination := store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 4}
	terminated, err := harness.TerminateAdapterConnectionBeforeHello(ctx, terminateInit.SessionID, termination)
	if err != nil || terminated.RevokedAt == nil || terminated.TerminalAt == nil || !terminated.RevokedAt.Equal(*terminated.TerminalAt) {
		t.Fatalf("termination = %+v, %v", terminated, err)
	}
	if exact, err := harness.TerminateAdapterConnectionBeforeHello(ctx, terminateInit.SessionID, termination); err != nil || !reflect.DeepEqual(exact, terminated) {
		t.Fatalf("exact termination = %+v, %v", exact, err)
	}
	if _, err := harness.AcceptAdapterHello(ctx, terminateInit.SessionID, store.AdapterHello{CredentialGeneration: 4}); err == nil {
		t.Fatal("late hello succeeded")
	}

	for _, item := range []struct {
		id, column string
	}{{"ses_revoked", "revoked_at"}, {"ses_terminal", "terminal_at"}} {
		if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: item.id, ActiveCredentialGeneration: 5, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if _, err := harness.pool.Exec(ctx, "UPDATE session_adapter_connections SET "+item.column+" = clock_timestamp() WHERE session_id = $1", item.id); err != nil {
			t.Fatal(err)
		}
		if _, err := harness.RefreshAdapterCredentialBeforeHello(ctx, item.id, store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 5, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err == nil {
			t.Fatal("invalidated refresh succeeded")
		}
		if _, err := harness.TerminateAdapterConnectionBeforeHello(ctx, item.id, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 5}); err == nil {
			t.Fatal("invalidated termination succeeded")
		}
	}
}

func TestAdapterConnectionPreHelloLifecycleRollbackAndRaces(t *testing.T) {
	ctx := context.Background()
	harness := newPostgresConnectionHarness(t)
	refreshInit := store.AdapterConnectionInitialize{SessionID: "ses_refresh_rollback", ActiveCredentialGeneration: 6, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	if _, err := harness.InitializeAdapterConnection(ctx, refreshInit); err != nil {
		t.Fatal(err)
	}
	expireAdapterConnectionCredential(t, harness, refreshInit.SessionID)
	before := mustConnection(t, harness, refreshInit.SessionID)
	tx, err := harness.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.NewAdapterConnectionTx(tx).RefreshAdapterCredentialBeforeHello(ctx, refreshInit.SessionID, store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 6, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)
	if got := mustConnection(t, harness, refreshInit.SessionID); !reflect.DeepEqual(got, before) {
		t.Fatalf("refresh rollback = %+v, want %+v", got, before)
	}

	terminateInit := store.AdapterConnectionInitialize{SessionID: "ses_terminate_rollback", ActiveCredentialGeneration: 7, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := harness.InitializeAdapterConnection(ctx, terminateInit); err != nil {
		t.Fatal(err)
	}
	tx, err = harness.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.NewAdapterConnectionTx(tx).TerminateAdapterConnectionBeforeHello(ctx, terminateInit.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 7}); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)
	if got := mustConnection(t, harness, terminateInit.SessionID); got.RevokedAt != nil || got.TerminalAt != nil {
		t.Fatalf("termination rollback = %+v", got)
	}

	raceInit := store.AdapterConnectionInitialize{SessionID: "ses_terminate_hello_race", ActiveCredentialGeneration: 8, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := harness.InitializeAdapterConnection(ctx, raceInit); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var terminateErr, helloErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, terminateErr = harness.TerminateAdapterConnectionBeforeHello(ctx, raceInit.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 8})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, helloErr = harness.AcceptAdapterHello(ctx, raceInit.SessionID, store.AdapterHello{CredentialGeneration: 8})
	}()
	close(start)
	wg.Wait()
	if (terminateErr == nil) == (helloErr == nil) {
		t.Fatalf("terminate/hello race: terminate=%v hello=%v", terminateErr, helloErr)
	}
	refreshHelloInit := store.AdapterConnectionInitialize{SessionID: "ses_refresh_hello_race", ActiveCredentialGeneration: 9, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	if _, err := harness.InitializeAdapterConnection(ctx, refreshHelloInit); err != nil {
		t.Fatal(err)
	}
	expireAdapterConnectionCredential(t, harness, refreshHelloInit.SessionID)
	refreshHello := store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 9, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	start = make(chan struct{})
	var refreshErr error
	helloErr = nil
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, refreshErr = harness.RefreshAdapterCredentialBeforeHello(ctx, refreshHelloInit.SessionID, refreshHello)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, helloErr = harness.AcceptAdapterHello(ctx, refreshHelloInit.SessionID, store.AdapterHello{CredentialGeneration: 9})
	}()
	close(start)
	wg.Wait()
	if refreshErr != nil || (helloErr == nil) != (mustConnection(t, harness, refreshHelloInit.SessionID).ConnectionEpoch == 1) {
		t.Fatalf("refresh/hello race: refresh=%v hello=%v", refreshErr, helloErr)
	}

	refreshTerminateInit := store.AdapterConnectionInitialize{SessionID: "ses_refresh_terminate_race", ActiveCredentialGeneration: 10, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	if _, err := harness.InitializeAdapterConnection(ctx, refreshTerminateInit); err != nil {
		t.Fatal(err)
	}
	expireAdapterConnectionCredential(t, harness, refreshTerminateInit.SessionID)
	start = make(chan struct{})
	refreshErr, terminateErr = nil, nil
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, refreshErr = harness.RefreshAdapterCredentialBeforeHello(ctx, refreshTerminateInit.SessionID, store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 10, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, terminateErr = harness.TerminateAdapterConnectionBeforeHello(ctx, refreshTerminateInit.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 10})
	}()
	close(start)
	wg.Wait()
	current := mustConnection(t, harness, refreshTerminateInit.SessionID)
	if terminateErr != nil || current.RevokedAt == nil || current.TerminalAt == nil {
		t.Fatalf("refresh/terminate race = %+v, refresh=%v terminate=%v", current, refreshErr, terminateErr)
	}

	exactInit := store.AdapterConnectionInitialize{SessionID: "ses_exact_race", ActiveCredentialGeneration: 11, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	if _, err := harness.InitializeAdapterConnection(ctx, exactInit); err != nil {
		t.Fatal(err)
	}
	expireAdapterConnectionCredential(t, harness, exactInit.SessionID)
	exact := store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 11, ActiveCredentialExpiresAt: time.Now().Add(time.Minute).Truncate(time.Microsecond)}
	if _, err := harness.RefreshAdapterCredentialBeforeHello(ctx, exactInit.SessionID, exact); err != nil {
		t.Fatal(err)
	}
	exactTerminateInit := store.AdapterConnectionInitialize{SessionID: "ses_exact_terminate", ActiveCredentialGeneration: 12, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := harness.InitializeAdapterConnection(ctx, exactTerminateInit); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 16)
	start = make(chan struct{})
	for range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, err := harness.RefreshAdapterCredentialBeforeHello(ctx, exactInit.SessionID, exact)
			errs <- err
		}()
		go func() {
			defer wg.Done()
			<-start
			_, err := harness.TerminateAdapterConnectionBeforeHello(ctx, exactTerminateInit.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 12})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("exact retry race: %v", err)
		}
	}
}

func expireAdapterConnectionCredential(t *testing.T, harness *postgresConnectionHarness, sessionID string) {
	t.Helper()
	if _, err := harness.pool.Exec(
		context.Background(),
		`UPDATE session_adapter_connections
		 SET created_at = clock_timestamp() - interval '2 seconds',
		     active_credential_expires_at = clock_timestamp() - interval '1 second',
		     updated_at = clock_timestamp()
		 WHERE session_id = $1`,
		sessionID,
	); err != nil {
		t.Fatalf("expire adapter credential %s: %v", sessionID, err)
	}
}

func TestAdapterConnectionRejectsPreHelloLiveOperations(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	request := store.AdapterConnectionInitialize{
		SessionID: "ses_connection", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
	}
	initialized, err := harness.InitializeAdapterConnection(context.Background(), request)
	if err != nil {
		t.Fatalf("initialize pre-hello connection: %v", err)
	}
	if _, err := harness.ValidateAdapterAdmission(context.Background(), request.SessionID, store.AdapterConnectionAdmission{
		CredentialGeneration: 1, ConnectionEpoch: 0, AcceptedFence: 0, GrantFence: 1,
	}); err == nil {
		t.Fatal("pre-hello admission unexpectedly succeeded")
	}
	if _, err := harness.PrepareAdapterCredentialRotation(context.Background(), request.SessionID, store.AdapterCredentialRotation{
		ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: 0, PendingGeneration: 2,
		ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_prehello",
	}); err == nil {
		t.Fatal("pre-hello rotation unexpectedly succeeded")
	}
	if _, err := harness.pool.Exec(context.Background(), `
UPDATE session_adapter_connections
SET pending_credential_generation = 2,
    pending_credential_expires_at = clock_timestamp() + interval '1 minute',
    rotation_id = 'rot_prehello', credential_generation_high_watermark = 2
WHERE session_id = 'ses_connection'
`); err != nil {
		t.Fatalf("seed pre-hello pending generation: %v", err)
	}
	pending := mustConnection(t, harness, request.SessionID)
	if _, err := harness.ActivateAdapterCredential(context.Background(), request.SessionID, store.AdapterCredentialActivation{
		ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: 0, PendingGeneration: 2, RotationID: "rot_prehello",
	}); err == nil {
		t.Fatal("pre-hello activation unexpectedly succeeded")
	}
	if current := mustConnection(t, harness, request.SessionID); !reflect.DeepEqual(current, pending) || initialized.ConnectionEpoch != 0 {
		t.Fatalf("pre-hello rejection mutated connection = %+v, want %+v", current, pending)
	}
}

func TestAdapterConnectionCallerOwnedTransaction(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	request := store.AdapterConnectionInitialize{
		SessionID: "ses_connection_transaction", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
	}
	tx, err := harness.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin caller transaction: %v", err)
	}
	bound := postgres.NewAdapterConnectionTx(tx)
	if _, err := bound.InitializeAdapterConnection(context.Background(), request); err != nil {
		t.Fatalf("initialize in caller transaction: %v", err)
	}
	if _, err := harness.AdapterConnection(context.Background(), request.SessionID); err == nil {
		t.Fatal("uncommitted caller transaction became externally visible")
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback caller transaction: %v", err)
	}
	if _, err := harness.AdapterConnection(context.Background(), request.SessionID); err == nil {
		t.Fatal("rolled back caller transaction left connection lineage")
	}
	tx, err = harness.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin committed caller transaction: %v", err)
	}
	bound = postgres.NewAdapterConnectionTx(tx)
	committed, err := bound.InitializeAdapterConnection(context.Background(), request)
	if err != nil {
		t.Fatalf("initialize committed caller transaction: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit caller transaction: %v", err)
	}
	if current := mustConnection(t, harness, request.SessionID); !reflect.DeepEqual(current, committed) {
		t.Fatalf("committed caller transaction = %+v, want %+v", current, committed)
	}
}

func mustConnection(t *testing.T, harness *postgresConnectionHarness, sessionID string) store.AdapterConnection {
	t.Helper()
	connection, err := harness.AdapterConnection(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("read adapter connection: %v", err)
	}
	return connection
}

func TestPrepareAdapterCredentialRotationSupersedesExpiredPending(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	ctx := context.Background()
	request := store.AdapterConnectionInitialize{SessionID: "ses_expired_pending_recovery", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := harness.InitializeAdapterConnection(ctx, request); err != nil {
		t.Fatalf("initialize connection: %v", err)
	}
	active, err := harness.AcceptAdapterHello(ctx, request.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept hello: %v", err)
	}
	if _, err := harness.PrepareAdapterCredentialRotation(ctx, request.SessionID, store.AdapterCredentialRotation{
		ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: active.ConnectionEpoch, PendingGeneration: 2,
		ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_lost_delivery",
	}); err != nil {
		t.Fatalf("prepare first rotation: %v", err)
	}
	if _, err := harness.pool.Exec(ctx, `UPDATE session_adapter_connections SET created_at = clock_timestamp() - interval '2 minutes', pending_credential_expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, request.SessionID); err != nil {
		t.Fatalf("expire pending rotation: %v", err)
	}
	recovered, err := harness.PrepareAdapterCredentialRotation(ctx, request.SessionID, store.AdapterCredentialRotation{
		ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: active.ConnectionEpoch, PendingGeneration: 3,
		ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_recovered",
	})
	if err != nil || recovered.PendingCredentialGeneration == nil || *recovered.PendingCredentialGeneration != 3 || recovered.RotationID == nil || *recovered.RotationID != "rot_recovered" || recovered.CredentialGenerationHighWatermark != 3 {
		t.Fatalf("expired pending recovery = %+v, %v", recovered, err)
	}
}

type postgresConnectionHarness struct {
	*postgres.Store
	pool       *pgxpool.Pool
	dsn        string
	schemaName string
}

func newPostgresConnectionHarness(t *testing.T) *postgresConnectionHarness {
	t.Helper()
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_connection_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	pool := openPool(t, dsn, schemaName, nil)
	resetSchema(t, pool)
	if _, err := pool.Exec(context.Background(), `CREATE SEQUENCE IF NOT EXISTS session_adapter_connection_accepted_fence_seq AS BIGINT MINVALUE 1 START WITH 1`); err != nil {
		t.Fatalf("create connection fence sequence: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
INSERT INTO agent_sessions (id) SELECT session_id FROM unnest($1::text[]) AS session_id
	`, []string{
		"ses_connection_transaction", "ses_connection", "ses_connection_expired", "ses_connection_active_expiry", "ses_connection_other",
		"ses_refresh", "ses_terminate", "ses_revoked", "ses_terminal", "ses_refresh_rollback", "ses_terminate_rollback",
		"ses_terminate_hello_race", "ses_refresh_hello_race", "ses_refresh_terminate_race", "ses_exact_race", "ses_exact_terminate",
		"ses_expired_pending_recovery",
	}); err != nil {
		t.Fatalf("seed connection sessions: %v", err)
	}
	harness := &postgresConnectionHarness{Store: postgres.New(pool), pool: pool, dsn: dsn, schemaName: schemaName}
	t.Cleanup(func() {
		harness.pool.Close()
		dropSchema(t, dsn, schemaName)
	})
	return harness
}

func (h *postgresConnectionHarness) reopen(t *testing.T) {
	t.Helper()
	h.pool = openPool(t, h.dsn, h.schemaName, nil)
	h.Store = postgres.New(h.pool)
}

func assertProposedEventRollback(t *testing.T, harness *postgresProposalHarness, sessionID string) {
	t.Helper()
	if latest, err := harness.LatestSeq(context.Background(), sessionID); err != nil || latest != 0 {
		t.Fatalf("proposed event rollback latest seq = %d, %v", latest, err)
	}
	var count int
	if err := harness.pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM session_events WHERE proposal_id IS NOT NULL").Scan(&count); err != nil || count != 0 {
		t.Fatalf("proposed event rollback rows = %d, %v", count, err)
	}
}

type postgresProposalHarness struct {
	*postgres.Store
	pool       *pgxpool.Pool
	dsn        string
	schemaName string
}

func newPostgresProposalHarness(t *testing.T) *postgresProposalHarness {
	t.Helper()
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_proposal_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	harness := &postgresProposalHarness{dsn: dsn, schemaName: schemaName}
	harness.reopen(t)
	resetSchema(t, harness.pool)
	harness.seedAuthority(t)
	t.Cleanup(func() {
		harness.pool.Close()
		dropSchema(t, dsn, schemaName)
	})
	return harness
}

func (h *postgresProposalHarness) reopen(t *testing.T) {
	t.Helper()
	h.reopenWithTracer(t, nil)
}

func (h *postgresProposalHarness) reopenWithTracer(t *testing.T, tracer pgx.QueryTracer) {
	t.Helper()
	h.pool = openPool(t, h.dsn, h.schemaName, tracer)
	h.Store = postgres.New(h.pool)
}

func (h *postgresProposalHarness) seedAuthority(t *testing.T) {
	t.Helper()
	sessionIDs := []string{"ses_proposal_1", "ses_proposal_conflict", "ses_proposal_snapshot", "ses_proposal_stale", "ses_proposal_reopen"}
	if _, err := h.pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) SELECT session_id FROM unnest($1::text[]) AS session_id`, sessionIDs); err != nil {
		t.Fatalf("seed proposal sessions: %v", err)
	}
	if _, err := h.pool.Exec(context.Background(), `
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at
)
SELECT session_id, 1, 1, 1, 1, clock_timestamp() + interval '1 hour'
FROM unnest($1::text[]) AS session_id
`, sessionIDs); err != nil {
		t.Fatalf("seed proposal authority: %v", err)
	}
}

type postgresAttachmentHarness struct {
	*postgres.Store
	pool       *pgxpool.Pool
	dsn        string
	schemaName string
}

func newPostgresAttachmentHarness(t *testing.T) *postgresAttachmentHarness {
	t.Helper()
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_attachment_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	harness := &postgresAttachmentHarness{dsn: dsn, schemaName: schemaName}
	harness.reopen(t)
	resetSchema(t, harness.pool)
	harness.seedSessions(t)
	t.Cleanup(func() {
		harness.pool.Close()
		dropSchema(t, dsn, schemaName)
	})
	return harness
}

func (h *postgresAttachmentHarness) reopen(t *testing.T) {
	t.Helper()
	h.pool = openPool(t, h.dsn, h.schemaName, nil)
	h.Store = postgres.New(h.pool)
}

func (h *postgresAttachmentHarness) seedSessions(t *testing.T) {
	t.Helper()
	if _, err := h.pool.Exec(context.Background(), `
INSERT INTO agent_sessions (id) SELECT session_id FROM unnest($1::text[]) AS session_id
`, []string{
		"ses_bootstrap_lookup", "ses_target_lookup", "ses_bootstrap_rewrite", "ses_target_rewrite",
		"ses_bootstrap_mutation", "ses_target_mutation", "ses_blocker_mutation",
		"ses_bootstrap_reconnect", "ses_target_reconnect",
		"ses_bootstrap_healthy", "ses_target_healthy",
		"ses_bootstrap_outcome", "ses_target_outcome", "ses_bootstrap_replacement",
		"ses_bootstrap_expired", "ses_target_expired", "ses_bootstrap_long", "ses_target_long",
		"ses_bootstrap_expiry", "ses_target_expiry", "ses_blocker_expiry",
		"ses_bootstrap_concurrent", "ses_target_concurrent",
		"ses_bootstrap_reopen", "ses_target_reopen",
		"ses_bootstrap_expired_retry", "ses_target_expired_retry",
		"ses_bootstrap_operation", "ses_target_operation",
	}); err != nil {
		t.Fatalf("seed attachment sessions: %v", err)
	}
}

type postgresCommandHarness struct {
	*postgres.Store
	pool       *pgxpool.Pool
	dsn        string
	schemaName string
}

func (h *postgresCommandHarness) reopen(t *testing.T) {
	t.Helper()
	h.reopenWithTracer(t, nil)
}

func (h *postgresCommandHarness) reopenWithTracer(t *testing.T, tracer pgx.QueryTracer) {
	t.Helper()
	h.pool = openPool(t, h.dsn, h.schemaName, tracer)
	h.Store = postgres.New(h.pool)
}

func newPostgresCommandHarness(t *testing.T, prefix string, tracer pgx.QueryTracer) *postgresCommandHarness {
	t.Helper()
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	harness := &postgresCommandHarness{dsn: dsn, schemaName: schemaName}
	harness.reopenWithTracer(t, tracer)
	resetSchema(t, harness.pool)
	harness.seedAuthority(t)
	t.Cleanup(func() {
		harness.pool.Close()
		dropSchema(t, dsn, schemaName)
	})
	return harness
}

func (h *postgresCommandHarness) seedAuthority(t *testing.T) {
	t.Helper()
	sessionIDs := []string{
		"ses_command_1", "ses_command_claim", "ses_command_stale",
		"ses_command_expired", "ses_command_reopen", "ses_command_invalid",
		"ses_command_unknown", "ses_settings_1", "ses_settings_2", "ses_settings_revoked_ack", "ses_settings_revoked_finalize",
		"ses_run_control_1", "ses_run_control_unsupported", "ses_run_control_replacement",
		"ses_file_reference_1", "ses_file_reference_replacement", "ses_file_reference_invalid",
		"ses_file_reference_deadline", "ses_file_reference_cross",
	}
	if _, err := h.pool.Exec(context.Background(), `
INSERT INTO agent_sessions (id) SELECT session_id FROM unnest($1::text[]) AS session_id
`, sessionIDs); err != nil {
		t.Fatalf("seed command sessions: %v", err)
	}
	if _, err := h.pool.Exec(context.Background(), `
INSERT INTO session_adapter_connections (
	session_id, connection_epoch, accepted_fence, active_credential_generation,
	credential_generation_high_watermark, active_credential_expires_at
)
SELECT session_id, 1, 1, 1, 1, statement_timestamp() + interval '1 hour'
FROM unnest($1::text[]) AS session_id
`, sessionIDs); err != nil {
		t.Fatalf("seed command authority: %v", err)
	}
}

type postgresHistoryHarness struct {
	*postgres.Store
	pool       *pgxpool.Pool
	dsn        string
	schemaName string
}

func (h *postgresHistoryHarness) reopen(t *testing.T) {
	t.Helper()

	h.pool = openPool(t, h.dsn, h.schemaName, nil)
	h.Store = postgres.New(h.pool)
}

func TestHistoryUsesInitialSnapshotAcrossConcurrentAppend(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_history_snapshot_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	tracer := newHistorySnapshotTracer()
	pool := openPool(t, dsn, schemaName, tracer)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	postgresStore := postgres.New(pool)
	if _, err := postgresStore.Append(context.Background(), "ses_history_snapshot", []store.PendingEvent{
		{Type: "session.message", Time: time.Unix(1, 0), Payload: []byte(`{"n":1}`)},
	}); err != nil {
		t.Fatalf("append initial history event: %v", err)
	}

	type historyResult struct {
		page store.HistoryPage
		err  error
	}
	result := make(chan historyResult, 1)
	go func() {
		page, err := postgresStore.History(context.Background(), "ses_history_snapshot", nil, 100)
		result <- historyResult{page: page, err: err}
	}()
	<-tracer.pageQuery
	if _, err := postgresStore.Append(context.Background(), "ses_history_snapshot", []store.PendingEvent{
		{Type: "session.message", Time: time.Unix(2, 0), Payload: []byte(`{"n":2}`)},
	}); err != nil {
		t.Fatalf("append concurrent history event: %v", err)
	}
	close(tracer.resume)

	got := <-result
	if got.err != nil {
		t.Fatalf("History() error = %v", got.err)
	}
	if got.page.LatestSeq != 1 || len(got.page.Events) != 1 || got.page.Events[0].Seq != 1 {
		t.Fatalf("history crossed initial snapshot: %+v", got.page)
	}
	if latest, err := postgresStore.LatestSeq(context.Background(), "ses_history_snapshot"); err != nil || latest != 2 {
		t.Fatalf("latest seq after concurrent append = %d, %v", latest, err)
	}
}

func TestHistoryRetainsHighWaterAfterAllEventsArePruned(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_history_all_pruned_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	postgresStore := postgres.New(pool)
	if _, err := postgresStore.Append(context.Background(), "ses_history_all_pruned", []store.PendingEvent{
		{Type: "session.message", Time: time.Unix(1, 0), Payload: []byte(`{"n":1}`)},
		{Type: "session.message", Time: time.Unix(2, 0), Payload: []byte(`{"n":2}`)},
		{Type: "session.message", Time: time.Unix(3, 0), Payload: []byte(`{"n":3}`)},
	}); err != nil {
		t.Fatalf("append history: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM session_events WHERE session_id = 'ses_history_all_pruned'`); err != nil {
		t.Fatalf("prune all history: %v", err)
	}

	page, err := postgresStore.History(context.Background(), "ses_history_all_pruned", nil, 100)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(page.Events) != 0 || page.LatestSeq != 3 || page.RetentionState != store.RetentionGap {
		t.Fatalf("all-pruned history lost durable truth: %+v", page)
	}
	if latest, err := postgresStore.LatestSeq(context.Background(), "ses_history_all_pruned"); err != nil || latest != 3 {
		t.Fatalf("LatestSeq() after all-pruned = %d, %v", latest, err)
	}
}

func TestHistoryReportsInternalRetentionGap(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_history_internal_gap_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	postgresStore := postgres.New(pool)
	events := make([]store.PendingEvent, 5)
	for index := range events {
		events[index] = store.PendingEvent{
			Type: "session.message", Time: time.Unix(int64(index+1), 0), Payload: []byte(`{"n":1}`),
		}
	}
	if _, err := postgresStore.Append(context.Background(), "ses_history_internal_gap", events); err != nil {
		t.Fatalf("append history: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM session_events WHERE session_id = 'ses_history_internal_gap' AND seq = 3`); err != nil {
		t.Fatalf("prune internal history: %v", err)
	}

	page, err := postgresStore.History(context.Background(), "ses_history_internal_gap", nil, 100)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	want := []int64{1, 2, 4, 5}
	if len(page.Events) != len(want) || page.LatestSeq != 5 || page.RetentionState != store.RetentionGap {
		t.Fatalf("internal-gap history = %+v", page)
	}
	for index, event := range page.Events {
		if event.Seq != want[index] {
			t.Fatalf("internal-gap event[%d].Seq = %d, want %d", index, event.Seq, want[index])
		}
	}
}

func TestAppendRollsBackBatchWhenLaterPayloadIsInvalid(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_append_rollback_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	postgresStore := postgres.New(pool)
	if _, err := postgresStore.Append(context.Background(), "ses_append_rollback", []store.PendingEvent{
		{Type: "session.message", Time: time.Unix(1, 0), Payload: []byte(`{"n":1}`)},
		{Type: "session.message", Time: time.Unix(2, 0), Payload: []byte(`not-json`)},
	}); err == nil {
		t.Fatal("Append() error = nil, want invalid JSON rollback")
	}
	if latest, err := postgresStore.LatestSeq(context.Background(), "ses_append_rollback"); err != nil || latest != 0 {
		t.Fatalf("latest seq after rollback = %d, %v", latest, err)
	}
}

func TestPendingCommandCommitRollsBackEventWhenLedgerInsertFails(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_command_rollback_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	harness := &postgresCommandHarness{dsn: dsn, schemaName: schemaName}
	harness.reopen(t)
	t.Cleanup(harness.pool.Close)
	resetSchema(t, harness.pool)
	harness.seedAuthority(t)
	if _, err := harness.pool.Exec(context.Background(), `
CREATE FUNCTION reject_test_pending_command() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'forced pending command failure';
END;
$$;
CREATE TRIGGER session_pending_commands_reject_test
BEFORE INSERT ON session_pending_commands
FOR EACH ROW EXECUTE FUNCTION reject_test_pending_command();
`); err != nil {
		t.Fatalf("install pending command failpoint: %v", err)
	}

	_, err := harness.CommitPendingCommand(context.Background(), "ses_command_1",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
		store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)},
		store.PendingCommandRequest{CommandID: "cmd_rollback", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)})
	if err == nil {
		t.Fatal("CommitPendingCommand() unexpectedly succeeded through failpoint")
	}
	if latest, latestErr := harness.LatestSeq(context.Background(), "ses_command_1"); latestErr != nil || latest != 0 {
		t.Fatalf("rolled-back pending command latest seq = %d, %v", latest, latestErr)
	}
	var commandCount int
	if countErr := harness.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM session_pending_commands`).Scan(&commandCount); countErr != nil || commandCount != 0 {
		t.Fatalf("rolled-back pending command rows = %d, %v", commandCount, countErr)
	}
}

func TestPendingCommandRejectsAuthorityExpiredDuringStreamLockWait(t *testing.T) {
	tracer := newQueryStartSignal("pg_advisory_xact_lock")
	harness := newPostgresCommandHarness(t, "agentwharf_command_expiry", tracer)
	blocker := openPool(t, harness.dsn, harness.schemaName, nil)
	t.Cleanup(blocker.Close)
	blockerTx, err := blocker.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin stream blocker: %v", err)
	}
	defer func() { _ = blockerTx.Rollback(context.Background()) }()
	if _, err := blockerTx.Exec(context.Background(), `SELECT pg_advisory_xact_lock($1)`, commandAdvisoryLockKey("ses_command_1")); err != nil {
		t.Fatalf("lock command stream: %v", err)
	}
	if _, err := harness.pool.Exec(context.Background(), `
UPDATE session_adapter_connections
SET active_credential_expires_at = clock_timestamp() + interval '250 milliseconds'
`); err != nil {
		t.Fatalf("shorten command authority: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, commitErr := harness.CommitPendingCommand(context.Background(), "ses_command_1",
			store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
			store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)},
			store.PendingCommandRequest{CommandID: "cmd_expiry_wait", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)})
		result <- commitErr
	}()
	<-tracer.started
	time.Sleep(300 * time.Millisecond)
	if err := blockerTx.Commit(context.Background()); err != nil {
		t.Fatalf("release command stream: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("command committed after authority expired during stream wait")
	}
	assertPendingCommandRollback(t, harness, "ses_command_1")
}

func TestPendingCommandClaimRejectsExpiryDuringRowLockWait(t *testing.T) {
	for _, test := range []struct {
		name              string
		expireAuthority   bool
		commandExpiration time.Duration
	}{
		{name: "authority", expireAuthority: true, commandExpiration: 10 * time.Second},
		{name: "command", commandExpiration: 350 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newPostgresCommandHarness(t, "agentwharf_claim_expiry", nil)
			request := store.PendingCommandRequest{CommandID: "cmd_claim_wait", Type: "session.send", ExpiresAt: time.Now().Add(test.commandExpiration)}
			if _, err := harness.CommitPendingCommand(context.Background(), "ses_command_claim",
				store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
				store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)}, request); err != nil {
				t.Fatalf("prepare blocked claim: %v", err)
			}
			blocker := openPool(t, harness.dsn, harness.schemaName, nil)
			t.Cleanup(blocker.Close)
			blockerTx, err := blocker.Begin(context.Background())
			if err != nil {
				t.Fatalf("begin claim blocker: %v", err)
			}
			defer func() { _ = blockerTx.Rollback(context.Background()) }()
			if _, err := blockerTx.Exec(context.Background(), `
SELECT 1 FROM session_pending_commands
WHERE session_id = 'ses_command_claim' AND cmd_id = 'cmd_claim_wait' FOR UPDATE
`); err != nil {
				t.Fatalf("lock pending command: %v", err)
			}
			if test.expireAuthority {
				if _, err := harness.pool.Exec(context.Background(), `
UPDATE session_adapter_connections
SET active_credential_expires_at = clock_timestamp() + interval '250 milliseconds'
`); err != nil {
					t.Fatalf("shorten claim authority: %v", err)
				}
			}
			tracer := newQueryStartSignal("LockPendingCommandForClaim")
			harness.pool.Close()
			harness.reopenWithTracer(t, tracer)
			result := make(chan error, 1)
			go func() {
				_, claimErr := harness.ClaimPendingCommand(context.Background(), "ses_command_claim",
					store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, request.CommandID)
				result <- claimErr
			}()
			<-tracer.started
			time.Sleep(400 * time.Millisecond)
			if err := blockerTx.Commit(context.Background()); err != nil {
				t.Fatalf("release pending command: %v", err)
			}
			if err := <-result; err == nil {
				t.Fatalf("claim succeeded after %s expired during row wait", test.name)
			}
			assertPendingCommandStatus(t, harness, "ses_command_claim", request.CommandID, store.PendingCommandPending)
		})
	}
}

func TestPendingCommandResolveRejectsAuthorityExpiredDuringRowLockWait(t *testing.T) {
	harness := newPostgresCommandHarness(t, "agentwharf_resolve_expiry", nil)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_resolve_wait", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := harness.CommitPendingCommand(context.Background(), "ses_command_claim", authority,
		store.PendingEvent{Type: "session.message", Time: time.Now(), Payload: []byte(`{"role":"user"}`)}, request); err != nil {
		t.Fatalf("prepare blocked resolve: %v", err)
	}
	if claim, err := harness.ClaimPendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID); err != nil || !claim.Claimed {
		t.Fatalf("claim before blocked resolve = %+v, %v", claim, err)
	}
	blocker := openPool(t, harness.dsn, harness.schemaName, nil)
	t.Cleanup(blocker.Close)
	blockerTx, err := blocker.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin resolve blocker: %v", err)
	}
	defer func() { _ = blockerTx.Rollback(context.Background()) }()
	if _, err := blockerTx.Exec(context.Background(), `
SELECT 1 FROM session_pending_commands
WHERE session_id = 'ses_command_claim' AND cmd_id = 'cmd_resolve_wait' FOR UPDATE
`); err != nil {
		t.Fatalf("lock received command: %v", err)
	}
	if _, err := harness.pool.Exec(context.Background(), `
UPDATE session_adapter_connections
SET active_credential_expires_at = clock_timestamp() + interval '250 milliseconds'
`); err != nil {
		t.Fatalf("shorten resolve authority: %v", err)
	}
	tracer := newQueryStartSignal("LockPendingCommandForResolve")
	harness.pool.Close()
	harness.reopenWithTracer(t, tracer)
	result := make(chan error, 1)
	go func() {
		_, resolveErr := harness.ResolvePendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID, store.PendingCommandCompleted)
		result <- resolveErr
	}()
	<-tracer.started
	time.Sleep(300 * time.Millisecond)
	if err := blockerTx.Commit(context.Background()); err != nil {
		t.Fatalf("release received command: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("resolve succeeded after authority expired during row wait")
	}
	assertPendingCommandStatus(t, harness, "ses_command_claim", request.CommandID, store.PendingCommandReceived)
}

func TestReplayStopsBeforeFetchingPastFirstCallbackError(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_store_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	tracer := &replayQueryTracer{}
	pool := openPool(t, dsn, schemaName, tracer)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	events := make([]store.PendingEvent, 64)
	for index := range events {
		events[index] = store.PendingEvent{Type: "session.message", Time: time.Unix(int64(index), 0), Payload: []byte(`{"n":1}`)}
	}
	postgresStore := postgres.New(pool)
	if _, err := postgresStore.Append(context.Background(), "ses_replay_early_stop", events); err != nil {
		t.Fatalf("append replay events: %v", err)
	}
	tracer.reset()
	callbackErr := errors.New("stop replay")
	calls := 0
	err := postgresStore.Replay(context.Background(), "ses_replay_early_stop", 0, func(store.Event) error {
		calls++
		return callbackErr
	})
	if !errors.Is(err, callbackErr) || calls != 1 {
		t.Fatalf("replay callback result = %v after %d calls", err, calls)
	}
	if tracer.eventQueries.Load() != 1 || !tracer.sawSingleRowLimit.Load() {
		t.Fatalf("first callback error fetched %d event queries with single-row=%t", tracer.eventQueries.Load(), tracer.sawSingleRowLimit.Load())
	}
}

func TestReplayUsesInitialSnapshotAcrossCallbackAppend(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_store_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })

	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)
	postgresStore := postgres.New(pool)
	if _, err := postgresStore.Append(context.Background(), "ses_replay_snapshot", []store.PendingEvent{{Type: "session.message", Time: time.Unix(1, 0), Payload: []byte(`{"n":1}`)}}); err != nil {
		t.Fatalf("append initial event: %v", err)
	}
	var replayed []int64
	err := postgresStore.Replay(context.Background(), "ses_replay_snapshot", 0, func(event store.Event) error {
		replayed = append(replayed, event.Seq)
		if event.Seq == 1 {
			_, err := postgresStore.Append(context.Background(), "ses_replay_snapshot", []store.PendingEvent{{Type: "session.message", Time: time.Unix(2, 0), Payload: []byte(`{"n":2}`)}})
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay initial snapshot: %v", err)
	}
	if len(replayed) != 1 || replayed[0] != 1 {
		t.Fatalf("replay crossed initial snapshot: %v", replayed)
	}
	if latest, err := postgresStore.LatestSeq(context.Background(), "ses_replay_snapshot"); err != nil || latest != 2 {
		t.Fatalf("callback append latest seq = %d, %v", latest, err)
	}
}

func testDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("AGENTWHARF_POSTGRES_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("SUPERWHV_TEST_DATABASE_URL")
	}
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set AGENTWHARF_POSTGRES_TEST_DATABASE_URL, SUPERWHV_TEST_DATABASE_URL, or DATABASE_URL to run PostgreSQL store tests")
	}
	return dsn
}

func setupSchema(t *testing.T, dsn string, schemaName string) {
	t.Helper()

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect postgres for schema setup: %v", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close schema setup connection: %v", err)
		}
	}()
	if _, err := conn.Exec(context.Background(), "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
}

func dropSchema(t *testing.T, dsn string, schemaName string) {
	t.Helper()

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Errorf("connect postgres for schema cleanup: %v", err)
		return
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close schema cleanup connection: %v", err)
		}
	}()
	if _, err := conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE"); err != nil {
		t.Errorf("drop schema: %v", err)
	}
}

func openPool(t *testing.T, dsn string, schemaName string, tracer pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres config: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName
	config.ConnConfig.Tracer = tracer

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatalf("ping postgres: %v", err)
	}
	return pool
}

type replayQueryTracer struct {
	eventQueries      atomic.Int64
	sawSingleRowLimit atomic.Bool
}

type historySnapshotTracer struct {
	pageQuery chan struct{}
	resume    chan struct{}
	once      sync.Once
}

type queryStartSignal struct {
	needle  string
	started chan struct{}
	once    sync.Once
}

func newQueryStartSignal(needle string) *queryStartSignal {
	return &queryStartSignal{needle: needle, started: make(chan struct{})}
}

func (tracer *queryStartSignal) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, tracer.needle) {
		tracer.once.Do(func() { close(tracer.started) })
	}
	return ctx
}

func (*queryStartSignal) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func newHistorySnapshotTracer() *historySnapshotTracer {
	return &historySnapshotTracer{pageQuery: make(chan struct{}), resume: make(chan struct{})}
}

func (tracer *historySnapshotTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "ORDER BY seq DESC") {
		tracer.once.Do(func() { close(tracer.pageQuery) })
		<-tracer.resume
	}
	return ctx
}

func (*historySnapshotTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *replayQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "FROM session_events") {
		tracer.eventQueries.Add(1)
		tracer.sawSingleRowLimit.Store(strings.Contains(data.SQL, "LIMIT 1"))
	}
	return ctx
}

func (tracer *replayQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *replayQueryTracer) reset() {
	tracer.eventQueries.Store(0)
	tracer.sawSingleRowLimit.Store(false)
}

func resetSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	fixture, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read sqlc schema fixture: %v", err)
	}
	if _, err := pool.Exec(context.Background(), string(fixture)); err != nil {
		t.Fatalf("reset session_events schema: %v", err)
	}
}

func commandAdvisoryLockKey(sessionID string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(sessionID))
	return int64(binary.BigEndian.Uint64(hash.Sum(nil)))
}

func assertPendingCommandRollback(t *testing.T, harness *postgresCommandHarness, sessionID string) {
	t.Helper()
	if latest, err := harness.LatestSeq(context.Background(), sessionID); err != nil || latest != 0 {
		t.Fatalf("rejected command latest seq = %d, %v", latest, err)
	}
	var count int
	if err := harness.pool.QueryRow(context.Background(), `
SELECT COUNT(*) FROM session_pending_commands WHERE session_id = $1
`, sessionID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected command rows = %d, %v", count, err)
	}
}

func assertPendingCommandStatus(t *testing.T, harness *postgresCommandHarness, sessionID, commandID string, want store.PendingCommandStatus) {
	t.Helper()
	var got string
	if err := harness.pool.QueryRow(context.Background(), `
SELECT status FROM session_pending_commands WHERE session_id = $1 AND cmd_id = $2
`, sessionID, commandID).Scan(&got); err != nil {
		t.Fatalf("read pending command status: %v", err)
	}
	if store.PendingCommandStatus(got) != want {
		t.Fatalf("pending command status = %s, want %s", got, want)
	}
}
