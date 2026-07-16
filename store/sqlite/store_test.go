package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
	"github.com/winghv/agentwharf/store/storetest"
)

func TestEventStoreContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T) store.EventStore {
		return openStore(t, filepath.Join(t.TempDir(), "events.db"))
	})
}

func TestHistoryStoreContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	storetest.HistoryContract(t, storetest.HistoryHarness{
		Open: func(t *testing.T) store.HistoryStore {
			return openStore(t, path)
		},
		Reopen: func(t *testing.T, current store.HistoryStore) store.HistoryStore {
			t.Helper()
			if err := current.(*sqlite.Store).Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			return openStore(t, path)
		},
		PruneBefore: func(t *testing.T, _ store.HistoryStore, sessionID string, beforeSeq int64) {
			t.Helper()
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatalf("open prune database: %v", err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Fatalf("close prune database: %v", err)
				}
			})
			if _, err := db.ExecContext(context.Background(), `
				DELETE FROM session_events WHERE session_id = ? AND seq < ?
			`, sessionID, beforeSeq); err != nil {
				t.Fatalf("prune history: %v", err)
			}
		},
	})
}

func TestPendingCommandStoreContract(t *testing.T) {
	storetest.PendingCommandContract(t, storetest.PendingCommandHarness{
		Open: func(t *testing.T) store.CommandLedgerStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			harness := &sqliteCommandHarness{Store: openStore(t, path), path: path}
			seedCommandAuthorities(t, path)
			return harness
		},
		Reopen: func(t *testing.T, current store.CommandLedgerStore) store.CommandLedgerStore {
			t.Helper()
			harness := current.(*sqliteCommandHarness)
			if err := harness.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			harness.Store = openStore(t, harness.path)
			return harness
		},
		Authority: func(t *testing.T, _ store.CommandLedgerStore) store.CommandAuthority {
			t.Helper()
			return store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
		},
		Invalidate: invalidateCommandAuthority,
	})
}

func TestProposalStoreContract(t *testing.T) {
	storetest.ProposalContract(t, storetest.ProposalHarness{
		Open: func(t *testing.T) store.ProposedEventStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			harness := &sqliteProposalHarness{Store: openStore(t, path), path: path}
			seedProposalAuthorities(t, path)
			return harness
		},
		Reopen: func(t *testing.T, current store.ProposedEventStore) store.ProposedEventStore {
			t.Helper()
			harness := current.(*sqliteProposalHarness)
			if err := harness.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			harness.Store = openStore(t, harness.path)
			return harness
		},
		Authority: func(t *testing.T, _ store.ProposedEventStore) store.CommandAuthority {
			t.Helper()
			return store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
		},
		Invalidate: invalidateProposalAuthority,
	})
}

func TestConnectionStoreContract(t *testing.T) {
	storetest.ConnectionContract(t, storetest.ConnectionHarness{
		Open: func(t *testing.T) store.AdapterConnectionStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			return &sqliteConnectionHarness{Store: openStore(t, path), path: path}
		},
		Invalidate: invalidateAdapterConnection,
	})
}

func TestProposalLedgerStoresReferencesOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	marker := "proposal-secret-marker"
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_corrupt",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.ProposedEventRequest{
			ProposalID: "proposal_reference_only",
			Event:      store.PendingEvent{Type: "session.state", Time: testTime(1), Payload: []byte(fmt.Sprintf(`{"marker":%q}`, marker))},
		}); err != nil {
		t.Fatalf("CommitProposedEvent() error = %v", err)
	}
	db := openRawSQLite(t, path)
	rows, err := db.QueryContext(context.Background(), `PRAGMA table_info(session_event_proposals)`)
	if err != nil {
		t.Fatalf("read proposal columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan proposal column: %v", err)
		}
		for _, forbidden := range []string{"payload", "content", "secret", "token", "credential", "provider"} {
			if strings.Contains(strings.ToLower(name), forbidden) {
				t.Fatalf("proposal column %q contains forbidden concept %q", name, forbidden)
			}
		}
	}
	var values string
	if err := db.QueryRowContext(context.Background(), `
SELECT session_id || '|' || proposal_id || '|' || event_seq FROM session_event_proposals WHERE proposal_id = ?
`, "proposal_reference_only").Scan(&values); err != nil {
		t.Fatalf("read proposal reference: %v", err)
	}
	if strings.Contains(values, marker) {
		t.Fatalf("proposal reference copied event content: %q", values)
	}
}

func TestProposalCorruptionFailsClosedWithoutMaterializingOversizedEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.ProposedEventRequest{ProposalID: "proposal_corrupt", Event: store.PendingEvent{
		Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`),
	}}
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_corrupt", authority, request); err != nil {
		t.Fatalf("CommitProposedEvent() error = %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_events SET payload = zeroblob(8 * 1024 * 1024) WHERE session_id = ? AND seq = 1
`, "ses_proposal_corrupt"); err != nil {
		t.Fatalf("corrupt proposal event payload: %v", err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_corrupt", authority, request); err == nil {
		t.Fatal("duplicate CommitProposedEvent() accepted oversized referenced payload")
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2*1024*1024 {
		t.Fatalf("oversized corrupt proposal allocated %d bytes, want <= 2 MiB", allocated)
	}
}

func TestProposalQueuedBehindAuthorityChangeWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin proposal authority change: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = ?
`, time.Now().UnixMilli(), "ses_proposal_queued"); err != nil {
		t.Fatalf("stage proposal authority change: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := proposals.CommitProposedEvent(ctx, "ses_proposal_queued",
			store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.ProposedEventRequest{
				ProposalID: "proposal_queued", Event: store.PendingEvent{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)},
			})
		result <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := db.ExecContext(context.Background(), `COMMIT`); err != nil {
		t.Fatalf("commit proposal authority change: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("queued stale CommitProposedEvent() unexpectedly succeeded")
	}
	if latest, err := proposals.LatestSeq(context.Background(), "ses_proposal_queued"); err != nil || latest != 0 {
		t.Fatalf("queued stale proposal latest seq = %d, %v; want 0, nil", latest, err)
	}
	var receipts int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM session_event_proposals WHERE session_id = ?`, "ses_proposal_queued").Scan(&receipts); err != nil {
		t.Fatalf("count queued proposal receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("queued stale proposal receipts = %d, want 0", receipts)
	}
}

func TestProposalDuplicateQueuedBehindAuthorityChangeReturnsNoReceipt(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		args      func(now int64, sessionID string) []any
	}{
		{name: "epoch", statement: `UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, sessionID} }},
		{name: "generation", statement: `UPDATE session_adapter_connections SET active_credential_generation = 2, credential_generation_high_watermark = 2, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, sessionID} }},
		{name: "revoked", statement: `UPDATE session_adapter_connections SET revoked_at_ms = ?, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, now, sessionID} }},
		{name: "expired", statement: `UPDATE session_adapter_connections SET created_at_ms = ?, active_credential_expires_at_ms = ?, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any {
			return []any{now - int64((2*time.Minute)/time.Millisecond), now - 1, now, sessionID}
		}},
		{name: "terminal", statement: `UPDATE session_adapter_connections SET terminal_at_ms = ?, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, now, sessionID} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
			seedProposalAuthorities(t, path)
			sessionID := "ses_proposal_duplicate_" + test.name
			authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
			request := store.ProposedEventRequest{ProposalID: "proposal_duplicate_authority", Event: store.PendingEvent{
				Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`),
			}}
			if _, err := proposals.CommitProposedEvent(context.Background(), sessionID, authority, request); err != nil {
				t.Fatalf("initial CommitProposedEvent() error = %v", err)
			}
			db := openRawSQLite(t, path)
			if _, err := db.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
				t.Fatalf("begin duplicate authority change: %v", err)
			}
			now := time.Now().UnixMilli()
			if _, err := db.ExecContext(context.Background(), test.statement, test.args(now, sessionID)...); err != nil {
				t.Fatalf("stage duplicate authority change: %v", err)
			}
			result := make(chan error, 1)
			go func() {
				_, err := proposals.CommitProposedEvent(context.Background(), sessionID, authority, request)
				result <- err
			}()
			var retryErr error
			received := false
			select {
			case retryErr = <-result:
				received = true
				if retryErr == nil {
					t.Fatal("duplicate proposal returned accepted receipt while authority change was pending")
				}
			case <-time.After(50 * time.Millisecond):
			}
			if _, err := db.ExecContext(context.Background(), `COMMIT`); err != nil {
				t.Fatalf("commit duplicate authority change: %v", err)
			}
			if !received {
				retryErr = <-result
			}
			if retryErr == nil {
				t.Fatal("duplicate proposal returned accepted receipt after authority change")
			}
			events := replayProposalEvents(t, proposals, sessionID)
			if len(events) != 1 || events[0].Seq != 1 {
				t.Fatalf("duplicate authority race changed durable events: %+v", events)
			}
		})
	}
}

func TestProposalReceiptFailureRollsBackEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `
CREATE TRIGGER fail_proposal_receipt BEFORE INSERT ON session_event_proposals
BEGIN SELECT RAISE(ABORT, 'proposal receipt failpoint'); END
`); err != nil {
		t.Fatalf("create proposal receipt failpoint: %v", err)
	}
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_rollback",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.ProposedEventRequest{
			ProposalID: "proposal_rollback", Event: store.PendingEvent{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)},
		}); err == nil {
		t.Fatal("CommitProposedEvent() unexpectedly survived receipt failpoint")
	}
	if latest, err := proposals.LatestSeq(context.Background(), "ses_proposal_rollback"); err != nil || latest != 0 {
		t.Fatalf("proposal receipt rollback latest seq = %d, %v; want 0, nil", latest, err)
	}
}

func replayProposalEvents(t *testing.T, proposals store.ProposedEventStore, sessionID string) []store.Event {
	t.Helper()
	var events []store.Event
	if err := proposals.Replay(context.Background(), sessionID, 0, func(event store.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	return events
}

func TestAttachmentStoreContract(t *testing.T) {
	storetest.AttachmentContract(t, storetest.AttachmentHarness{
		Open: func(t *testing.T) store.AttachmentStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			return &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
		},
		Reopen: func(t *testing.T, current store.AttachmentStore) store.AttachmentStore {
			t.Helper()
			harness := current.(*sqliteAttachmentHarness)
			if err := harness.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			harness.Store = openStore(t, harness.path)
			return harness
		},
	})
}

func TestAttachmentCorruptionFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		args      func() []any
	}{
		{
			name:      "future creation window",
			statement: `UPDATE session_attachments SET created_at_ms = ?, updated_at_ms = ?, expires_at_ns = ? WHERE attach_id = ?`,
			args: func() []any {
				created := time.Now().Add(time.Minute).UnixMilli()
				return []any{created, created, (created + 10000) * int64(time.Millisecond), "attach_corrupt"}
			},
		},
		{
			name:      "oversized queue reason",
			statement: `UPDATE session_attachments SET status = 'queued', queue_reason = ?, blocking_session_id = ? WHERE attach_id = ?`,
			args: func() []any {
				return []any{strings.Repeat("x", 129), "ses_blocker", "attach_corrupt"}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
			request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
				AttachID: "attach_corrupt", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
				TargetCredentialLineageRef: "lineage_attach_corrupt",
			}, ExpiresAt: time.Now().Add(20 * time.Second)}
			if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
				t.Fatalf("CreateAttachment() error = %v", err)
			}
			db := openRawSQLite(t, path)
			if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
				t.Fatalf("enable corruption fixture: %v", err)
			}
			if _, err := db.ExecContext(context.Background(), test.statement, test.args()...); err != nil {
				t.Fatalf("corrupt attachment row: %v", err)
			}
			if _, err := ledger.Attachment(context.Background(), request.Identity.AttachID); err == nil {
				t.Fatal("Attachment() accepted corrupt row")
			}
		})
	}
}

func TestAttachmentConflictAndCancellationLeaveNoPartialState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	first := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_first", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_first",
	}, ExpiresAt: time.Now().Add(20 * time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), first); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	conflict := first
	conflict.Identity.AttachID = "attach_conflict"
	conflict.Identity.BootstrapSessionID = "ses_other_bootstrap"
	conflict.Identity.TargetCredentialLineageRef = "lineage_conflict"
	if _, err := ledger.CreateAttachment(context.Background(), conflict); err == nil {
		t.Fatal("target-conflicting CreateAttachment() unexpectedly succeeded")
	}
	if _, err := ledger.Attachment(context.Background(), conflict.Identity.AttachID); err == nil {
		t.Fatal("target-conflicting attachment left partial state")
	}
	cancel := store.AttachmentUpdate{Status: store.AttachmentCanceled, DeliveryState: store.AttachmentDeliveryPending,
		Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerNewRunRequired}}
	if _, err := ledger.UpdateAttachment(context.Background(), first.Identity.AttachID, 0, cancel); err != nil {
		t.Fatalf("cancel UpdateAttachment() error = %v", err)
	}
	if _, err := ledger.UpdateAttachment(context.Background(), first.Identity.AttachID, 1,
		store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived}); err == nil {
		t.Fatal("canceled attachment recorded start receipt")
	}
}

func TestAttachmentStartReceiptRechecksExpiryAtWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_expiry_race", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_expiry_race",
	}, ExpiresAt: time.Now().Add(time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}

	db := openRawSQLite(t, path)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open lock connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(context.Background(), `PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("set lock timeout: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("hold attachment write lock: %v", err)
	}

	updated := make(chan error, 1)
	go func() {
		_, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0,
			store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived})
		updated <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if delay := time.Until(request.ExpiresAt) + 50*time.Millisecond; delay > 0 {
		time.Sleep(delay)
	}
	if _, err := conn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatalf("release attachment write lock: %v", err)
	}
	if err := <-updated; err == nil || !strings.Contains(err.Error(), "attachment version conflict") {
		t.Fatalf("expired write-time start receipt error = %v, want attachment version conflict", err)
	}

	attachment, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
	if err != nil {
		t.Fatalf("Attachment() error = %v", err)
	}
	if attachment.Status != store.AttachmentJoinPending || attachment.DeliveryVersion != 0 ||
		attachment.ExpiresAt == nil || !attachment.ExpiresAt.Equal(request.ExpiresAt) {
		t.Fatalf("expired start receipt changed durable truth: %+v", attachment)
	}
}

func TestAttachmentUpdateBusyRetryIsBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_busy_deadline", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_busy_deadline",
	}, ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}

	db := openRawSQLite(t, path)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open lock connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("hold attachment write lock: %v", err)
	}
	started := time.Now()
	_, updateErr := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0,
		store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived})
	elapsed := time.Since(started)
	if _, err := conn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatalf("release attachment write lock: %v", err)
	}
	if !errors.Is(updateErr, context.DeadlineExceeded) {
		t.Fatalf("busy attachment update error = %v, want deadline exceeded", updateErr)
	}
	if elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("busy attachment update elapsed = %v, want bounded near two seconds", elapsed)
	}

	attachment, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
	if err != nil {
		t.Fatalf("Attachment() error = %v", err)
	}
	if attachment.Status != store.AttachmentJoinPending || attachment.DeliveryVersion != 0 {
		t.Fatalf("timed-out attachment update changed durable truth: %+v", attachment)
	}
}

func TestAttachmentSummaryDoesNotAliasCallerOrAttachment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_alias", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_alias",
	}, ExpiresAt: time.Now().Add(20 * time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	reason := "capacity"
	blockingSessionID := "ses_blocker"
	expiresAt := request.ExpiresAt
	update := store.AttachmentUpdate{Status: store.AttachmentQueued, DeliveryState: store.AttachmentDeliveryPending,
		QueueReason: &reason, ExpiresAt: &expiresAt, BlockingSessionID: &blockingSessionID,
		Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerQueued, Reason: &reason,
			ExpiresAt: &expiresAt, BlockingSessionID: &blockingSessionID}}
	mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update)
	if err != nil {
		t.Fatalf("UpdateAttachment() error = %v", err)
	}
	wantReason, wantBlocker, wantExpiry := reason, blockingSessionID, expiresAt
	reason = "rewritten"
	blockingSessionID = "ses_rewritten"
	expiresAt = expiresAt.Add(-time.Second)
	*mutation.Attachment.ExpiresAt = mutation.Attachment.ExpiresAt.Add(-time.Second)
	if mutation.Summary.Blocker == nil || mutation.Summary.Blocker.Reason == nil || *mutation.Summary.Blocker.Reason != wantReason ||
		mutation.Summary.Blocker.BlockingSessionID == nil || *mutation.Summary.Blocker.BlockingSessionID != wantBlocker ||
		mutation.Summary.ExpiresAt == nil || !mutation.Summary.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("summary changed through pointer alias: %+v", mutation.Summary)
	}
	stored, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
	if err != nil || stored.QueueReason == nil || *stored.QueueReason != wantReason || stored.BlockingSessionID == nil ||
		*stored.BlockingSessionID != wantBlocker || stored.ExpiresAt == nil || !stored.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("committed attachment changed through pointer alias: %+v, %v", stored, err)
	}
}

func TestPendingCommandLedgerStoresReferencesOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	marker := "ledger-secret-marker"
	request := store.PendingCommandRequest{CommandID: "cmd_reference_only", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.PendingEvent{
		Type: "session.message", Time: testTime(1), Payload: []byte(fmt.Sprintf(`{"role":"user","content":[{"text":%q}]}`, marker)),
	}, request); err != nil {
		t.Fatalf("CommitPendingCommand() error = %v", err)
	}

	db := openRawSQLite(t, path)
	rows, err := db.QueryContext(context.Background(), `PRAGMA table_info(session_pending_commands)`)
	if err != nil {
		t.Fatalf("read pending-command columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan pending-command column: %v", err)
		}
		lower := strings.ToLower(name)
		for _, forbidden := range []string{"payload", "content", "secret", "token", "credential", "provider"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("pending-command column %q contains forbidden concept %q", name, forbidden)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pending-command columns: %v", err)
	}

	var values string
	if err := db.QueryRowContext(context.Background(), `
SELECT session_id || '|' || cmd_id || '|' || type || '|' || event_seq || '|' || status || '|' || expires_at_ns
FROM session_pending_commands WHERE session_id = ? AND cmd_id = ?
`, "ses_command_1", request.CommandID).Scan(&values); err != nil {
		t.Fatalf("read pending-command values: %v", err)
	}
	if strings.Contains(values, marker) {
		t.Fatalf("pending-command row copied event content: %q", values)
	}
}

func TestPendingCommandCorruptStatusFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_corrupt", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`),
	}, request); err != nil {
		t.Fatalf("CommitPendingCommand() error = %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("enable corruption fixture: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_pending_commands SET status = 'corrupt' WHERE cmd_id = ?`, request.CommandID); err != nil {
		t.Fatalf("corrupt pending-command status: %v", err)
	}
	if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_1", authority, request.CommandID); err == nil {
		t.Fatal("ClaimPendingCommand() accepted corrupt status")
	}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(2), Payload: []byte(`{"role":"user"}`),
	}, request); err == nil {
		t.Fatal("duplicate CommitPendingCommand() returned corrupt ledger truth")
	}
}

func TestPendingCommandCorruptReferenceFailsClosed(t *testing.T) {
	futureCreatedAt := time.Now().Add(time.Minute).UnixMilli()
	for _, test := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name:      "missing event",
			statement: `DELETE FROM session_events WHERE session_id = ? AND seq = 1`,
			args:      []any{"ses_command_1"},
		},
		{
			name:      "wrong event type",
			statement: `UPDATE session_events SET type = 'session.state' WHERE session_id = ? AND seq = 1`,
			args:      []any{"ses_command_1"},
		},
		{
			name:      "non-user event",
			statement: `UPDATE session_events SET payload = ? WHERE session_id = ? AND seq = 1`,
			args:      []any{[]byte(`{"role":"agent"}`), "ses_command_1"},
		},
		{
			name:      "unbounded expiry",
			statement: `UPDATE session_pending_commands SET expires_at_ns = (created_at_ms + 31000) * 1000000 WHERE session_id = ? AND cmd_id = ?`,
			args:      []any{"ses_command_1", "cmd_corrupt_reference"},
		},
		{
			name:      "future expiry window",
			statement: `UPDATE session_pending_commands SET created_at_ms = ?, expires_at_ns = ? WHERE session_id = ? AND cmd_id = ?`,
			args:      []any{futureCreatedAt, (futureCreatedAt + 10000) * int64(time.Millisecond), "ses_command_1", "cmd_corrupt_reference"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
			seedCommandAuthorities(t, path)
			authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
			request := store.PendingCommandRequest{CommandID: "cmd_corrupt_reference", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
			event := store.PendingEvent{Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`)}
			if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err != nil {
				t.Fatalf("CommitPendingCommand() error = %v", err)
			}
			db := openRawSQLite(t, path)
			if _, err := db.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
				t.Fatalf("disable foreign keys for corruption fixture: %v", err)
			}
			if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
				t.Fatalf("enable corruption fixture: %v", err)
			}
			if _, err := db.ExecContext(context.Background(), test.statement, test.args...); err != nil {
				t.Fatalf("corrupt pending-command reference: %v", err)
			}
			if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err == nil {
				t.Fatal("duplicate CommitPendingCommand() returned corrupt reference truth")
			}
			if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_1", authority, request.CommandID); err == nil {
				t.Fatal("ClaimPendingCommand() accepted corrupt reference truth")
			}
		})
	}
}

func TestPendingCommandPayloadBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_payload_exact", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(1), Payload: userPayloadBytes(t, 64*1024),
	}, request); err != nil {
		t.Fatalf("exact-bound CommitPendingCommand() error = %v", err)
	}
	request.CommandID = "cmd_payload_oversized"
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(2), Payload: userPayloadBytes(t, 64*1024+1),
	}, request); err == nil {
		t.Fatal("oversized CommitPendingCommand() unexpectedly succeeded")
	}

	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events SET payload = ? WHERE session_id = ? AND seq = 1`, userPayloadBytes(t, 64*1024+1), "ses_command_1"); err != nil {
		t.Fatalf("corrupt event payload size: %v", err)
	}
	request.CommandID = "cmd_payload_exact"
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(3), Payload: []byte(`{"role":"user"}`),
	}, request); err == nil {
		t.Fatal("duplicate CommitPendingCommand() accepted oversized referenced payload")
	}
}

func TestPendingCommandOversizedCorruptReferenceIsNotMaterialized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_payload_materialization", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	event := store.PendingEvent{Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err != nil {
		t.Fatalf("CommitPendingCommand() error = %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events SET payload = zeroblob(8 * 1024 * 1024) WHERE session_id = ? AND seq = 1`, "ses_command_1"); err != nil {
		t.Fatalf("corrupt event payload size: %v", err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err == nil {
		t.Fatal("duplicate CommitPendingCommand() accepted oversized referenced payload")
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2*1024*1024 {
		t.Fatalf("oversized corrupt reference allocated %d bytes, want <= 2 MiB", allocated)
	}
}

func TestPendingCommandQueuedBehindAuthorityChangeWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin authority change: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = ?
`, time.Now().UnixMilli(), "ses_command_1"); err != nil {
		t.Fatalf("stage authority change: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := ledger.CommitPendingCommand(ctx, "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.PendingEvent{
			Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`),
		}, store.PendingCommandRequest{CommandID: "cmd_queued_stale", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)})
		result <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := db.ExecContext(context.Background(), `COMMIT`); err != nil {
		t.Fatalf("commit authority change: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("queued stale CommitPendingCommand() unexpectedly succeeded")
	}
	latest, err := ledger.LatestSeq(context.Background(), "ses_command_1")
	if err != nil || latest != 0 {
		t.Fatalf("queued stale latest seq = %d, %v; want 0, nil", latest, err)
	}
	var commands int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM session_pending_commands WHERE session_id = ?`, "ses_command_1").Scan(&commands); err != nil {
		t.Fatalf("count queued stale commands: %v", err)
	}
	if commands != 0 {
		t.Fatalf("queued stale pending commands = %d, want 0", commands)
	}
}

func TestClosedStoreRejectsOperations(t *testing.T) {
	st, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := st.Append(context.Background(), "ses_closed", []store.PendingEvent{{Type: "session.message", Time: testTime(1), Payload: []byte(`{}`)}}); err == nil {
		t.Fatal("Append() on closed store unexpectedly succeeded")
	}
	if err := st.Replay(context.Background(), "ses_closed", 0, func(store.Event) error { return nil }); err == nil {
		t.Fatal("Replay() on closed store unexpectedly succeeded")
	}
	if _, err := st.LatestSeq(context.Background(), "ses_closed"); err == nil {
		t.Fatal("LatestSeq() on closed store unexpectedly succeeded")
	}
	if _, err := st.History(context.Background(), "ses_closed", nil, 1); err == nil {
		t.Fatal("History() on closed store unexpectedly succeeded")
	}
}

func TestReplayRejectsNilCallback(t *testing.T) {
	st := openStore(t, filepath.Join(t.TempDir(), "events.db"))
	if err := st.Replay(context.Background(), "ses_nil_callback", 0, nil); err == nil {
		t.Fatal("Replay(nil) unexpectedly succeeded")
	}
}

func TestHistoryReportsInternalRetentionGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	st := openStore(t, path)
	if _, err := st.Append(context.Background(), "ses_history_internal_gap", []store.PendingEvent{
		{Type: "session.message", Time: testTime(1), Payload: []byte(`{"n":1}`)},
		{Type: "session.message", Time: testTime(2), Payload: []byte(`{"n":2}`)},
		{Type: "session.message", Time: testTime(3), Payload: []byte(`{"n":3}`)},
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open history database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(context.Background(), `
		DELETE FROM session_events WHERE session_id = ? AND seq = ?
	`, "ses_history_internal_gap", 2); err != nil {
		t.Fatalf("delete internal history event: %v", err)
	}

	page, err := st.History(context.Background(), "ses_history_internal_gap", nil, 100)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if page.RetentionState != store.RetentionGap || page.LatestSeq != 3 || len(page.Events) != 2 || page.Events[0].Seq != 1 || page.Events[1].Seq != 3 {
		t.Fatalf("History() = %+v, want internal retention gap", page)
	}
}

func testTime(sequence int64) time.Time {
	return time.UnixMilli(1764937200000 + sequence)
}

func openStore(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	st, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return st
}

type sqliteCommandHarness struct {
	*sqlite.Store
	path string
}

type sqliteProposalHarness struct {
	*sqlite.Store
	path string
}

type sqliteConnectionHarness struct {
	*sqlite.Store
	path string
}

type sqliteAttachmentHarness struct {
	*sqlite.Store
	path string
}

func invalidateAdapterConnection(t *testing.T, current store.AdapterConnectionStore, terminal bool) {
	t.Helper()
	harness := current.(*sqliteConnectionHarness)
	db := openRawSQLite(t, harness.path)
	now := time.Now().UnixMilli()
	column := "revoked_at_ms"
	if terminal {
		column = "terminal_at_ms"
	}
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_adapter_connections SET `+column+` = ?, updated_at_ms = ? WHERE session_id = 'ses_connection'
`, now, now); err != nil {
		t.Fatalf("invalidate adapter connection terminal=%v: %v", terminal, err)
	}
}

func seedProposalAuthorities(t *testing.T, path string) {
	t.Helper()
	db := openRawSQLite(t, path)
	now := time.Now().UnixMilli()
	for _, sessionID := range []string{
		"ses_proposal_1", "ses_proposal_conflict", "ses_proposal_snapshot", "ses_proposal_stale", "ses_proposal_reopen",
		"ses_proposal_corrupt", "ses_proposal_queued", "ses_proposal_rollback",
		"ses_proposal_duplicate_epoch", "ses_proposal_duplicate_generation", "ses_proposal_duplicate_revoked",
		"ses_proposal_duplicate_expired", "ses_proposal_duplicate_terminal",
	} {
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at_ms, created_at_ms, updated_at_ms
) VALUES (?, 1, 1, 1, 1, ?, ?, ?)
`, sessionID, now+int64(time.Hour/time.Millisecond), now, now); err != nil {
			t.Fatalf("seed proposal authority for %s: %v", sessionID, err)
		}
	}
}

func invalidateProposalAuthority(t *testing.T, current store.ProposedEventStore, kind storetest.CommandAuthorityFailure) {
	t.Helper()
	harness := current.(*sqliteProposalHarness)
	db := openRawSQLite(t, harness.path)
	now := time.Now().UnixMilli()
	var statement string
	var args []any
	switch kind {
	case storetest.CommandAuthoritySuperseded:
		statement = `UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now}
	case storetest.CommandAuthorityRevoked:
		statement = `UPDATE session_adapter_connections SET revoked_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now, now}
	case storetest.CommandAuthorityExpired:
		statement = `UPDATE session_adapter_connections SET created_at_ms = ?, active_credential_expires_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now - int64((2*time.Minute)/time.Millisecond), now - 1, now}
	case storetest.CommandAuthorityTerminal:
		statement = `UPDATE session_adapter_connections SET terminal_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now, now}
	default:
		t.Fatalf("unknown proposal authority failure %q", kind)
	}
	if _, err := db.ExecContext(context.Background(), statement, args...); err != nil {
		t.Fatalf("invalidate proposal authority %s: %v", kind, err)
	}
}

func seedCommandAuthorities(t *testing.T, path string) {
	t.Helper()
	db := openRawSQLite(t, path)
	now := time.Now().UnixMilli()
	for _, sessionID := range []string{
		"ses_command_1", "ses_command_claim", "ses_command_stale",
		"ses_command_expired", "ses_command_reopen", "ses_command_invalid",
	} {
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at_ms, created_at_ms, updated_at_ms
) VALUES (?, 1, 1, 1, 1, ?, ?, ?)
`, sessionID, now+int64(time.Hour/time.Millisecond), now, now); err != nil {
			t.Fatalf("seed command authority for %s: %v", sessionID, err)
		}
	}
}

func invalidateCommandAuthority(t *testing.T, current store.CommandLedgerStore, kind storetest.CommandAuthorityFailure) {
	t.Helper()
	harness := current.(*sqliteCommandHarness)
	db := openRawSQLite(t, harness.path)
	now := time.Now().UnixMilli()
	var statement string
	var args []any
	switch kind {
	case storetest.CommandAuthoritySuperseded:
		statement = `UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ?`
		args = []any{now}
	case storetest.CommandAuthorityRevoked:
		statement = `UPDATE session_adapter_connections SET revoked_at_ms = ?, updated_at_ms = ?`
		args = []any{now, now}
	case storetest.CommandAuthorityExpired:
		statement = `UPDATE session_adapter_connections SET created_at_ms = ?, active_credential_expires_at_ms = ?, updated_at_ms = ?`
		args = []any{now - int64((2*time.Minute)/time.Millisecond), now - 1, now}
	case storetest.CommandAuthorityTerminal:
		statement = `UPDATE session_adapter_connections SET terminal_at_ms = ?, updated_at_ms = ?`
		args = []any{now, now}
	default:
		t.Fatalf("unknown command authority failure %q", kind)
	}
	if _, err := db.ExecContext(context.Background(), statement, args...); err != nil {
		t.Fatalf("invalidate command authority %s: %v", kind, err)
	}
}

func openRawSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw sqlite database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func userPayloadBytes(t *testing.T, size int) []byte {
	t.Helper()
	prefix := []byte(`{"role":"user","padding":"`)
	suffix := []byte(`"}`)
	if size < len(prefix)+len(suffix) {
		t.Fatalf("payload size %d is too small", size)
	}
	payload := make([]byte, size)
	copy(payload, prefix)
	for index := len(prefix); index < size-len(suffix); index++ {
		payload[index] = 'x'
	}
	copy(payload[size-len(suffix):], suffix)
	return payload
}
