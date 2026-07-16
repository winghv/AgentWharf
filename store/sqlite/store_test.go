package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
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
