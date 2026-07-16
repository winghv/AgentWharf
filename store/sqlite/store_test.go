package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
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
