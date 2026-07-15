package postgres_test

import (
	"context"
	"errors"
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
	"github.com/winghv/agentwharf/store/storetest"
)

var schemaSeq atomic.Uint64

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
			dsn := testDSN(t)
			schemaName := fmt.Sprintf("agentwharf_command_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
			setupSchema(t, dsn, schemaName)
			harness := &postgresCommandHarness{dsn: dsn, schemaName: schemaName}
			harness.reopen(t)
			resetSchema(t, harness.pool)
			harness.seedAuthority(t)
			t.Cleanup(func() {
				harness.pool.Close()
				dropSchema(t, dsn, schemaName)
			})
			return harness
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

type postgresCommandHarness struct {
	*postgres.Store
	pool       *pgxpool.Pool
	dsn        string
	schemaName string
}

func (h *postgresCommandHarness) reopen(t *testing.T) {
	t.Helper()
	h.pool = openPool(t, h.dsn, h.schemaName, nil)
	h.Store = postgres.New(h.pool)
}

func (h *postgresCommandHarness) seedAuthority(t *testing.T) {
	t.Helper()
	sessionIDs := []string{
		"ses_command_1", "ses_command_claim", "ses_command_stale",
		"ses_command_expired", "ses_command_reopen", "ses_command_invalid",
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
