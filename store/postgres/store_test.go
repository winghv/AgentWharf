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
