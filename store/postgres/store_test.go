package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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
