package postgres_test

import (
	"context"
	"fmt"
	"os"
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

		pool := openPool(t, dsn, schemaName)
		t.Cleanup(func() {
			pool.Close()
		})
		resetSchema(t, pool)
		return postgres.New(pool)
	})
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

func openPool(t *testing.T, dsn string, schemaName string) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres config: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName

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
