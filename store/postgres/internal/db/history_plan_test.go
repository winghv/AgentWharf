package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type explainPlan struct {
	Plan explainNode `json:"Plan"`
}

type explainNode struct {
	IndexCond           string        `json:"Index Cond"`
	Filter              string        `json:"Filter"`
	RowsRemovedByFilter int64         `json:"Rows Removed by Filter"`
	Plans               []explainNode `json:"Plans"`
}

func TestReverseHistoryGeneratedQueriesUseBoundedGenericPlans(t *testing.T) {
	dsn := postgresPlanDSN(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	schema := fmt.Sprintf("history_plan_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoted+" CASCADE") })
	if _, err := conn.Exec(ctx, "SET search_path TO "+quoted); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	fixture, err := os.ReadFile(filepath.Join("..", "..", "schema.sql"))
	if err != nil {
		t.Fatalf("read schema fixture: %v", err)
	}
	if _, err := conn.Exec(ctx, string(fixture)); err != nil {
		t.Fatalf("apply schema fixture: %v", err)
	}
	if _, err := conn.Exec(ctx, `
ALTER TABLE session_events DISABLE TRIGGER session_events_track_stream_insert;
INSERT INTO session_events (session_id, seq, type, payload, created_at)
SELECT 'ses_plan', value, 'session.message', jsonb_build_object('n', value), statement_timestamp()
FROM generate_series(1, 100000) AS value;
ALTER TABLE session_events ENABLE TRIGGER session_events_track_stream_insert;
ANALYZE session_events;
SET plan_cache_mode = force_generic_plan;
`); err != nil {
		t.Fatalf("seed plan fixture: %v", err)
	}

	if strings.Contains(reverseSessionEventPage, "IS NULL") || !strings.Contains(reverseSessionEventPageBefore, "seq < $2") {
		t.Fatal("generated history queries did not separate cursor and no-cursor predicates")
	}
	assertPreparedHistoryPlan(t, ctx, conn, "history_no_cursor", "text, integer", reverseSessionEventPage,
		"'ses_plan', 2", false)
	assertPreparedHistoryPlan(t, ctx, conn, "history_cursor", "text, bigint, integer", reverseSessionEventPageBefore,
		"'ses_plan', 2, 2", true)
}

func assertPreparedHistoryPlan(t *testing.T, ctx context.Context, conn *pgx.Conn, name, types, query, args string, requireSeq bool) {
	t.Helper()
	if _, err := conn.Exec(ctx, "PREPARE "+name+"("+types+") AS "+query); err != nil {
		t.Fatalf("prepare %s: %v", name, err)
	}
	var raw []byte
	if err := conn.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE "+name+"("+args+")").Scan(&raw); err != nil {
		t.Fatalf("explain %s: %v", name, err)
	}
	var plans []explainPlan
	if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("decode %s plan: %v", name, err)
	}
	if !hasBoundedIndexPlan(plans[0].Plan, requireSeq) {
		t.Fatalf("%s lacks bounded session/seq index condition: %s", name, raw)
	}
}

func hasBoundedIndexPlan(node explainNode, requireSeq bool) bool {
	condition := strings.ToLower(node.IndexCond)
	bounded := strings.Contains(condition, "session_id") && (!requireSeq || strings.Contains(condition, "seq < $2"))
	if bounded && node.Filter == "" && node.RowsRemovedByFilter == 0 {
		return true
	}
	for _, child := range node.Plans {
		if hasBoundedIndexPlan(child, requireSeq) {
			return true
		}
	}
	return false
}

func postgresPlanDSN(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"AGENTWHARF_POSTGRES_TEST_DATABASE_URL", "SUPERWHV_TEST_DATABASE_URL", "DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	t.Skip("set a PostgreSQL test database URL")
	return ""
}
