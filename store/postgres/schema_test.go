package postgres_test

import (
	"os"
	"strings"
	"testing"
)

func TestSchemaFixtureIsEventStoreOnly(t *testing.T) {
	fixture, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read sqlc schema fixture: %v", err)
	}
	schema := string(fixture)
	for _, expected := range []string{
		"sqlc/self-hosted schema fixture",
		"CREATE TABLE session_events",
		"UNIQUE (session_id, seq)",
		"session_events_session_seq_idx",
	} {
		if !strings.Contains(schema, expected) {
			t.Fatalf("schema fixture missing %q", expected)
		}
	}
	for _, forbidden := range []string{"task_id", "run_id", "vm_id", "org_id", "provider", "credential", "token", "bearer", "content"} {
		if strings.Contains(strings.ToLower(schema), forbidden) {
			t.Fatalf("schema fixture contains non-EventStore field %q", forbidden)
		}
	}
}
