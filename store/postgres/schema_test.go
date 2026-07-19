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
		"CREATE TABLE session_event_streams",
		"session_event_streams_enforce_monotonicity",
		"session_events_track_stream_insert",
		"session_events_mark_stream_retention_gap",
		"CREATE TABLE session_attention_summaries",
		"last_durable_event_at",
		"last_client_command_at",
		"CREATE TABLE session_pending_commands",
		"session_id TEXT NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 255) REFERENCES agent_sessions(id)",
		"CREATE TABLE session_attachments",
		"status = 'join_pending' AND delivery_state IN ('pending', 'received', 'completed')",
		"proposal_id TEXT",
		"CREATE TABLE session_adapter_connections",
		"pending_credential_generation <> prior_recovery_credential_generation",
		"revoked_at IS NULL OR revoked_at >= created_at",
		"terminal_at IS NULL OR terminal_at >= created_at",
		"CREATE TABLE session_attach_attempts",
		"CREATE TABLE session_workspace_leases",
		"lease_id TEXT NOT NULL",
		"status IN ('reserved', 'start_received', 'quarantined', 'released')",
	} {
		if !strings.Contains(schema, expected) {
			t.Fatalf("schema fixture missing %q", expected)
		}
	}
	for _, forbidden := range []string{"task_id", "run_id", "vm_id", "org_id", "provider_object", "raw_credential", "credential_value", "token", "bearer", "content"} {
		if strings.Contains(strings.ToLower(schema), forbidden) {
			t.Fatalf("schema fixture contains non-EventStore field %q", forbidden)
		}
	}
}

func TestSchemaFixtureSessionEventsHasNoPlatformForeignKey(t *testing.T) {
	fixture, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read sqlc schema fixture: %v", err)
	}
	schema := string(fixture)
	eventStart := strings.Index(schema, "CREATE TABLE session_events")
	eventEnd := strings.Index(schema, "CREATE INDEX session_events_session_seq_idx")
	if eventStart < 0 || eventEnd <= eventStart {
		t.Fatal("schema fixture has malformed session_events section")
	}
	if strings.Contains(schema[eventStart:eventEnd], "REFERENCES agent_sessions(id)") {
		t.Fatal("session_events fixture retains production-unbacked agent_sessions foreign key")
	}
}

func TestSchemaFixtureCreatesStreamBeforeItsTriggers(t *testing.T) {
	fixture, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read sqlc schema fixture: %v", err)
	}
	schema := string(fixture)
	table := strings.Index(schema, "CREATE TABLE session_event_streams")
	insertTrigger := strings.Index(schema, "CREATE TRIGGER session_events_track_stream_insert")
	deleteTrigger := strings.Index(schema, "CREATE TRIGGER session_events_mark_stream_retention_gap")
	nextTable := strings.Index(schema, "CREATE TABLE session_attention_summaries")
	if table < 0 || insertTrigger <= table || deleteTrigger <= insertTrigger || nextTable <= deleteTrigger {
		t.Fatal("stream table and retention triggers are not created in dependency order")
	}
}

func TestSchemaFixtureForeignKeysUseDefaultReferentialActions(t *testing.T) {
	fixture, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read sqlc schema fixture: %v", err)
	}
	schema := strings.ToUpper(string(fixture))
	for _, unexpected := range []string{" ON DELETE ", " ON UPDATE ", " MATCH ", " DEFERRABLE"} {
		if strings.Contains(schema, unexpected) {
			t.Fatalf("schema fixture unexpectedly overrides foreign key semantics with %q", unexpected)
		}
	}
}
