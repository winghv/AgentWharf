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
		"CREATE TABLE session_attention_summaries",
		"last_durable_event_at",
		"last_client_command_at",
		"CREATE TABLE session_pending_commands",
		"CREATE TABLE session_attachments",
		"proposal_id TEXT",
		"CREATE TABLE session_adapter_connections",
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
