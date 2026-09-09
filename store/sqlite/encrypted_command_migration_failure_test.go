package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestEncryptedCommandMigrationFailurePreservesOriginal(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	// A legacy/corrupt row violates the destination FK. The upgrade must refuse
	// rather than drop it or disable foreign key enforcement.
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON;
 CREATE TABLE session_events(session_id TEXT,seq INTEGER,UNIQUE(session_id,seq));
 CREATE TABLE session_pending_commands(session_id TEXT,cmd_id TEXT,type TEXT CHECK(type='session.send'),event_seq INTEGER,status TEXT,expires_at_ns INTEGER,created_at_ms INTEGER,updated_at_ms INTEGER,PRIMARY KEY(session_id,cmd_id));
 CREATE INDEX session_pending_commands_status_expiry_idx ON session_pending_commands(status,expires_at_ns);
 INSERT INTO session_pending_commands VALUES('session','cmd','session.send',1,'received',2000000000,1000,1100);`); err != nil {
		t.Fatal(err)
	}
	backend := &Store{db: db}
	for attempt := 0; attempt < 2; attempt++ {
		if err := backend.migrateEncryptedCommandTypes(ctx); err == nil {
			t.Fatal("invalid original silently migrated")
		}
		var status string
		if err := db.QueryRowContext(ctx, `SELECT status FROM session_pending_commands WHERE cmd_id='cmd'`).Scan(&status); err != nil || status != "received" {
			t.Fatalf("original row lost: %s %v", status, err)
		}
		var temporary, index int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name='session_pending_commands_encrypted_v2'`).Scan(&temporary); err != nil || temporary != 0 {
			t.Fatal("temporary table survived failed transaction", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name='session_pending_commands_status_expiry_idx'`).Scan(&index); err != nil || index != 1 {
			t.Fatal("original index lost", err)
		}
	}
	// Repair only the isolated test fixture, then prove a failed attempt did not
	// poison a subsequent legitimate upgrade.
	if _, err := db.ExecContext(ctx, `INSERT INTO session_events VALUES('session',1)`); err != nil {
		t.Fatal(err)
	}
	if err := backend.migrateEncryptedCommandTypes(ctx); err != nil {
		t.Fatal(err)
	}
}
