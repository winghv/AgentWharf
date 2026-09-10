package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestEncryptedCommandMigrationPreservesLegacyRows(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON;
 CREATE TABLE session_events(session_id TEXT,seq INTEGER,UNIQUE(session_id,seq));
 INSERT INTO session_events VALUES('session',1);
 CREATE TABLE session_pending_commands(session_id TEXT,cmd_id TEXT,type TEXT CHECK(type='session.send'),event_seq INTEGER,status TEXT,expires_at_ns INTEGER,created_at_ms INTEGER,updated_at_ms INTEGER,PRIMARY KEY(session_id,cmd_id));
 INSERT INTO session_pending_commands VALUES('session','cmd','session.send',1,'received',2000000000,1000,1100);`); err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db}
	for i := 0; i < 2; i++ {
		if err := s.migrateEncryptedCommandTypes(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var status string
	var expires, created, updated int64
	if err := db.QueryRowContext(ctx, `SELECT status,expires_at_ns,created_at_ms,updated_at_ms FROM session_pending_commands WHERE cmd_id='cmd'`).Scan(&status, &expires, &created, &updated); err != nil || status != "received" || expires != 2000000000 || created != 1000 || updated != 1100 {
		t.Fatal("migration changed row", err)
	}
	for _, kind := range []string{"session.interrupt", "session.stop", "permission.respond", "session.settings.change", "session.membership.change", "session.file.read", "session.file.list"} {
		if _, err := db.ExecContext(ctx, `INSERT INTO session_pending_commands VALUES('session',?,?,1,'pending',2000000000,1000,1000)`, kind, kind); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_pending_commands VALUES('session','bad','unsupported',1,'pending',2000000000,1000,1000)`); err == nil {
		t.Fatal("unknown command accepted")
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_pending_commands VALUES('session','missing','session.stop',2,'pending',2000000000,1000,1000)`); err == nil {
		t.Fatal("foreign key lost")
	}
}

func TestEncryptedCommandMigrationUpgradesPreFileListSchema(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "pre-file-list.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON;
 CREATE TABLE session_events(session_id TEXT,seq INTEGER,UNIQUE(session_id,seq));
 INSERT INTO session_events VALUES('session',1);
 CREATE TABLE session_pending_commands(session_id TEXT,cmd_id TEXT,type TEXT CHECK(type IN ('session.send','session.interrupt','session.stop','permission.respond','session.settings.change','session.membership.change','session.file.read')),event_seq INTEGER,status TEXT,expires_at_ns INTEGER,created_at_ms INTEGER,updated_at_ms INTEGER,PRIMARY KEY(session_id,cmd_id),FOREIGN KEY(session_id,event_seq) REFERENCES session_events(session_id,seq));
 CREATE INDEX session_pending_commands_status_expiry_idx ON session_pending_commands(status,expires_at_ns);
 INSERT INTO session_pending_commands VALUES('session','read','session.file.read',1,'received',2000000000,1000,1100);`); err != nil {
		t.Fatal(err)
	}
	store := &Store{db: db}
	if err := store.migrateEncryptedCommandTypes(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_pending_commands VALUES('session','list','session.file.list',1,'pending',2000000000,1000,1000)`); err != nil {
		t.Fatal("file-list command unavailable after migration", err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM session_pending_commands WHERE cmd_id='read'`).Scan(&status); err != nil || status != "received" {
		t.Fatal("existing file-read row changed", err)
	}
}
