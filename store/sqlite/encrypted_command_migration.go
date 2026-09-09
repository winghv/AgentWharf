package sqlite

import (
	"context"
	"fmt"
	"strings"
)

// SQLite cannot alter a CHECK constraint in place. Rebuild atomically, copying
// every existing column verbatim; failed copying leaves the original intact.
func (s *Store) migrateEncryptedCommandTypes(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var definition string
	if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='session_pending_commands'`).Scan(&definition); err != nil {
		return err
	}
	if strings.Contains(definition, "'session.file.list'") {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `
CREATE TABLE session_pending_commands_encrypted_v2 (
 session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
 cmd_id TEXT NOT NULL CHECK (length(cmd_id) BETWEEN 1 AND 256),
 type TEXT NOT NULL CHECK (type IN ('session.send','session.interrupt','session.stop','permission.respond','session.settings.change','session.membership.change','session.file.read','session.file.list')),
 event_seq INTEGER NOT NULL CHECK (event_seq > 0),
 status TEXT NOT NULL CHECK (status IN ('pending','received','completed','outcome_unknown')),
 expires_at_ns INTEGER NOT NULL,
 created_at_ms INTEGER NOT NULL,
 updated_at_ms INTEGER NOT NULL,
 PRIMARY KEY(session_id,cmd_id),
 FOREIGN KEY(session_id,event_seq) REFERENCES session_events(session_id,seq),
 CHECK (expires_at_ns > created_at_ms * 1000000),
 CHECK (expires_at_ns <= (created_at_ms + 30000) * 1000000)
);
INSERT INTO session_pending_commands_encrypted_v2
 SELECT session_id,cmd_id,type,event_seq,status,expires_at_ns,created_at_ms,updated_at_ms FROM session_pending_commands;
DROP TABLE session_pending_commands;
ALTER TABLE session_pending_commands_encrypted_v2 RENAME TO session_pending_commands;
CREATE INDEX session_pending_commands_status_expiry_idx ON session_pending_commands(status,expires_at_ns);
`); err != nil {
		return fmt.Errorf("migrate encrypted command types: %w", err)
	}
	return tx.Commit()
}
