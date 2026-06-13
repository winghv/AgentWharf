package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/winghv/agentwharf/store"
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite event store: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	st := &Store{db: db}
	if err := st.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Append(ctx context.Context, sessionID string, evs []store.PendingEvent) (firstSeq int64, err error) {
	if len(evs) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin append transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var latest int64
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(seq), 0)
FROM session_events
WHERE session_id = ?
`, sessionID).Scan(&latest); err != nil {
		return 0, fmt.Errorf("select latest seq: %w", err)
	}

	firstSeq = latest + 1
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?)
`)
	if err != nil {
		return 0, fmt.Errorf("prepare append event: %w", err)
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close append statement: %w", closeErr)
		}
	}()

	createdAt := time.Now().UnixMilli()
	for i, ev := range evs {
		seq := firstSeq + int64(i)
		if _, err := stmt.ExecContext(ctx, sessionID, seq, ev.Type, []byte(ev.Payload), ev.Time.UnixMilli(), createdAt); err != nil {
			return 0, fmt.Errorf("append event seq %d: %w", seq, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit append transaction: %w", err)
	}
	return firstSeq, nil
}

func (s *Store) Replay(ctx context.Context, sessionID string, afterSeq int64, fn func(store.Event) error) (err error) {
	if fn == nil {
		return errors.New("replay callback is nil")
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT session_id, seq, type, payload, event_time_ms
FROM session_events
WHERE session_id = ? AND seq > ?
ORDER BY seq ASC
	`, sessionID, afterSeq)
	if err != nil {
		return fmt.Errorf("query replay events: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close replay rows: %w", closeErr)
		}
	}()

	for rows.Next() {
		var (
			ev          store.Event
			payload     []byte
			eventTimeMS int64
		)
		if err := rows.Scan(&ev.SessionID, &ev.Seq, &ev.Type, &payload, &eventTimeMS); err != nil {
			return fmt.Errorf("scan replay event: %w", err)
		}
		ev.Time = time.UnixMilli(eventTimeMS)
		ev.Payload = append(ev.Payload[:0], payload...)
		if err := fn(ev); err != nil {
			return fmt.Errorf("replay event seq %d: %w", ev.Seq, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate replay events: %w", err)
	}
	return nil
}

func (s *Store) LatestSeq(ctx context.Context, sessionID string) (int64, error) {
	var latest int64
	if err := s.db.QueryRowContext(ctx, `
SELECT COALESCE(MAX(seq), 0)
FROM session_events
WHERE session_id = ?
`, sessionID).Scan(&latest); err != nil {
		return 0, fmt.Errorf("select latest seq: %w", err)
	}
	return latest, nil
}

func (s *Store) init(ctx context.Context) error {
	pragmas := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA foreign_keys = ON`,
	}
	for _, pragma := range pragmas {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure sqlite event store %q: %w", pragma, err)
		}
	}

	if _, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS session_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL CHECK (seq > 0),
	type TEXT NOT NULL,
	payload BLOB NOT NULL,
	event_time_ms INTEGER NOT NULL,
	created_at_ms INTEGER NOT NULL,
	UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS session_events_session_seq_idx
ON session_events (session_id, seq);
`); err != nil {
		return fmt.Errorf("initialize sqlite event store schema: %w", err)
	}
	return nil
}
