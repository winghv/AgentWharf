package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/winghv/agentwharf/store"
)

func (s *Store) ValidateAdapterEffectAdmission(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	if s == nil || s.connectionTx == nil { return store.AdapterConnection{}, errors.New("adapter authority transaction is required") }
	if _, err := s.connectionTx.ExecContext(ctx, `UPDATE session_adapter_connections SET updated_at_ms=updated_at_ms WHERE session_id=?`, sessionID); err != nil { return store.AdapterConnection{}, fmt.Errorf("lock adapter admission: %w", err) }
	return s.ValidateAdapterAdmission(ctx, sessionID, admission)
}

func (s *Store) AppendAdapterEvents(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, events []store.PendingEvent) (firstSeq int64, err error) {
	if s == nil || s.db == nil || len(events) == 0 {
		return 0, errors.New("invalid sqlite adapter event commit")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin adapter event commit: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE session_adapter_connections SET updated_at_ms=updated_at_ms WHERE session_id=?`, sessionID); err != nil {
		return 0, fmt.Errorf("lock adapter authority: %w", err)
	}
	bound := &Store{db: s.db, fenceDB: s.fenceDB, connectionTx: tx}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE session_id=?`, sessionID).Scan(&firstSeq); err != nil {
		return 0, fmt.Errorf("select adapter event sequence: %w", err)
	}
	firstSeq++
	createdAt := time.Now().UnixMilli()
	for index, event := range events {
		if _, err := tx.ExecContext(ctx, `INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)`, sessionID, firstSeq+int64(index), event.Type, []byte(event.Payload), event.Time.UnixMilli(), createdAt); err != nil {
			return 0, fmt.Errorf("append adapter event: %w", err)
		}
	}
	if _, err := bound.ValidateAdapterAdmission(ctx, sessionID, admission); err != nil {
		return 0, errors.New("adapter authority lost")
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit adapter events: %w", err)
	}
	return firstSeq, nil
}
