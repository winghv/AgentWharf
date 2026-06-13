package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/store"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Append(ctx context.Context, sessionID string, evs []store.PendingEvent) (firstSeq int64, err error) {
	if len(evs) == 0 {
		return 0, nil
	}
	if s.pool == nil {
		return 0, errors.New("postgres event store pool is nil")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin append transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryLockKey(sessionID)); err != nil {
		return 0, fmt.Errorf("lock session event stream: %w", err)
	}

	var latest int64
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(MAX(seq), 0)
FROM session_events
WHERE session_id = $1
`, sessionID).Scan(&latest); err != nil {
		return 0, fmt.Errorf("select latest seq: %w", err)
	}

	firstSeq = latest + 1
	for i, ev := range evs {
		seq := firstSeq + int64(i)
		if _, err := tx.Exec(ctx, `
INSERT INTO session_events (session_id, seq, type, payload, created_at)
VALUES ($1, $2, $3, $4, $5)
`, sessionID, seq, ev.Type, []byte(ev.Payload), ev.Time); err != nil {
			return 0, fmt.Errorf("append event seq %d: %w", seq, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit append transaction: %w", err)
	}
	return firstSeq, nil
}

func (s *Store) Replay(ctx context.Context, sessionID string, afterSeq int64, fn func(store.Event) error) (err error) {
	if fn == nil {
		return errors.New("replay callback is nil")
	}
	if s.pool == nil {
		return errors.New("postgres event store pool is nil")
	}

	rows, err := s.pool.Query(ctx, `
SELECT session_id, seq, type, payload, created_at
FROM session_events
WHERE session_id = $1 AND seq > $2
ORDER BY seq ASC
`, sessionID, afterSeq)
	if err != nil {
		return fmt.Errorf("query replay events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ev        store.Event
			payload   []byte
			createdAt time.Time
		)
		if err := rows.Scan(&ev.SessionID, &ev.Seq, &ev.Type, &payload, &createdAt); err != nil {
			return fmt.Errorf("scan replay event: %w", err)
		}
		ev.Time = createdAt
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
	if s.pool == nil {
		return 0, errors.New("postgres event store pool is nil")
	}

	var latest int64
	if err := s.pool.QueryRow(ctx, `
SELECT COALESCE(MAX(seq), 0)
FROM session_events
WHERE session_id = $1
`, sessionID).Scan(&latest); err != nil {
		return 0, fmt.Errorf("select latest seq: %w", err)
	}
	return latest, nil
}

func advisoryLockKey(sessionID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sessionID))
	return int64(binary.BigEndian.Uint64(h.Sum(nil)))
}
