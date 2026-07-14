package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres/internal/db"
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

	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return 0, fmt.Errorf("lock session event stream: %w", err)
	}

	latest, err := queries.LatestSessionEventSeq(ctx, sessionID)
	if err != nil {
		return 0, fmt.Errorf("select latest seq: %w", err)
	}

	firstSeq = latest + 1
	for i, ev := range evs {
		seq := firstSeq + int64(i)
		if err := queries.InsertSessionEvent(ctx, db.InsertSessionEventParams{
			SessionID: sessionID,
			Seq:       seq,
			Type:      ev.Type,
			Payload:   ev.Payload,
			CreatedAt: pgtype.Timestamptz{Time: ev.Time, Valid: true},
		}); err != nil {
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

	queries := db.New(s.pool)
	for nextSeq := afterSeq; ; {
		row, err := queries.NextSessionEvent(ctx, db.NextSessionEventParams{
			SessionID: sessionID,
			Seq:       nextSeq,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("query replay event: %w", err)
		}
		ev := store.Event{
			SessionID: row.SessionID,
			Seq:       row.Seq,
			Type:      row.Type,
			Time:      row.CreatedAt.Time,
			Payload:   append([]byte(nil), row.Payload...),
		}
		if err := fn(ev); err != nil {
			return fmt.Errorf("replay event seq %d: %w", ev.Seq, err)
		}
		nextSeq = ev.Seq
	}
}

func (s *Store) LatestSeq(ctx context.Context, sessionID string) (int64, error) {
	if s.pool == nil {
		return 0, errors.New("postgres event store pool is nil")
	}

	latest, err := db.New(s.pool).LatestSessionEventSeq(ctx, sessionID)
	if err != nil {
		return 0, fmt.Errorf("select latest seq: %w", err)
	}
	return latest, nil
}

func advisoryLockKey(sessionID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sessionID))
	return int64(binary.BigEndian.Uint64(h.Sum(nil)))
}
