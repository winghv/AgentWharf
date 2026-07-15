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

const maxHistoryPageSize = 100

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

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin replay transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	queries := db.New(tx)
	for nextSeq := afterSeq; ; {
		row, err := queries.NextSessionEvent(ctx, db.NextSessionEventParams{
			SessionID: sessionID,
			Seq:       nextSeq,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit replay transaction: %w", err)
			}
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

func (s *Store) History(ctx context.Context, sessionID string, beforeSeq *int64, limit int) (store.HistoryPage, error) {
	if limit < 1 || limit > maxHistoryPageSize {
		return store.HistoryPage{}, errors.New("history limit is out of range")
	}
	if s.pool == nil {
		return store.HistoryPage{}, errors.New("postgres event store pool is nil")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return store.HistoryPage{}, fmt.Errorf("begin history transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	queries := db.New(tx)
	bounds, err := queries.SessionEventHistoryBounds(ctx, sessionID)
	if err != nil {
		return store.HistoryPage{}, fmt.Errorf("select history bounds: %w", err)
	}
	cursor := pgtype.Int8{}
	if beforeSeq != nil {
		cursor = pgtype.Int8{Int64: *beforeSeq, Valid: true}
	}
	rows, err := queries.ReverseSessionEventPage(ctx, db.ReverseSessionEventPageParams{
		SessionID: sessionID,
		BeforeSeq: cursor,
		PageLimit: int32(limit + 1),
	})
	if err != nil {
		return store.HistoryPage{}, fmt.Errorf("select reverse history page: %w", err)
	}

	page := store.HistoryPage{
		LatestSeq:      bounds.LatestSeq,
		RetentionState: store.RetentionComplete,
	}
	if bounds.EarliestSeq > 1 {
		page.RetentionState = store.RetentionGap
	}
	if len(rows) > limit {
		rows = rows[:limit]
		next := rows[len(rows)-1].Seq
		page.NextBeforeSeq = &next
	}
	page.Events = make([]store.Event, len(rows))
	for index, row := range rows {
		page.Events[len(rows)-1-index] = store.Event{
			SessionID: row.SessionID,
			Seq:       row.Seq,
			Type:      row.Type,
			Time:      row.CreatedAt.Time,
			Payload:   append([]byte(nil), row.Payload...),
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return store.HistoryPage{}, fmt.Errorf("commit history transaction: %w", err)
	}
	return page, nil
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
