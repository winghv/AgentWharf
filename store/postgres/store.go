package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

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
	firstSeq, err = appendEventsLocked(ctx, queries, sessionID, evs)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit append transaction: %w", err)
	}
	return firstSeq, nil
}

func appendEventsLocked(ctx context.Context, queries *db.Queries, sessionID string, evs []store.PendingEvent) (int64, error) {
	latest, err := queries.LatestSessionEventSeq(ctx, sessionID)
	if err != nil {
		return 0, fmt.Errorf("select latest seq: %w", err)
	}
	firstSeq := latest + 1
	for index, event := range evs {
		seq := firstSeq + int64(index)
		if err := queries.InsertSessionEvent(ctx, db.InsertSessionEventParams{
			SessionID: sessionID,
			Seq:       seq,
			Type:      event.Type,
			Payload:   event.Payload,
			CreatedAt: pgtype.Timestamptz{Time: event.Time, Valid: true},
		}); err != nil {
			return 0, fmt.Errorf("append event seq %d: %w", seq, err)
		}
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
			Payload:   compactJSON(row.Payload),
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
	historyState, err := queries.SessionEventHistoryState(ctx, sessionID)
	if err != nil {
		return store.HistoryPage{}, fmt.Errorf("select history state: %w", err)
	}
	rows := make([]historyRow, 0, limit+1)
	if beforeSeq == nil {
		pageRows, queryErr := queries.ReverseSessionEventPage(ctx, db.ReverseSessionEventPageParams{
			SessionID: sessionID,
			PageLimit: int32(limit + 1),
		})
		if queryErr != nil {
			return store.HistoryPage{}, fmt.Errorf("select reverse history page: %w", queryErr)
		}
		for _, row := range pageRows {
			rows = append(rows, historyRow(row))
		}
	} else {
		pageRows, queryErr := queries.ReverseSessionEventPageBefore(ctx, db.ReverseSessionEventPageBeforeParams{
			SessionID: sessionID,
			BeforeSeq: *beforeSeq,
			PageLimit: int32(limit + 1),
		})
		if queryErr != nil {
			return store.HistoryPage{}, fmt.Errorf("select reverse history page before cursor: %w", queryErr)
		}
		for _, row := range pageRows {
			rows = append(rows, historyRow(row))
		}
	}

	page := store.HistoryPage{
		LatestSeq:      historyState.LatestSeq,
		RetentionState: store.RetentionComplete,
	}
	if historyState.RetentionGap {
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
			Payload:   compactJSON(row.Payload),
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return store.HistoryPage{}, fmt.Errorf("commit history transaction: %w", err)
	}
	return page, nil
}

type historyRow struct {
	SessionID string
	Seq       int64
	Type      string
	Payload   []byte
	CreatedAt pgtype.Timestamptz
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

func (s *Store) CommitPendingCommand(ctx context.Context, sessionID string, authority store.CommandAuthority, event store.PendingEvent, request store.PendingCommandRequest) (store.PendingCommandCommit, error) {
	if s.pool == nil {
		return store.PendingCommandCommit{}, errors.New("postgres event store pool is nil")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("begin pending command transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := lockCommandAuthority(ctx, queries, sessionID, authority); err != nil {
		return store.PendingCommandCommit{}, err
	}
	storeNow, err := queries.CommandStoreNow(ctx)
	if err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("read command Store clock: %w", err)
	}
	if err := validatePendingCommandInput(event, request, storeNow.Time); err != nil {
		return store.PendingCommandCommit{}, err
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("lock pending command stream: %w", err)
	}
	if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
		return store.PendingCommandCommit{}, err
	}
	storeNow, err = queries.CommandStoreNow(ctx)
	if err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("refresh command Store clock: %w", err)
	}
	if err := validatePendingCommandInput(event, request, storeNow.Time); err != nil {
		return store.PendingCommandCommit{}, err
	}
	existing, err := queries.PendingCommandByID(ctx, db.PendingCommandByIDParams{SessionID: sessionID, CmdID: request.CommandID})
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return store.PendingCommandCommit{}, fmt.Errorf("commit duplicate pending command lookup: %w", err)
		}
		return store.PendingCommandCommit{Command: pendingCommand(existing), Duplicate: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.PendingCommandCommit{}, fmt.Errorf("select pending command: %w", err)
	}
	seq, err := appendEventsLocked(ctx, queries, sessionID, []store.PendingEvent{event})
	if err != nil {
		return store.PendingCommandCommit{}, err
	}
	row, err := queries.InsertPendingCommand(ctx, db.InsertPendingCommandParams{
		SessionID:            sessionID,
		CmdID:                request.CommandID,
		Type:                 request.Type,
		EventSeq:             seq,
		ExpiresAt:            pgtype.Timestamptz{Time: request.ExpiresAt, Valid: true},
		ConnectionEpoch:      authority.ConnectionEpoch,
		CredentialGeneration: authority.CredentialGeneration,
	})
	if err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("insert pending command: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("commit pending command: %w", err)
	}
	return store.PendingCommandCommit{Command: pendingCommand(row)}, nil
}

func (s *Store) ClaimPendingCommand(ctx context.Context, sessionID string, authority store.CommandAuthority, commandID string) (store.PendingCommandClaim, error) {
	tx, queries, err := s.beginCommandMutation(ctx, sessionID, authority)
	if err != nil {
		return store.PendingCommandClaim{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, err := queries.LockPendingCommandForClaim(ctx, db.LockPendingCommandForClaimParams{SessionID: sessionID, CmdID: commandID})
	if err != nil {
		return store.PendingCommandClaim{}, fmt.Errorf("lock claimable pending command: %w", err)
	}
	command := pendingCommand(row)
	if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
		return store.PendingCommandClaim{}, err
	}
	storeNow, err := queries.CommandStoreNow(ctx)
	if err != nil {
		return store.PendingCommandClaim{}, fmt.Errorf("refresh claim Store clock: %w", err)
	}
	if !command.ExpiresAt.After(storeNow.Time) {
		return store.PendingCommandClaim{}, errors.New("pending command expired")
	}
	claimed := false
	if command.Status == store.PendingCommandPending {
		updated, updateErr := queries.UpdatePendingCommandStatus(ctx, db.UpdatePendingCommandStatusParams{
			Status: string(store.PendingCommandReceived), SessionID: sessionID, CmdID: commandID,
			ExpectedStatus:   string(store.PendingCommandPending),
			RequireUnexpired: true, ConnectionEpoch: authority.ConnectionEpoch,
			CredentialGeneration: authority.CredentialGeneration,
		})
		if updateErr != nil {
			return store.PendingCommandClaim{}, fmt.Errorf("claim pending command: %w", updateErr)
		}
		command = pendingCommand(updated)
		claimed = true
	}
	if err := tx.Commit(ctx); err != nil {
		return store.PendingCommandClaim{}, fmt.Errorf("commit pending command claim: %w", err)
	}
	return store.PendingCommandClaim{Command: command, Claimed: claimed}, nil
}

func (s *Store) ResolvePendingCommand(ctx context.Context, sessionID string, authority store.CommandAuthority, commandID string, status store.PendingCommandStatus) (store.PendingCommand, error) {
	if status != store.PendingCommandCompleted && status != store.PendingCommandOutcomeUnknown {
		return store.PendingCommand{}, errors.New("invalid pending command outcome")
	}
	tx, queries, err := s.beginCommandMutation(ctx, sessionID, authority)
	if err != nil {
		return store.PendingCommand{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, err := queries.LockPendingCommandForResolve(ctx, db.LockPendingCommandForResolveParams{SessionID: sessionID, CmdID: commandID})
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("lock resolvable pending command: %w", err)
	}
	if store.PendingCommandStatus(row.Status) != store.PendingCommandReceived {
		return store.PendingCommand{}, errors.New("pending command is not received")
	}
	if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
		return store.PendingCommand{}, err
	}
	updated, err := queries.UpdatePendingCommandStatus(ctx, db.UpdatePendingCommandStatusParams{
		Status: string(status), SessionID: sessionID, CmdID: commandID,
		ExpectedStatus:   string(store.PendingCommandReceived),
		RequireUnexpired: false, ConnectionEpoch: authority.ConnectionEpoch,
		CredentialGeneration: authority.CredentialGeneration,
	})
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("resolve pending command: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return store.PendingCommand{}, fmt.Errorf("commit pending command resolution: %w", err)
	}
	return pendingCommand(updated), nil
}

func (s *Store) beginCommandMutation(ctx context.Context, sessionID string, authority store.CommandAuthority) (pgx.Tx, *db.Queries, error) {
	if s.pool == nil {
		return nil, nil, errors.New("postgres event store pool is nil")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("begin command mutation: %w", err)
	}
	queries := db.New(tx)
	if err := lockCommandAuthority(ctx, queries, sessionID, authority); err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, err
	}
	return tx, queries, nil
}

func lockCommandAuthority(ctx context.Context, queries *db.Queries, sessionID string, authority store.CommandAuthority) error {
	_, err := queries.LockCommandAuthority(ctx, db.LockCommandAuthorityParams{
		SessionID: sessionID, ConnectionEpoch: authority.ConnectionEpoch,
		CredentialGeneration: authority.CredentialGeneration,
	})
	if err != nil {
		return fmt.Errorf("lock current command authority: %w", err)
	}
	return nil
}

func validateCommandAuthorityCurrent(ctx context.Context, queries *db.Queries, sessionID string, authority store.CommandAuthority) error {
	current, err := queries.CommandAuthorityCurrent(ctx, db.CommandAuthorityCurrentParams{
		SessionID: sessionID, ConnectionEpoch: authority.ConnectionEpoch,
		CredentialGeneration: authority.CredentialGeneration,
	})
	if err != nil {
		return fmt.Errorf("revalidate command authority: %w", err)
	}
	if !current {
		return errors.New("command authority is no longer current")
	}
	return nil
}

func pendingCommand(row db.SessionPendingCommand) store.PendingCommand {
	return store.PendingCommand{
		SessionID: row.SessionID, CommandID: row.CmdID, Type: row.Type, EventSeq: row.EventSeq,
		Status: store.PendingCommandStatus(row.Status), ExpiresAt: row.ExpiresAt.Time,
	}
}

func validatePendingCommandInput(event store.PendingEvent, request store.PendingCommandRequest, storeNow time.Time) error {
	var payload struct {
		Role string `json:"role"`
	}
	if request.CommandID == "" || len(request.CommandID) > 256 || request.Type != "session.send" ||
		event.Type != "session.message" || json.Unmarshal(event.Payload, &payload) != nil || payload.Role != "user" ||
		!request.ExpiresAt.After(storeNow) || request.ExpiresAt.After(storeNow.Add(30*time.Second)) {
		return errors.New("invalid pending command")
	}
	return nil
}

func compactJSON(payload []byte) []byte {
	var compact bytes.Buffer
	if json.Compact(&compact, payload) != nil {
		return append([]byte(nil), payload...)
	}
	return compact.Bytes()
}

func advisoryLockKey(sessionID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sessionID))
	return int64(binary.BigEndian.Uint64(h.Sum(nil)))
}
