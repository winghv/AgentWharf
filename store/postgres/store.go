package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
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
	pool         *pgxpool.Pool
	connectionTx pgx.Tx
}

const maxHistoryPageSize = 100
const maxAttachAttemptTTL = 5 * time.Minute

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// NewAdapterConnectionTx binds connection operations to a caller-owned
// transaction. The caller alone commits or rolls it back.
func NewAdapterConnectionTx(tx pgx.Tx) *Store {
	return &Store{connectionTx: tx}
}

func (s *Store) AttentionSnapshot(ctx context.Context, sessionIDs []string) ([]store.SessionAttentionSummary, error) {
	if s.pool == nil {
		return nil, errors.New("postgres event store pool is nil")
	}
	if len(sessionIDs) == 0 || len(sessionIDs) > 100 {
		return nil, errors.New("attention snapshot session IDs are out of range")
	}
	seen := make(map[string]struct{}, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		if !validConnectionID(sessionID) {
			return nil, errors.New("attention snapshot session ID is invalid")
		}
		seen[sessionID] = struct{}{}
	}
	if len(seen) != len(sessionIDs) {
		return nil, errors.New("attention snapshot session IDs must be unique")
	}
	rows, err := db.New(s.pool).AttentionSnapshot(ctx, sessionIDs)
	if err != nil {
		return nil, fmt.Errorf("select attention snapshot: %w", err)
	}
	summaries := make([]store.SessionAttentionSummary, len(rows))
	for index, row := range rows {
		summary, err := attentionSummary(row)
		if err != nil {
			return nil, err
		}
		summaries[index] = summary
	}
	return summaries, nil
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
	firstSeq, terminal, err := appendEventsLocked(ctx, queries, sessionID, evs)
	if err != nil {
		return 0, err
	}
	if terminal {
		if err := queries.FenceAttentionTerminal(ctx, sessionID); err != nil {
			return 0, fmt.Errorf("fence terminal append event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit append transaction: %w", err)
	}
	return firstSeq, nil
}

func appendEventsLocked(ctx context.Context, queries *db.Queries, sessionID string, evs []store.PendingEvent) (int64, bool, error) {
	latest, err := queries.LatestSessionEventSeq(ctx, sessionID)
	if err != nil {
		return 0, false, fmt.Errorf("select latest seq: %w", err)
	}
	firstSeq := latest + 1
	terminal := false
	for index, event := range evs {
		seq := firstSeq + int64(index)
		projection := attentionEventProjection(event)
		if err := queries.InsertSessionEvent(ctx, db.InsertSessionEventParams{
			SessionID: sessionID,
			Seq:       seq,
			Type:      event.Type,
			Payload:   event.Payload,
			CreatedAt: pgtype.Timestamptz{Time: event.Time, Valid: true},
		}); err != nil {
			return 0, false, fmt.Errorf("append event seq %d: %w", seq, err)
		}
		storeNow, err := queries.AttentionStoreNow(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("read attention Store clock: %w", err)
		}
		if err := projectAttentionEvent(ctx, queries, sessionID, seq, projection, storeNow.Time); err != nil {
			return 0, false, fmt.Errorf("project attention event seq %d: %w", seq, err)
		}
		terminal = terminal || projection.terminal
	}
	return firstSeq, terminal, nil
}

type attentionProjection struct {
	state                any
	stateObserved        bool
	permissionID         pgtype.Text
	permissionDecisionID pgtype.Text
	permissionChange     bool
	terminalOutcome      pgtype.Text
	projectionIncomplete bool
	terminal             bool
}

func projectAttentionEvent(ctx context.Context, queries *db.Queries, sessionID string, seq int64, projection attentionProjection, at time.Time) error {
	latestChangeSeq := pgtype.Int8{}
	if projection.state != nil {
		latestChangeSeq = pgtype.Int8{Int64: seq, Valid: true}
	}
	return queries.UpsertAttentionEvent(ctx, db.UpsertAttentionEventParams{
		SessionID: sessionID, LatestSeq: seq, EventState: projection.state, PermissionID: projection.permissionID,
		PermissionDecisionID: projection.permissionDecisionID, PermissionChange: projection.permissionChange,
		TerminalOutcome: projection.terminalOutcome, LatestChangeSeq: latestChangeSeq,
		EventTime: pgtype.Timestamptz{Time: at, Valid: true}, StateObserved: projection.stateObserved,
		ProjectionIncomplete: projection.projectionIncomplete,
	})
}

func attentionEventProjection(event store.PendingEvent) attentionProjection {
	projection := attentionProjection{}
	if event.Type == "permission.request" {
		var payload struct {
			RequestID string `json:"request_id"`
		}
		if json.Unmarshal(event.Payload, &payload) != nil || !validConnectionID(payload.RequestID) {
			projection.projectionIncomplete = true
			return projection
		}
		projection.permissionID, projection.permissionChange = pgtype.Text{String: payload.RequestID, Valid: true}, true
		return projection
	}
	if event.Type == "permission.decision" {
		var payload struct {
			RequestID string `json:"request_id"`
			Decision  string `json:"decision"`
		}
		if json.Unmarshal(event.Payload, &payload) != nil || !validConnectionID(payload.RequestID) ||
			(payload.Decision != "approve" && payload.Decision != "deny" && payload.Decision != "expired") {
			projection.projectionIncomplete = true
			return projection
		}
		projection.permissionDecisionID, projection.permissionChange = pgtype.Text{String: payload.RequestID, Valid: true}, true
		return projection
	}
	if event.Type == "session.error" {
		return attentionProjection{state: "error", stateObserved: true, terminalOutcome: pgtype.Text{String: "error", Valid: true}, terminal: true}
	}
	if event.Type != "session.state" {
		return projection
	}
	projection.stateObserved = true
	var payload struct {
		State string `json:"state"`
	}
	if json.Unmarshal(event.Payload, &payload) != nil {
		projection.projectionIncomplete = true
		return projection
	}
	switch payload.State {
	case "working":
		projection.state = "busy"
	case "starting", "ready", "busy", "waiting_permission", "recovering", "ended", "error":
		projection.state = payload.State
	default:
		projection.projectionIncomplete = true
		return projection
	}
	if projection.state == "ended" || projection.state == "error" {
		projection.terminal = true
		projection.terminalOutcome = pgtype.Text{String: projection.state.(string), Valid: true}
	}
	return projection
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
	seq, terminal, err := appendEventsLocked(ctx, queries, sessionID, []store.PendingEvent{event})
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
	if err := upsertAttentionLedger(ctx, queries, sessionID, nil, &storeNow.Time); err != nil {
		return store.PendingCommandCommit{}, err
	}
	if terminal {
		if err := queries.FenceAttentionTerminal(ctx, sessionID); err != nil {
			return store.PendingCommandCommit{}, fmt.Errorf("fence terminal pending command event: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("commit pending command: %w", err)
	}
	return store.PendingCommandCommit{Command: pendingCommand(row)}, nil
}

func upsertAttentionLedger(ctx context.Context, queries *db.Queries, sessionID string, blocker *store.AttentionBlocker, at *time.Time) error {
	params := db.UpsertAttentionLedgerParams{SessionID: sessionID}
	if at != nil {
		params.ClientCommandAt = pgtype.Timestamptz{Time: *at, Valid: true}
	}
	if blocker != nil {
		params.BlockerKind = pgtype.Text{String: blocker.Kind, Valid: true}
		params.BlockerReason = textValue(blocker.Reason)
		params.BlockerExpiresAt = timeValue(blocker.ExpiresAt)
		params.BlockingSessionID = textValue(blocker.BlockingSessionID)
		params.BlockerOperation = textValue(blocker.Operation)
	}
	if err := queries.UpsertAttentionLedger(ctx, params); err != nil {
		return fmt.Errorf("project attention ledger: %w", err)
	}
	return nil
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
	var blocker *store.AttentionBlocker
	if status == store.PendingCommandOutcomeUnknown {
		operation := "command"
		blocker = &store.AttentionBlocker{Kind: store.AttentionBlockerOutcomeUnknown, Operation: &operation}
	}
	if blocker != nil {
		if err := upsertAttentionLedger(ctx, queries, sessionID, blocker, nil); err != nil {
			return store.PendingCommand{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return store.PendingCommand{}, fmt.Errorf("commit pending command resolution: %w", err)
	}
	return pendingCommand(updated), nil
}

func (s *Store) CommitProposedEvent(ctx context.Context, sessionID string, authority store.CommandAuthority, proposal store.ProposedEventRequest) (store.ProposedEventReceipt, error) {
	if s.pool == nil {
		return store.ProposedEventReceipt{}, errors.New("postgres event store pool is nil")
	}
	event := proposal.Event
	event.Payload = append([]byte(nil), proposal.Event.Payload...)
	if err := validateProposedEventInput(sessionID, authority, proposal.ProposalID, event); err != nil {
		return store.ProposedEventReceipt{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("begin proposed event transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := lockCommandAuthority(ctx, queries, sessionID, authority); err != nil {
		return store.ProposedEventReceipt{}, err
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("lock proposed event stream: %w", err)
	}
	if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
		return store.ProposedEventReceipt{}, err
	}
	existing, err := queries.ProposedEventByID(ctx, db.ProposedEventByIDParams{
		SessionID: sessionID, ProposalID: pgtype.Text{String: proposal.ProposalID, Valid: true},
		EventType: event.Type, Payload: event.Payload,
	})
	if err == nil {
		if !existing.Matches.Valid || !existing.Matches.Bool {
			return store.ProposedEventReceipt{}, errors.New("conflicting proposed event retry")
		}
		if err := tx.Commit(ctx); err != nil {
			return store.ProposedEventReceipt{}, fmt.Errorf("commit proposed event duplicate: %w", err)
		}
		return proposedEventReceipt(sessionID, proposal.ProposalID, existing.Seq), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.ProposedEventReceipt{}, fmt.Errorf("select proposed event: %w", err)
	}
	latest, err := queries.LatestSessionEventSeq(ctx, sessionID)
	if err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("select proposed event sequence: %w", err)
	}
	row, err := queries.InsertProposedEvent(ctx, db.InsertProposedEventParams{
		SessionID: sessionID, Seq: latest + 1, EventType: event.Type, Payload: event.Payload,
		ProposalID: pgtype.Text{String: proposal.ProposalID, Valid: true}, CreatedAt: pgtype.Timestamptz{Time: event.Time, Valid: true},
		ConnectionEpoch: authority.ConnectionEpoch, CredentialGeneration: authority.CredentialGeneration,
	})
	if err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("insert proposed event: %w", err)
	}
	projection := attentionEventProjection(event)
	storeNow, err := queries.AttentionStoreNow(ctx)
	if err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("read proposed attention Store clock: %w", err)
	}
	if err := projectAttentionEvent(ctx, queries, row.SessionID, row.Seq, projection, storeNow.Time); err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("project proposed attention event seq %d: %w", row.Seq, err)
	}
	if projection.terminal {
		if err := queries.FenceAttentionTerminal(ctx, row.SessionID); err != nil {
			return store.ProposedEventReceipt{}, fmt.Errorf("fence terminal proposed event seq %d: %w", row.Seq, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("commit proposed event: %w", err)
	}
	return proposedEventReceipt(row.SessionID, proposal.ProposalID, row.Seq), nil
}

func (s *Store) WithAdapterConnectionTransaction(ctx context.Context, fn func(store.AdapterConnectionStore) error) error {
	if fn == nil {
		return errors.New("adapter connection transaction callback is nil")
	}
	if s.connectionTx != nil {
		return errors.New("nested adapter connection transaction is not supported")
	}
	if s.pool == nil {
		return errors.New("postgres event store pool is nil")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin adapter connection transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(&Store{connectionTx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit adapter connection transaction: %w", err)
	}
	return nil
}

func (s *Store) InitializeAdapterConnection(ctx context.Context, request store.AdapterConnectionInitialize) (store.AdapterConnection, error) {
	if !validConnectionID(request.SessionID) || request.ActiveCredentialGeneration < 1 {
		return store.AdapterConnection{}, errors.New("invalid adapter connection initialization")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.InitializeAdapterConnection(ctx, db.InitializeAdapterConnectionParams{
		SessionID: request.SessionID, ActiveGeneration: request.ActiveCredentialGeneration,
		ActiveExpiresAt: pgtype.Timestamptz{Time: request.ActiveCredentialExpiresAt, Valid: true},
	})
	if err == nil {
		return adapterConnection(row), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.AdapterConnection{}, fmt.Errorf("initialize adapter connection: %w", err)
	}
	existing, err := queries.MatchingInitialAdapterConnection(ctx, db.MatchingInitialAdapterConnectionParams{
		SessionID: request.SessionID, ActiveGeneration: request.ActiveCredentialGeneration,
		ActiveExpiresAt: pgtype.Timestamptz{Time: request.ActiveCredentialExpiresAt, Valid: true},
	})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("adapter connection initialization conflicts with existing state: %w", err)
	}
	return adapterConnection(existing), nil
}

func (s *Store) RefreshAdapterCredentialBeforeHello(ctx context.Context, sessionID string, refresh store.AdapterCredentialPreHelloRefresh) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || refresh.ExpectedActiveCredentialGeneration < 1 || refresh.ActiveCredentialExpiresAt.IsZero() {
		return store.AdapterConnection{}, errors.New("invalid pre-hello adapter credential refresh")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.RefreshAdapterCredentialBeforeHello(ctx, db.RefreshAdapterCredentialBeforeHelloParams{
		SessionID: sessionID, ExpectedActiveGeneration: refresh.ExpectedActiveCredentialGeneration,
		ActiveExpiresAt: pgtype.Timestamptz{Time: refresh.ActiveCredentialExpiresAt, Valid: true},
	})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("refresh pre-hello adapter credential: %w", err)
	}
	return adapterConnection(db.SessionAdapterConnection(row)), nil
}

func (s *Store) TerminateAdapterConnectionBeforeHello(ctx context.Context, sessionID string, termination store.AdapterConnectionPreHelloTermination) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || termination.ExpectedActiveCredentialGeneration < 1 {
		return store.AdapterConnection{}, errors.New("invalid pre-hello adapter connection termination")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.TerminateAdapterConnectionBeforeHello(ctx, db.TerminateAdapterConnectionBeforeHelloParams{
		SessionID: sessionID, ExpectedActiveGeneration: termination.ExpectedActiveCredentialGeneration,
	})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("terminate pre-hello adapter connection: %w", err)
	}
	return adapterConnection(db.SessionAdapterConnection(row)), nil
}

func (s *Store) AcceptAdapterHello(ctx context.Context, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || hello.CredentialGeneration < 1 {
		return store.AdapterConnection{}, errors.New("invalid adapter hello")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.AcceptAdapterHello(ctx, db.AcceptAdapterHelloParams{SessionID: sessionID, CredentialGeneration: hello.CredentialGeneration})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("accept adapter hello: %w", err)
	}
	return adapterConnection(row), nil
}

func (s *Store) ValidateAdapterAdmission(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || admission.CredentialGeneration < 1 || admission.ConnectionEpoch < 1 ||
		admission.AcceptedFence < 1 || admission.GrantFence <= admission.AcceptedFence {
		return store.AdapterConnection{}, errors.New("invalid adapter admission")
	}
	if s.connectionTx != nil {
		if err := s.connectionTx.QueryRow(ctx, `SELECT 1 FROM session_adapter_connections WHERE session_id=$1 AND active_credential_generation=$2 AND connection_epoch=$3 AND accepted_fence=$4 AND connection_epoch>0 AND accepted_fence>0 AND $5::BIGINT>accepted_fence AND active_credential_expires_at>clock_timestamp() AND revoked_at IS NULL AND terminal_at IS NULL FOR UPDATE`, sessionID, admission.CredentialGeneration, admission.ConnectionEpoch, admission.AcceptedFence, admission.GrantFence).Scan(new(int)); err != nil {
			return store.AdapterConnection{}, fmt.Errorf("validate adapter admission: %w", err)
		}
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.ValidateAdapterAdmission(ctx, db.ValidateAdapterAdmissionParams{
		SessionID: sessionID, CredentialGeneration: admission.CredentialGeneration,
		ConnectionEpoch: admission.ConnectionEpoch, AcceptedFence: admission.AcceptedFence, GrantFence: admission.GrantFence,
	})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("validate adapter admission: %w", err)
	}
	return adapterConnection(row), nil
}

func (s *Store) PrepareAdapterCredentialRotation(ctx context.Context, sessionID string, rotation store.AdapterCredentialRotation) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || rotation.ExpectedActiveCredentialGeneration < 1 || rotation.ExpectedEpoch < 1 ||
		rotation.PendingGeneration < 1 || !validAttachmentText(rotation.RotationID, 255) {
		return store.AdapterConnection{}, errors.New("invalid adapter credential rotation")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.PrepareAdapterCredentialRotation(ctx, db.PrepareAdapterCredentialRotationParams{
		PendingGeneration: pgtype.Int8{Int64: rotation.PendingGeneration, Valid: true}, PendingExpiresAt: pgtype.Timestamptz{Time: rotation.ExpiresAt, Valid: true},
		RotationID: pgtype.Text{String: rotation.RotationID, Valid: true}, SessionID: sessionID,
		ExpectedActiveGeneration: rotation.ExpectedActiveCredentialGeneration, ExpectedEpoch: rotation.ExpectedEpoch,
	})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("prepare adapter credential rotation: %w", err)
	}
	return adapterConnection(row), nil
}

func (s *Store) ActivateAdapterCredential(ctx context.Context, sessionID string, activation store.AdapterCredentialActivation) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || activation.ExpectedActiveCredentialGeneration < 1 || activation.ExpectedEpoch < 1 ||
		activation.PendingGeneration < 1 || !validAttachmentText(activation.RotationID, 255) {
		return store.AdapterConnection{}, errors.New("invalid adapter credential activation")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.ActivateAdapterCredential(ctx, db.ActivateAdapterCredentialParams{
		SessionID: sessionID, ExpectedActiveGeneration: activation.ExpectedActiveCredentialGeneration,
		ExpectedEpoch: activation.ExpectedEpoch, PendingGeneration: pgtype.Int8{Int64: activation.PendingGeneration, Valid: true},
		RotationID: pgtype.Text{String: activation.RotationID, Valid: true},
	})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("activate adapter credential: %w", err)
	}
	return adapterConnection(row), nil
}

func (s *Store) AdapterConnection(ctx context.Context, sessionID string) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) {
		return store.AdapterConnection{}, errors.New("invalid adapter connection session")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.AdapterConnectionByID(ctx, sessionID)
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("select adapter connection: %w", err)
	}
	return adapterConnection(row), nil
}

func (s *Store) adapterConnectionQueries() (*db.Queries, error) {
	if s.connectionTx != nil {
		return db.New(s.connectionTx), nil
	}
	if s.pool == nil {
		return nil, errors.New("postgres event store pool is nil")
	}
	return db.New(s.pool), nil
}

func validConnectionID(value string) bool {
	return len(value) > 0 && len(value) <= 255
}

func adapterConnection(row db.SessionAdapterConnection) store.AdapterConnection {
	return store.AdapterConnection{
		SessionID: row.SessionID, ConnectionEpoch: row.ConnectionEpoch, AcceptedFence: row.AcceptedFence,
		ActiveCredentialGeneration:        row.ActiveCredentialGeneration,
		CredentialGenerationHighWatermark: row.CredentialGenerationHighWatermark,
		ActiveCredentialExpiresAt:         row.ActiveCredentialExpiresAt.Time,
		PendingCredentialGeneration:       int64Pointer(row.PendingCredentialGeneration),
		PendingCredentialExpiresAt:        timePointer(row.PendingCredentialExpiresAt),
		PriorRecoveryGeneration:           int64Pointer(row.PriorRecoveryCredentialGeneration),
		RotationID:                        textPointer(row.RotationID), RevokedAt: timePointer(row.RevokedAt), TerminalAt: timePointer(row.TerminalAt),
	}
}

func int64Pointer(value pgtype.Int8) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

func attentionSummary(row db.SessionAttentionSummary) (store.SessionAttentionSummary, error) {
	if !validConnectionID(row.SessionID) || row.LatestSeq < 0 || row.SummaryVersion < 0 ||
		(row.ProjectionState != store.AttentionProjectionComplete && row.ProjectionState != store.AttentionProjectionIncomplete) {
		return store.SessionAttentionSummary{}, errors.New("attention summary row is invalid")
	}
	summary := store.SessionAttentionSummary{SessionID: row.SessionID, LatestSeq: row.LatestSeq, State: row.State,
		SummaryVersion: row.SummaryVersion, StateOfProjection: row.ProjectionState,
		LastDurableEventAt: timePointer(row.LastDurableEventAt), LastClientCommandAt: timePointer(row.LastClientCommandAt),
		TerminalOutcome: textPointer(row.TerminalOutcome), LatestChangeSeq: int64Pointer(row.LatestChangeSeq)}
	if row.PermissionID.Valid != row.PermissionStatus.Valid || (row.PermissionID.Valid && row.PermissionStatus.String != store.AttentionPermissionPending) {
		return store.SessionAttentionSummary{}, errors.New("attention permission row is invalid")
	}
	if row.PermissionID.Valid {
		summary.Permission = &store.AttentionPermission{ID: row.PermissionID.String, Status: row.PermissionStatus.String}
	}
	if !row.BlockerKind.Valid && (row.BlockerReason.Valid || row.BlockerExpiresAt.Valid || row.BlockingSessionID.Valid || row.BlockerOperation.Valid) {
		return store.SessionAttentionSummary{}, errors.New("attention blocker row is invalid")
	}
	if row.BlockerKind.Valid {
		summary.Blocker = &store.AttentionBlocker{Kind: row.BlockerKind.String, Reason: textPointer(row.BlockerReason),
			ExpiresAt: timePointer(row.BlockerExpiresAt), BlockingSessionID: textPointer(row.BlockingSessionID), Operation: textPointer(row.BlockerOperation)}
	}
	return summary, nil
}

func validateProposedEventInput(sessionID string, authority store.CommandAuthority, proposalID string, event store.PendingEvent) error {
	if sessionID == "" || len(sessionID) > 255 || proposalID == "" || len(proposalID) > 255 ||
		authority.ConnectionEpoch < 1 || authority.CredentialGeneration < 1 || event.Type == "" || !json.Valid(event.Payload) {
		return errors.New("invalid proposed event")
	}
	return nil
}

func proposedEventReceipt(sessionID, proposalID string, seq int64) store.ProposedEventReceipt {
	return store.ProposedEventReceipt{SessionID: sessionID, ProposalID: proposalID, Seq: seq, Status: store.ProposedEventAccepted}
}

func (s *Store) CommitAttachAttempt(ctx context.Context, request store.AttachAttemptRequest) (store.AttachAttemptCommit, error) {
	if s.pool == nil {
		return store.AttachAttemptCommit{}, errors.New("postgres event store pool is nil")
	}
	if err := validateAttachAttempt(request); err != nil {
		return store.AttachAttemptCommit{}, err
	}
	queries := db.New(s.pool)
	storeNow, err := queries.AttachAttemptStoreNow(ctx)
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("read attach attempt Store clock: %w", err)
	}
	if !request.ExpiresAt.After(storeNow.Time) || request.ExpiresAt.After(storeNow.Time.Add(maxAttachAttemptTTL)) {
		return store.AttachAttemptCommit{}, errors.New("attach attempt expiry is outside the Store-clock admission window")
	}
	params := db.InsertAttachAttemptParams{
		AttemptJtiHash: request.Identity.JTIHash[:], AttachID: request.Identity.AttachID,
		BootstrapSessionID: request.Identity.BootstrapSessionID, TargetSessionID: request.Identity.TargetSessionID,
		Provider: request.Identity.Provider, FingerprintDomain: request.Fingerprint.Domain,
		FingerprintVersion: int32(request.Fingerprint.Version), FingerprintDigest: request.Fingerprint.Digest[:],
		FingerprintKeyVersion: int32(request.Fingerprint.KeyVersion), ExpiresAt: pgtype.Timestamptz{Time: request.ExpiresAt, Valid: true},
		AdmissionOutcome: string(request.Outcome), IssuedCredentialGeneration: nullableInt64(request.IssuedCredentialGeneration),
	}
	row, err := queries.InsertAttachAttempt(ctx, params)
	if err == nil {
		return store.AttachAttemptCommit{Attempt: attachAttempt(row)}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.AttachAttemptCommit{}, fmt.Errorf("insert attach attempt: %w", err)
	}
	row, err = queries.AttachAttemptByJTIHash(ctx, request.Identity.JTIHash[:])
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("load existing attach attempt: %w", err)
	}
	current := attachAttempt(row)
	if !sameAttachAttempt(current, request) {
		return store.AttachAttemptCommit{}, errors.New("attach attempt is immutable")
	}
	return store.AttachAttemptCommit{Attempt: current, Duplicate: true}, nil
}

func (s *Store) AttachAttempt(ctx context.Context, jtiHash [32]byte) (store.AttachAttempt, error) {
	if s.pool == nil {
		return store.AttachAttempt{}, errors.New("postgres event store pool is nil")
	}
	if jtiHash == ([32]byte{}) {
		return store.AttachAttempt{}, errors.New("attach attempt JTI hash is required")
	}
	row, err := db.New(s.pool).AttachAttemptByJTIHash(ctx, jtiHash[:])
	if err != nil {
		return store.AttachAttempt{}, fmt.Errorf("load attach attempt: %w", err)
	}
	return attachAttempt(row), nil
}

func validateAttachAttempt(request store.AttachAttemptRequest) error {
	identity, fingerprint := request.Identity, request.Fingerprint
	if identity.JTIHash == ([32]byte{}) || !validConnectionID(identity.AttachID) || !validConnectionID(identity.BootstrapSessionID) ||
		!validConnectionID(identity.TargetSessionID) || identity.BootstrapSessionID == identity.TargetSessionID || identity.Provider == "" ||
		len(identity.Provider) > 128 || fingerprint.Domain != "agentwharf.attach-request.v1" || fingerprint.Version != 1 ||
		fingerprint.Digest == ([32]byte{}) || fingerprint.KeyVersion < 1 || fingerprint.KeyVersion > int64(^uint32(0)>>1) || request.ExpiresAt.IsZero() {
		return errors.New("invalid attach attempt")
	}
	if request.Outcome == store.AttachAttemptAccepted && request.IssuedCredentialGeneration != nil && *request.IssuedCredentialGeneration > 0 {
		return nil
	}
	if request.Outcome == store.AttachAttemptRejected && request.IssuedCredentialGeneration == nil {
		return nil
	}
	return errors.New("invalid attach attempt outcome")
}

func attachAttempt(row db.SessionAttachAttempt) store.AttachAttempt {
	var jtiHash, digest [32]byte
	copy(jtiHash[:], row.AttemptJtiHash)
	copy(digest[:], row.FingerprintDigest)
	return store.AttachAttempt{
		Identity: store.AttachAttemptIdentity{JTIHash: jtiHash, AttachID: row.AttachID, BootstrapSessionID: row.BootstrapSessionID,
			TargetSessionID: row.TargetSessionID, Provider: row.Provider},
		Fingerprint: store.AttachAttemptFingerprint{Domain: row.FingerprintDomain, Version: int64(row.FingerprintVersion),
			Digest: digest, KeyVersion: int64(row.FingerprintKeyVersion)},
		ExpiresAt: row.ExpiresAt.Time, Outcome: store.AttachAttemptOutcome(row.AdmissionOutcome),
		IssuedCredentialGeneration: int64Pointer(row.IssuedCredentialGeneration),
	}
}

func sameAttachAttempt(current store.AttachAttempt, request store.AttachAttemptRequest) bool {
	return current.Identity == request.Identity && current.Fingerprint == request.Fingerprint && current.ExpiresAt.Equal(request.ExpiresAt) &&
		current.Outcome == request.Outcome && ((current.IssuedCredentialGeneration == nil && request.IssuedCredentialGeneration == nil) ||
		(current.IssuedCredentialGeneration != nil && request.IssuedCredentialGeneration != nil && *current.IssuedCredentialGeneration == *request.IssuedCredentialGeneration))
}

func (s *Store) ReserveWorkspaceLease(ctx context.Context, reserve store.WorkspaceLeaseReserve) (store.WorkspaceLease, error) {
	if s.pool == nil {
		return store.WorkspaceLease{}, errors.New("postgres event store pool is nil")
	}
	if err := validateWorkspaceLeaseReserve(reserve); err != nil {
		return store.WorkspaceLease{}, err
	}
	queries := db.New(s.pool)
	params := workspaceLeaseReserveParams(reserve)
	row, err := queries.InsertWorkspaceLease(ctx, params)
	if err == nil {
		return workspaceLease(row)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.WorkspaceLease{}, fmt.Errorf("insert workspace lease: %w", err)
	}
	existing, err := queries.WorkspaceLeaseByKey(ctx, workspaceLeaseKeyText(reserve.Key))
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("read existing workspace lease: %w", err)
	}
	current, err := workspaceLease(existing)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if current.Status == store.WorkspaceLeaseReserved && sameWorkspaceLeaseReserve(current, reserve) {
		return current, nil
	}
	if current.Status != store.WorkspaceLeaseReleased {
		return store.WorkspaceLease{}, errors.New("workspace lease already has a live owner")
	}
	row, err = queries.ReserveReleasedWorkspaceLease(ctx, db.ReserveReleasedWorkspaceLeaseParams{
		WorkerID: params.WorkerID, SessionID: params.SessionID, ConnectionEpoch: params.ConnectionEpoch,
		CredentialGeneration: params.CredentialGeneration, LeaseID: params.LeaseID,
		ChildParentWorkspaceKey: params.ChildParentWorkspaceKey, ChildCapabilityDigest: params.ChildCapabilityDigest,
		ChildScopeExpiresAt: params.ChildScopeExpiresAt, ExpiresAt: params.ExpiresAt, WorkspaceKey: params.WorkspaceKey,
	})
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("reserve released workspace lease: %w", err)
	}
	return workspaceLease(row)
}

func (s *Store) WorkspaceLease(ctx context.Context, key store.WorkspaceLeaseKey) (store.WorkspaceLease, error) {
	if s.pool == nil {
		return store.WorkspaceLease{}, errors.New("postgres event store pool is nil")
	}
	if key == (store.WorkspaceLeaseKey{}) {
		return store.WorkspaceLease{}, errors.New("workspace lease key is required")
	}
	row, err := db.New(s.pool).WorkspaceLeaseByKey(ctx, workspaceLeaseKeyText(key))
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("read workspace lease: %w", err)
	}
	return workspaceLease(row)
}

func (s *Store) RecordWorkspaceStartReceived(ctx context.Context, key store.WorkspaceLeaseKey, expectedVersion int64, owner store.WorkspaceLeaseOwner) (store.WorkspaceLease, error) {
	if s.pool == nil {
		return store.WorkspaceLease{}, errors.New("postgres event store pool is nil")
	}
	if key == (store.WorkspaceLeaseKey{}) || expectedVersion < 1 || !validWorkspaceLeaseOwner(owner) {
		return store.WorkspaceLease{}, errors.New("invalid workspace start receipt")
	}
	row, err := db.New(s.pool).RecordWorkspaceStartReceived(ctx, db.RecordWorkspaceStartReceivedParams{
		WorkspaceKey: workspaceLeaseKeyText(key), ExpectedVersion: expectedVersion, WorkerID: owner.WorkerID,
		SessionID: owner.SessionID, ConnectionEpoch: owner.ConnectionEpoch,
		CredentialGeneration: owner.CredentialGeneration, LeaseID: owner.LeaseID,
	})
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("record workspace start receipt: %w", err)
	}
	return workspaceLease(row)
}

func (s *Store) QuarantineWorkspaceLease(ctx context.Context, key store.WorkspaceLeaseKey, expectedVersion int64) (store.WorkspaceLease, error) {
	if s.pool == nil {
		return store.WorkspaceLease{}, errors.New("postgres event store pool is nil")
	}
	if key == (store.WorkspaceLeaseKey{}) || expectedVersion < 1 {
		return store.WorkspaceLease{}, errors.New("invalid workspace quarantine")
	}
	row, err := db.New(s.pool).QuarantineWorkspaceLease(ctx, db.QuarantineWorkspaceLeaseParams{
		WorkspaceKey: workspaceLeaseKeyText(key), ExpectedVersion: expectedVersion,
	})
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("quarantine workspace lease: %w", err)
	}
	return workspaceLease(row)
}

func (s *Store) ReleaseWorkspaceLeaseAfterQuiescence(ctx context.Context, key store.WorkspaceLeaseKey, expectedVersion int64, owner store.WorkspaceLeaseOwner) (store.WorkspaceLease, error) {
	if s.pool == nil {
		return store.WorkspaceLease{}, errors.New("postgres event store pool is nil")
	}
	if key == (store.WorkspaceLeaseKey{}) || expectedVersion < 1 || !validWorkspaceLeaseOwner(owner) {
		return store.WorkspaceLease{}, errors.New("invalid workspace quiescence release")
	}
	row, err := db.New(s.pool).ReleaseWorkspaceLeaseAfterQuiescence(ctx, db.ReleaseWorkspaceLeaseAfterQuiescenceParams{
		WorkspaceKey: workspaceLeaseKeyText(key), ExpectedVersion: expectedVersion, WorkerID: owner.WorkerID,
		SessionID: owner.SessionID, ConnectionEpoch: owner.ConnectionEpoch,
		CredentialGeneration: owner.CredentialGeneration, LeaseID: owner.LeaseID,
	})
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("release workspace lease after quiescence: %w", err)
	}
	return workspaceLease(row)
}

func validateWorkspaceLeaseReserve(reserve store.WorkspaceLeaseReserve) error {
	if reserve.Key == (store.WorkspaceLeaseKey{}) || !validWorkspaceLeaseOwner(reserve.Owner) || reserve.ExpiresAt.IsZero() {
		return errors.New("invalid workspace lease reserve")
	}
	if scope := reserve.ChildScope; scope != nil {
		if scope.ParentKey == (store.WorkspaceLeaseKey{}) || scope.ParentKey == reserve.Key ||
			scope.CapabilityDigest == ([32]byte{}) || scope.ExpiresAt.IsZero() {
			return errors.New("invalid workspace child scope")
		}
	}
	return nil
}

func validWorkspaceLeaseOwner(owner store.WorkspaceLeaseOwner) bool {
	return validConnectionID(owner.WorkerID) && validConnectionID(owner.SessionID) && validConnectionID(owner.LeaseID) &&
		owner.ConnectionEpoch > 0 && owner.CredentialGeneration > 0
}

func workspaceLeaseReserveParams(reserve store.WorkspaceLeaseReserve) db.InsertWorkspaceLeaseParams {
	params := db.InsertWorkspaceLeaseParams{
		WorkspaceKey: workspaceLeaseKeyText(reserve.Key), WorkerID: reserve.Owner.WorkerID, SessionID: reserve.Owner.SessionID,
		ConnectionEpoch: reserve.Owner.ConnectionEpoch, CredentialGeneration: reserve.Owner.CredentialGeneration,
		LeaseID: reserve.Owner.LeaseID, ExpiresAt: pgtype.Timestamptz{Time: reserve.ExpiresAt, Valid: true},
	}
	if scope := reserve.ChildScope; scope != nil {
		params.ChildParentWorkspaceKey = pgtype.Text{String: workspaceLeaseKeyText(scope.ParentKey), Valid: true}
		params.ChildCapabilityDigest = append([]byte(nil), scope.CapabilityDigest[:]...)
		params.ChildScopeExpiresAt = pgtype.Timestamptz{Time: scope.ExpiresAt, Valid: true}
	}
	return params
}

func workspaceLeaseKeyText(key store.WorkspaceLeaseKey) string {
	return hex.EncodeToString(key[:])
}

func workspaceLease(row db.SessionWorkspaceLease) (store.WorkspaceLease, error) {
	key, err := workspaceLeaseKey(row.WorkspaceKey)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	lease := store.WorkspaceLease{
		Key: key, Owner: store.WorkspaceLeaseOwner{WorkerID: row.WorkerID, SessionID: row.SessionID,
			ConnectionEpoch: row.ConnectionEpoch, CredentialGeneration: row.CredentialGeneration, LeaseID: row.LeaseID},
		Status: store.WorkspaceLeaseStatus(row.Status), Version: row.Version, ExpiresAt: row.ReservationExpiresAt.Time,
	}
	if !row.ChildParentWorkspaceKey.Valid && row.ChildCapabilityDigest == nil && !row.ChildScopeExpiresAt.Valid {
		return lease, nil
	}
	parent, err := workspaceLeaseKey(row.ChildParentWorkspaceKey.String)
	if err != nil || len(row.ChildCapabilityDigest) != 32 || !row.ChildScopeExpiresAt.Valid {
		return store.WorkspaceLease{}, errors.New("invalid durable workspace child scope")
	}
	var digest [32]byte
	copy(digest[:], row.ChildCapabilityDigest)
	lease.ChildScope = &store.WorkspaceLeaseChildScope{ParentKey: parent, CapabilityDigest: digest, ExpiresAt: row.ChildScopeExpiresAt.Time}
	return lease, nil
}

func workspaceLeaseKey(value string) (store.WorkspaceLeaseKey, error) {
	var key store.WorkspaceLeaseKey
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(key) {
		return key, errors.New("invalid durable workspace lease key")
	}
	copy(key[:], decoded)
	return key, nil
}

func sameWorkspaceLeaseReserve(lease store.WorkspaceLease, reserve store.WorkspaceLeaseReserve) bool {
	if lease.Key != reserve.Key || lease.Owner != reserve.Owner || !lease.ExpiresAt.Equal(reserve.ExpiresAt) {
		return false
	}
	left, right := lease.ChildScope, reserve.ChildScope
	return (left == nil && right == nil) || (left != nil && right != nil && left.ParentKey == right.ParentKey &&
		left.CapabilityDigest == right.CapabilityDigest && left.ExpiresAt.Equal(right.ExpiresAt))
}

func nullableInt64(value *int64) pgtype.Int8 {
	if value == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *value, Valid: true}
}

func (s *Store) CreateAttachment(ctx context.Context, request store.AttachmentCreate) (store.AttachmentCommit, error) {
	if s.pool == nil {
		return store.AttachmentCommit{}, errors.New("postgres event store pool is nil")
	}
	if err := validateAttachmentIdentity(request.Identity); err != nil {
		return store.AttachmentCommit{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("begin attachment create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	existing, err := queries.AttachmentByID(ctx, request.Identity.AttachID)
	if err == nil {
		attachment := attachment(existing)
		if attachment.Identity != request.Identity {
			return store.AttachmentCommit{}, errors.New("attachment identity is immutable")
		}
		if err := tx.Commit(ctx); err != nil {
			return store.AttachmentCommit{}, fmt.Errorf("commit attachment no-op: %w", err)
		}
		return store.AttachmentCommit{Attachment: attachment, Summary: attachmentSummary(attachment, nil), Noop: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.AttachmentCommit{}, fmt.Errorf("select attachment: %w", err)
	}
	storeNow, err := queries.AttachmentStoreNow(ctx)
	if err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("read attachment Store clock: %w", err)
	}
	if !validAttachmentExpiry(request.ExpiresAt, storeNow.Time) {
		return store.AttachmentCommit{}, errors.New("attachment expiry is outside the Store-clock delivery window")
	}
	row, err := queries.InsertAttachment(ctx, db.InsertAttachmentParams{
		AttachID: request.Identity.AttachID, BootstrapSessionID: request.Identity.BootstrapSessionID,
		TargetSessionID:            request.Identity.TargetSessionID,
		ExpiresAt:                  pgtype.Timestamptz{Time: request.ExpiresAt, Valid: true},
		TargetCredentialLineageRef: request.Identity.TargetCredentialLineageRef,
	})
	if err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("insert attachment: %w", err)
	}
	created := attachment(row)
	if err := upsertAttentionLedger(ctx, queries, created.Identity.TargetSessionID, attentionBlockerForAttachment(created, nil), nil); err != nil {
		return store.AttachmentCommit{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("commit attachment create: %w", err)
	}
	return store.AttachmentCommit{Attachment: created, Summary: attachmentSummary(created, nil)}, nil
}

func (s *Store) Attachment(ctx context.Context, attachID string) (store.Attachment, error) {
	if s.pool == nil {
		return store.Attachment{}, errors.New("postgres event store pool is nil")
	}
	row, err := db.New(s.pool).AttachmentByID(ctx, attachID)
	if err != nil {
		return store.Attachment{}, fmt.Errorf("select attachment: %w", err)
	}
	return attachment(row), nil
}

func (s *Store) AttachmentForTarget(ctx context.Context, targetSessionID string) (store.Attachment, error) {
	if s.pool == nil {
		return store.Attachment{}, errors.New("postgres event store pool is nil")
	}
	row, err := db.New(s.pool).AttachmentByTarget(ctx, targetSessionID)
	if err != nil {
		return store.Attachment{}, fmt.Errorf("select target attachment: %w", err)
	}
	return attachment(row), nil
}

func (s *Store) UpdateAttachment(ctx context.Context, attachID string, expectedVersion int64, update store.AttachmentUpdate) (store.AttachmentMutation, error) {
	if s.pool == nil {
		return store.AttachmentMutation{}, errors.New("postgres event store pool is nil")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("begin attachment update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	currentRow, err := queries.LockAttachment(ctx, attachID)
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("lock attachment: %w", err)
	}
	current := attachment(currentRow)
	storeNow, err := queries.AttachmentStoreNow(ctx)
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("read attachment update clock: %w", err)
	}
	if current.DeliveryVersion != expectedVersion {
		return store.AttachmentMutation{}, errors.New("stale attachment version")
	}
	if err := validateAttachmentUpdate(current, update, storeNow.Time); err != nil {
		return store.AttachmentMutation{}, err
	}
	row, err := queries.UpdateAttachment(ctx, db.UpdateAttachmentParams{
		Status: string(update.Status), DeliveryState: string(update.DeliveryState),
		QueueReason: textValue(update.QueueReason), ExpiresAt: timeValue(update.ExpiresAt),
		BlockingSessionID: textValue(update.BlockingSessionID), AttachID: attachID,
		ExpectedVersion: expectedVersion,
	})
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("update attachment: %w", err)
	}
	updated := attachment(row)
	if err := upsertAttentionLedger(ctx, queries, updated.Identity.TargetSessionID, attentionBlockerForAttachment(updated, update.Blocker), nil); err != nil {
		return store.AttachmentMutation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("commit attachment update: %w", err)
	}
	return store.AttachmentMutation{Attachment: updated, Summary: attachmentSummary(updated, update.Blocker)}, nil
}

func attentionBlockerForAttachment(attachment store.Attachment, explicit *store.AttachmentBlocker) *store.AttentionBlocker {
	if explicit != nil {
		return &store.AttentionBlocker{Kind: string(explicit.Kind), Reason: explicit.Reason, ExpiresAt: explicit.ExpiresAt,
			BlockingSessionID: explicit.BlockingSessionID, Operation: explicit.Operation}
	}
	if attachment.Status != store.AttachmentJoinPending {
		return nil
	}
	reason := "join_pending"
	return &store.AttentionBlocker{Kind: store.AttentionBlockerQueued, Reason: &reason, ExpiresAt: attachment.ExpiresAt}
}

func validateAttachmentIdentity(identity store.AttachmentIdentity) error {
	if !validAttachmentText(identity.AttachID, 255) ||
		!validAttachmentText(identity.BootstrapSessionID, 255) ||
		!validAttachmentText(identity.TargetSessionID, 255) ||
		!validAttachmentText(identity.TargetCredentialLineageRef, 255) {
		return errors.New("attachment identity is invalid")
	}
	if identity.BootstrapSessionID == identity.TargetSessionID {
		return errors.New("attachment bootstrap and target sessions must differ")
	}
	return nil
}

func validateAttachmentUpdate(current store.Attachment, update store.AttachmentUpdate, storeNow time.Time) error {
	if current.Status == store.AttachmentCanceled ||
		(current.Status == store.AttachmentStartReceived && update.Status != store.AttachmentStartReceived) {
		return errors.New("terminal attachment status cannot be reopened")
	}
	if update.Status == store.AttachmentStartReceived && current.Status != store.AttachmentStartReceived &&
		(current.ExpiresAt == nil || !current.ExpiresAt.After(storeNow)) {
		return errors.New("expired attachment cannot record start receipt")
	}
	if update.ExpiresAt != nil {
		if !validAttachmentExpiry(*update.ExpiresAt, storeNow) {
			return errors.New("attachment expiry is outside the Store-clock delivery window")
		}
		if current.ExpiresAt != nil && update.ExpiresAt.After(*current.ExpiresAt) {
			return errors.New("attachment expiry cannot be extended")
		}
	}

	summaryMatches := func(kind store.AttachmentBlockerKind, reason, expiry, blockingSession, operation bool) bool {
		blocker := update.Blocker
		if blocker == nil || blocker.Kind != kind ||
			(!reason && blocker.Reason != nil) || (!expiry && blocker.ExpiresAt != nil) ||
			(!blockingSession && blocker.BlockingSessionID != nil) || (!operation && blocker.Operation != nil) {
			return false
		}
		if reason && (blocker.Reason == nil || update.QueueReason == nil || *blocker.Reason != *update.QueueReason) {
			return false
		}
		if expiry && (blocker.ExpiresAt == nil || update.ExpiresAt == nil || !blocker.ExpiresAt.Equal(*update.ExpiresAt)) {
			return false
		}
		if blockingSession && (blocker.BlockingSessionID == nil || update.BlockingSessionID == nil || *blocker.BlockingSessionID != *update.BlockingSessionID) {
			return false
		}
		return true
	}

	valid := false
	switch update.Status {
	case store.AttachmentQueued:
		valid = update.DeliveryState == store.AttachmentDeliveryPending &&
			update.QueueReason != nil && validAttachmentText(*update.QueueReason, 128) &&
			update.ExpiresAt != nil && update.BlockingSessionID != nil &&
			validAttachmentText(*update.BlockingSessionID, 255) &&
			*update.BlockingSessionID != current.Identity.TargetSessionID &&
			summaryMatches(store.AttachmentBlockerQueued, true, true, true, false)
	case store.AttachmentStartReceived:
		if update.QueueReason == nil && update.ExpiresAt == nil && update.BlockingSessionID == nil {
			if update.DeliveryState == store.AttachmentDeliveryOutcomeUnknown {
				valid = summaryMatches(store.AttachmentBlockerOutcomeUnknown, false, false, false, true) &&
					validAttachmentOperation(update.Blocker.Operation)
			} else {
				valid = (update.DeliveryState == store.AttachmentDeliveryReceived || update.DeliveryState == store.AttachmentDeliveryCompleted) && update.Blocker == nil
			}
		}
	case store.AttachmentReauthorizationRequired:
		if update.QueueReason == nil && update.ExpiresAt == nil && update.BlockingSessionID == nil {
			if update.DeliveryState == store.AttachmentDeliveryOutcomeUnknown {
				valid = summaryMatches(store.AttachmentBlockerOutcomeUnknown, false, false, false, true) &&
					validAttachmentOperation(update.Blocker.Operation)
			} else {
				valid = update.DeliveryState == store.AttachmentDeliveryPending &&
					summaryMatches(store.AttachmentBlockerReauthorizationRequired, false, false, false, false)
			}
		}
	case store.AttachmentCanceled:
		valid = update.DeliveryState == store.AttachmentDeliveryPending &&
			update.QueueReason == nil && update.ExpiresAt == nil && update.BlockingSessionID == nil &&
			summaryMatches(store.AttachmentBlockerNewRunRequired, false, false, false, false)
	}
	if !valid {
		return errors.New("invalid attachment update")
	}
	return nil
}

func validAttachmentExpiry(expiresAt, storeNow time.Time) bool {
	return expiresAt.After(storeNow) && !expiresAt.After(storeNow.Add(30*time.Second))
}

func validAttachmentText(value string, max int) bool {
	return len(value) > 0 && len(value) <= max
}

func validAttachmentOperation(value *string) bool {
	return value != nil && (*value == "start" || *value == "command")
}

func attachment(row db.SessionAttachment) store.Attachment {
	return store.Attachment{
		Identity: store.AttachmentIdentity{
			AttachID:                   row.AttachID,
			BootstrapSessionID:         row.BootstrapSessionID,
			TargetSessionID:            row.TargetSessionID,
			TargetCredentialLineageRef: row.TargetCredentialLineageRef,
		},
		Status:            store.AttachmentStatus(row.Status),
		DeliveryState:     store.AttachmentDeliveryState(row.DeliveryState),
		DeliveryVersion:   row.DeliveryVersion,
		QueueReason:       textPointer(row.QueueReason),
		ExpiresAt:         timePointer(row.ExpiresAt),
		CanceledAt:        timePointer(row.CanceledAt),
		BlockingSessionID: textPointer(row.BlockingSessionID),
	}
}

func attachmentSummary(attachment store.Attachment, blocker *store.AttachmentBlocker) store.AttachmentSummary {
	return store.AttachmentSummary{
		AttachID:        attachment.Identity.AttachID,
		TargetSessionID: attachment.Identity.TargetSessionID,
		DeliveryVersion: attachment.DeliveryVersion,
		ExpiresAt:       cloneTime(attachment.ExpiresAt),
		Blocker:         cloneAttachmentBlocker(blocker),
	}
}

func textValue(value *string) pgtype.Text {
	if value == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *value, Valid: true}
}

func timeValue(value *time.Time) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *value, Valid: true}
}

func textPointer(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	copy := value.String
	return &copy
}

func timePointer(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	copy := value.Time
	return &copy
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneAttachmentBlocker(value *store.AttachmentBlocker) *store.AttachmentBlocker {
	if value == nil {
		return nil
	}
	copy := *value
	copy.Reason = cloneString(value.Reason)
	copy.ExpiresAt = cloneTime(value.ExpiresAt)
	copy.BlockingSessionID = cloneString(value.BlockingSessionID)
	copy.Operation = cloneString(value.Operation)
	return &copy
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
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
