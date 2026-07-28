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
	"strings"
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

var _ store.SessionAdmissionTruthStore = (*Store)(nil)

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

func (s *Store) SessionAdmissionTruth(ctx context.Context, sessionID string) (store.SessionAdmissionTruth, error) {
	return s.sessionAdmissionTruth(ctx, sessionID, false)
}

func (s *Store) AdapterSessionAdmissionTruth(ctx context.Context, sessionID string) (store.SessionAdmissionTruth, error) {
	return s.sessionAdmissionTruth(ctx, sessionID, true)
}

func (s *Store) sessionAdmissionTruth(ctx context.Context, sessionID string, admitStarting bool) (store.SessionAdmissionTruth, error) {
	truth := store.SessionAdmissionTruth{SessionID: sessionID}
	if !validConnectionID(sessionID) {
		return truth, errors.New("session admission session ID is invalid")
	}
	queries, err := s.adapterConnectionQueries()
	if err != nil {
		return truth, err
	}
	row, err := queries.SessionAdmissionTruth(ctx, sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return truth, nil
	}
	if err != nil {
		return truth, fmt.Errorf("select session admission truth: %w", err)
	}
	// Client admission keeps a preallocated warm target attach-only. Adapter
	// admission is distinct: the first authenticated hello must be able to
	// attach the starting Session before it can emit its ready state.
	if row.Status == "starting" && !admitStarting {
		return truth, nil
	}
	terminal := row.EndedAt.Valid || row.Status == "ended" || row.Status == "error"
	truth.Provider = row.Provider
	truth.Exists = true
	truth.Complete = row.Provider != "" && row.Status != ""
	truth.Terminal = terminal
	truth.Live = !terminal
	return truth, nil
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

func (s *Store) AttentionSummaryPage(ctx context.Context, request store.AttentionSummaryPageRequest) (store.AttentionSummaryPage, error) {
	if s.pool == nil {
		return store.AttentionSummaryPage{}, errors.New("postgres event store pool is nil")
	}
	if request.Limit < 1 || request.Limit > store.MaxAttentionSummaryPageSize {
		return store.AttentionSummaryPage{}, errors.New("attention summary page limit is out of range")
	}
	if request.AfterSessionID != "" && !validConnectionID(request.AfterSessionID) {
		return store.AttentionSummaryPage{}, errors.New("attention summary page cursor is invalid")
	}
	queries := db.New(s.pool)
	snapshot, err := queries.AttentionStoreNow(ctx)
	if err != nil || !snapshot.Valid {
		return store.AttentionSummaryPage{}, errors.New("read attention summary Store clock")
	}
	rows, err := queries.AttentionSummaryPage(ctx, db.AttentionSummaryPageParams{
		AfterSessionID: request.AfterSessionID,
		PageLimit:      int32(request.Limit + 1),
	})
	if err != nil {
		return store.AttentionSummaryPage{}, fmt.Errorf("select attention summary page: %w", err)
	}
	page := store.AttentionSummaryPage{Summaries: make([]store.SessionAttentionSummary, 0, request.Limit), SnapshotAt: snapshot.Time.UTC()}
	for _, row := range rows {
		summary, err := attentionSummary(row)
		if err != nil {
			return store.AttentionSummaryPage{}, err
		}
		if len(page.Summaries) == request.Limit {
			cursor := page.Summaries[len(page.Summaries)-1].SessionID
			page.NextAfterSessionID = &cursor
			break
		}
		page.Summaries = append(page.Summaries, summary)
	}
	return page, nil
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

func (s *Store) PublishSettingsCapability(ctx context.Context, sessionID string, update store.SettingsCapabilityUpdate) (store.SettingsCapability, error) {
	if s.pool == nil || !validConnectionID(sessionID) || !validPostgresSettingsCapability(update) {
		return store.SettingsCapability{}, errors.New("invalid settings capability")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.SettingsCapability{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	authority := store.CommandAuthority{ConnectionEpoch: update.Writer.ConnectionEpoch, CredentialGeneration: update.Writer.CredentialGeneration}
	if err := lockCommandAuthority(ctx, queries, sessionID, authority); err != nil {
		return store.SettingsCapability{}, err
	}
	if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, update.Writer); err != nil {
		return store.SettingsCapability{}, err
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.SettingsCapability{}, fmt.Errorf("lock settings capability stream: %w", err)
	}
	if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
		return store.SettingsCapability{}, err
	}
	if err := verifyPostgresSettingsCapabilityEvent(ctx, tx, sessionID, update.EventSeq); err != nil {
		return store.SettingsCapability{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_settings_capabilities (session_id,capability_event_seq,fingerprint,effective_model_id,effective_permission_mode_id,capability_version,writer_connection_epoch,writer_credential_generation,writer_lease_id) VALUES ($1,$2,$3,$4,$5,1,$6,$7,$8) ON CONFLICT (session_id) DO UPDATE SET capability_event_seq=EXCLUDED.capability_event_seq,fingerprint=EXCLUDED.fingerprint,effective_model_id=EXCLUDED.effective_model_id,effective_permission_mode_id=EXCLUDED.effective_permission_mode_id,capability_version=session_settings_capabilities.capability_version+1,writer_connection_epoch=EXCLUDED.writer_connection_epoch,writer_credential_generation=EXCLUDED.writer_credential_generation,writer_lease_id=EXCLUDED.writer_lease_id,updated_at=statement_timestamp()`, sessionID, update.EventSeq, update.Fingerprint, update.EffectiveModelID, update.EffectivePermissionModeID, update.Writer.ConnectionEpoch, update.Writer.CredentialGeneration, update.Writer.LeaseID); err != nil {
		return store.SettingsCapability{}, err
	}
	result, err := queryPostgresSettingsCapability(ctx, tx, sessionID, false)
	if err != nil {
		return store.SettingsCapability{}, err
	}
	return result, tx.Commit(ctx)
}

func validPostgresSettingsCapability(update store.SettingsCapabilityUpdate) bool {
	return update.EventSeq > 0 && validPostgresSettingsFingerprint(update.Fingerprint) && validPostgresSettingsID(update.EffectiveModelID) && validPostgresSettingsID(update.EffectivePermissionModeID) && validPostgresSettingsWriter(update.Writer)
}

func (s *Store) SettingsCommandReserve(ctx context.Context, sessionID string, request store.SettingsCommandRequest) (store.SettingsCommandReserve, error) {
	if s.pool == nil || !validPostgresSettingsRequest(sessionID, request) {
		return store.SettingsCommandReserve{}, errors.New("invalid settings command reservation")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("begin settings reservation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	authority := store.CommandAuthority{ConnectionEpoch: request.Writer.ConnectionEpoch, CredentialGeneration: request.Writer.CredentialGeneration}
	if err := lockCommandAuthority(ctx, queries, sessionID, authority); err != nil {
		return store.SettingsCommandReserve{}, err
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("lock settings reservation stream: %w", err)
	}
	if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
		return store.SettingsCommandReserve{}, err
	}
	if err := validatePostgresCurrentSettingsWriter(ctx, tx, sessionID, request.Writer); err != nil {
		return store.SettingsCommandReserve{}, err
	}
	existing, err := queryPostgresSettingsCommand(ctx, tx, sessionID, request.CommandID, true)
	if err == nil {
		if existing.RequestFingerprint != request.RequestFingerprint || !samePostgresSettingsOptionalID(existing.RequestedModelID, request.RequestedModelID) || !samePostgresSettingsOptionalID(existing.RequestedPermissionModeID, request.RequestedPermissionModeID) {
			return store.SettingsCommandReserve{}, errors.New("settings command ID is reused")
		}
		if err := tx.Commit(ctx); err != nil {
			return store.SettingsCommandReserve{}, fmt.Errorf("commit duplicate settings reservation: %w", err)
		}
		return store.SettingsCommandReserve{Command: existing, Duplicate: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.SettingsCommandReserve{}, err
	}
	capability, err := queryPostgresSettingsCapability(ctx, tx, sessionID, true)
	if err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("select settings capability: %w", err)
	}
	if capability.Fingerprint != request.RequestFingerprint || capability.Writer != request.Writer {
		return store.SettingsCommandReserve{}, errors.New("settings capability is stale or writer is fenced")
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.SettingsCommandReserve{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO session_settings_commands (session_id,cmd_id,request_fingerprint,requested_model_id,requested_permission_mode_id,reservation_version,delivery_deadline,writer_connection_epoch,writer_credential_generation,writer_lease_id,reserved_capability_event_seq,reserved_fingerprint,reserved_effective_model_id,reserved_effective_permission_mode_id,status) VALUES ($1,$2,$3,$4,$5,1,$6,$7,$8,$9,$10,$11,$12,$13,'delivery_pending')`, sessionID, request.CommandID, request.RequestFingerprint, request.RequestedModelID, request.RequestedPermissionModeID, now.Add(5*time.Second), request.Writer.ConnectionEpoch, request.Writer.CredentialGeneration, request.Writer.LeaseID, capability.EventSeq, capability.Fingerprint, capability.EffectiveModelID, capability.EffectivePermissionModeID)
	if err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("insert settings reservation: %w", err)
	}
	command, err := queryPostgresSettingsCommand(ctx, tx, sessionID, request.CommandID, false)
	if err != nil {
		return store.SettingsCommandReserve{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("commit settings reservation: %w", err)
	}
	return store.SettingsCommandReserve{Command: command}, nil
}

func (s *Store) AcknowledgeSettingsCommandDelivery(ctx context.Context, sessionID, commandID string, reservationVersion int64, writer store.SettingsWriter) (store.SettingsCommand, error) {
	if s.pool == nil || !validConnectionID(sessionID) || commandID == "" || reservationVersion < 1 || !validPostgresSettingsWriter(writer) {
		return store.SettingsCommand{}, errors.New("invalid settings delivery acknowledgement")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.SettingsCommand{}, fmt.Errorf("begin settings delivery transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	authority := store.CommandAuthority{ConnectionEpoch: writer.ConnectionEpoch, CredentialGeneration: writer.CredentialGeneration}
	if err := lockCommandAuthority(ctx, queries, sessionID, authority); err != nil {
		return store.SettingsCommand{}, err
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("lock settings delivery stream: %w", err)
	}
	if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
		return store.SettingsCommand{}, err
	}
	if err := validatePostgresCurrentSettingsWriter(ctx, tx, sessionID, writer); err != nil {
		return store.SettingsCommand{}, err
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE session_settings_commands SET status='pending', operation_deadline=$1, updated_at=clock_timestamp() WHERE session_id=$2 AND cmd_id=$3 AND reservation_version=$4 AND status='delivery_pending' AND delivery_deadline>$5 AND writer_connection_epoch=$6 AND writer_credential_generation=$7 AND writer_lease_id=$8`, now.Add(30*time.Second), sessionID, commandID, reservationVersion, now, writer.ConnectionEpoch, writer.CredentialGeneration, writer.LeaseID)
	if err != nil || result.RowsAffected() != 1 {
		return store.SettingsCommand{}, errors.New("settings delivery acknowledgement is fenced")
	}
	command, err := queryPostgresSettingsCommand(ctx, tx, sessionID, commandID, false)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("commit settings delivery acknowledgement: %w", err)
	}
	return command, nil
}

func (s *Store) RecoverSettingsCommand(ctx context.Context, sessionID, commandID string, priorWriter store.SettingsWriter) (store.SettingsCommand, error) {
	if s.pool == nil || !validConnectionID(sessionID) || commandID == "" || !validPostgresSettingsWriter(priorWriter) {
		return store.SettingsCommand{}, errors.New("invalid settings recovery")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.SettingsCommand{}, fmt.Errorf("begin settings recovery transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("lock settings recovery stream: %w", err)
	}
	command, err := queryPostgresSettingsCommand(ctx, tx, sessionID, commandID, true)
	if err != nil || command.Writer != priorWriter {
		return store.SettingsCommand{}, errors.New("settings recovery lost writer fence")
	}
	if command.Status == store.SettingsCommandDeliveryPending {
		now, err := postgresSettingsNow(ctx, tx)
		if err != nil || command.DeliveryDeadline.After(now) {
			return store.SettingsCommand{}, errors.New("settings delivery deadline has not elapsed")
		}
		reason := "adapter_delivery_failed"
		payload, err := store.SettingsTerminalEventPayload(command, command.ReservedCapability, store.SettingsCommandRejected, &reason)
		if err != nil {
			return store.SettingsCommand{}, err
		}
		seq, _, err := appendEventsLocked(ctx, queries, sessionID, []store.PendingEvent{{Type: "session.settings.effective", Time: now, Payload: payload}})
		if err != nil {
			return store.SettingsCommand{}, err
		}
		result, err := tx.Exec(ctx, `UPDATE session_settings_commands SET status='rejected',terminal_event_seq=$1,updated_at=clock_timestamp() WHERE session_id=$2 AND cmd_id=$3 AND status='delivery_pending' AND delivery_deadline<=$4 AND terminal_event_seq IS NULL`, seq, sessionID, commandID, now)
		if err != nil || result.RowsAffected() != 1 {
			return store.SettingsCommand{}, errors.New("settings delivery deadline finalization lost race")
		}
		command, err = queryPostgresSettingsCommand(ctx, tx, sessionID, commandID, false)
		if err != nil {
			return store.SettingsCommand{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return store.SettingsCommand{}, err
		}
		return command, nil
	}
	if command.Status != store.SettingsCommandPending {
		return store.SettingsCommand{}, errors.New("settings recovery requires a pending command")
	}
	capability, err := queryPostgresSettingsCapability(ctx, tx, sessionID, true)
	if err != nil || capability.Writer == priorWriter || capability.EventSeq <= command.ReservedCapability.EventSeq {
		return store.SettingsCommand{}, errors.New("settings recovery requires a fresh replacement writer")
	}
	if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, capability.Writer); err != nil {
		return store.SettingsCommand{}, errors.New("settings recovery replacement writer is not live")
	}
	result, err := tx.Exec(ctx, `UPDATE session_settings_commands SET status='recovery_pending', updated_at=clock_timestamp() WHERE session_id=$1 AND cmd_id=$2 AND status='pending' AND writer_connection_epoch=$3 AND writer_credential_generation=$4 AND writer_lease_id=$5`, sessionID, commandID, priorWriter.ConnectionEpoch, priorWriter.CredentialGeneration, priorWriter.LeaseID)
	if err != nil || result.RowsAffected() != 1 {
		return store.SettingsCommand{}, errors.New("settings recovery lost writer fence")
	}
	command, err = queryPostgresSettingsCommand(ctx, tx, sessionID, commandID, false)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("commit settings recovery: %w", err)
	}
	return command, nil
}

func (s *Store) FinalizeSettingsCommand(ctx context.Context, sessionID, commandID string, finalize store.SettingsCommandFinalize) (store.SettingsCommand, error) {
	if s.pool == nil || !validPostgresSettingsFinalize(sessionID, commandID, finalize) {
		return store.SettingsCommand{}, errors.New("invalid settings finalization")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.SettingsCommand{}, fmt.Errorf("begin settings finalization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if finalize.Writer != nil {
		authority := store.CommandAuthority{ConnectionEpoch: finalize.Writer.ConnectionEpoch, CredentialGeneration: finalize.Writer.CredentialGeneration}
		if err := lockCommandAuthority(ctx, queries, sessionID, authority); err != nil {
			return store.SettingsCommand{}, err
		}
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("lock settings finalization stream: %w", err)
	}
	if finalize.Writer != nil {
		authority := store.CommandAuthority{ConnectionEpoch: finalize.Writer.ConnectionEpoch, CredentialGeneration: finalize.Writer.CredentialGeneration}
		if err := validateCommandAuthorityCurrent(ctx, queries, sessionID, authority); err != nil {
			return store.SettingsCommand{}, err
		}
		if err := validatePostgresCurrentSettingsWriter(ctx, tx, sessionID, *finalize.Writer); err != nil {
			return store.SettingsCommand{}, err
		}
	}
	command, err := queryPostgresSettingsCommand(ctx, tx, sessionID, commandID, true)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if command.ReservationVersion != finalize.ReservationVersion || command.Status != finalize.ExpectedStatus || (finalize.Writer != nil && command.Writer != *finalize.Writer) {
		return store.SettingsCommand{}, errors.New("settings finalization is fenced")
	}
	capability, err := queryPostgresSettingsCapability(ctx, tx, sessionID, true)
	if err != nil || !samePostgresSettingsCapability(capability, finalize.EffectiveCapability) {
		return store.SettingsCommand{}, errors.New("effective settings capability is not current")
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := validatePostgresSettingsFinalization(command, capability, finalize, now); err != nil {
		return store.SettingsCommand{}, err
	}
	payload, err := store.SettingsTerminalEventPayload(command, capability, finalize.Outcome, finalize.ReasonCode)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	seq, _, err := appendEventsLocked(ctx, queries, sessionID, []store.PendingEvent{{Type: "session.settings.effective", Time: now, Payload: payload}})
	if err != nil {
		return store.SettingsCommand{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE session_settings_commands SET status=$1, terminal_event_seq=$2, updated_at=clock_timestamp() WHERE session_id=$3 AND cmd_id=$4 AND reservation_version=$5 AND status=$6 AND terminal_event_seq IS NULL`, finalize.Outcome, seq, sessionID, commandID, finalize.ReservationVersion, finalize.ExpectedStatus)
	if err != nil || result.RowsAffected() != 1 {
		return store.SettingsCommand{}, errors.New("settings finalization lost race")
	}
	command, err = queryPostgresSettingsCommand(ctx, tx, sessionID, commandID, false)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("commit settings finalization: %w", err)
	}
	return command, nil
}

func (s *Store) SettingsCommand(ctx context.Context, sessionID, commandID string) (store.SettingsCommand, error) {
	if s.pool == nil || !validConnectionID(sessionID) || commandID == "" {
		return store.SettingsCommand{}, errors.New("invalid settings command lookup")
	}
	return queryPostgresSettingsCommand(ctx, s.pool, sessionID, commandID, false)
}

func (s *Store) PendingSettingsCommands(ctx context.Context, sessionID string) ([]store.SettingsCommand, error) {
	if s.pool == nil || !validConnectionID(sessionID) {
		return nil, errors.New("invalid settings pending Session")
	}
	rows, err := s.pool.Query(ctx, `SELECT cmd_id FROM session_settings_commands WHERE session_id=$1 AND status IN ('delivery_pending','pending','recovery_pending') ORDER BY reservation_version`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	commands := []store.SettingsCommand{}
	for rows.Next() {
		var commandID string
		if err := rows.Scan(&commandID); err != nil {
			return nil, err
		}
		command, err := queryPostgresSettingsCommand(ctx, s.pool, sessionID, commandID, false)
		if err != nil {
			return nil, err
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return commands, nil
}

func (s *Store) PublishRunControlCapability(ctx context.Context, sessionID string, update store.RunControlCapabilityUpdate) (store.RunControlCapability, error) {
	if !validPostgresRunControlCapabilityUpdate(sessionID, update) {
		return store.RunControlCapability{}, errors.New("invalid run-control capability")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.RunControlCapability{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.RunControlCapability{}, err
	}
	if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, update.Writer); err != nil {
		return store.RunControlCapability{}, err
	}
	if err := verifyPostgresRunControlCapabilityEvent(ctx, tx, sessionID, update.EventSeq); err != nil {
		return store.RunControlCapability{}, err
	}
	result, err := tx.Exec(ctx, `INSERT INTO session_run_control_capabilities (session_id,capability_event_seq,capability_version,interrupt_supported,stop_supported,writer_connection_epoch,writer_credential_generation,writer_lease_id) VALUES ($1,$2,$2,$3,$4,$5,$6,$7) ON CONFLICT(session_id) DO UPDATE SET capability_event_seq=EXCLUDED.capability_event_seq,capability_version=EXCLUDED.capability_version,interrupt_supported=EXCLUDED.interrupt_supported,stop_supported=EXCLUDED.stop_supported,writer_connection_epoch=EXCLUDED.writer_connection_epoch,writer_credential_generation=EXCLUDED.writer_credential_generation,writer_lease_id=EXCLUDED.writer_lease_id,updated_at=statement_timestamp() WHERE session_run_control_capabilities.capability_event_seq < EXCLUDED.capability_event_seq`, sessionID, update.EventSeq, update.InterruptSupported, update.StopSupported, update.Writer.ConnectionEpoch, update.Writer.CredentialGeneration, update.Writer.LeaseID)
	if err != nil {
		return store.RunControlCapability{}, err
	}
	if result.RowsAffected() != 1 {
		return store.RunControlCapability{}, errors.New("run-control capability event is stale")
	}
	capability, err := queryPostgresRunControlCapability(ctx, tx, sessionID, false)
	if err != nil {
		return store.RunControlCapability{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.RunControlCapability{}, err
	}
	return capability, nil
}

func (s *Store) RunControlReserve(ctx context.Context, sessionID string, request store.RunControlRequest) (store.RunControlReserve, error) {
	if !validPostgresRunControlRequest(sessionID, request) {
		return store.RunControlReserve{}, errors.New("invalid run-control reservation")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.RunControlReserve{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.RunControlReserve{}, err
	}
	if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, request.Writer); err != nil {
		return store.RunControlReserve{}, err
	}
	existing, err := queryPostgresRunControlReservation(ctx, tx, sessionID, request.CommandID, true)
	if err == nil {
		if existing.Operation != request.Operation {
			return store.RunControlReserve{}, errors.New("run-control command ID is reused")
		}
		if err := tx.Commit(ctx); err != nil {
			return store.RunControlReserve{}, err
		}
		return store.RunControlReserve{Reservation: existing, Duplicate: true}, nil
	}
	if err != nil && err != pgx.ErrNoRows {
		return store.RunControlReserve{}, err
	}
	capability, err := queryPostgresRunControlCapability(ctx, tx, sessionID, true)
	if err != nil || capability.Writer != request.Writer {
		return store.RunControlReserve{}, errors.New("run-control capability is stale or writer is fenced")
	}
	if (request.Operation == store.RunControlInterrupt && !capability.InterruptSupported) || (request.Operation == store.RunControlStop && !capability.StopSupported) {
		return store.RunControlReserve{}, errors.New("run-control operation is unsupported")
	}
	if err := validatePostgresRunControlPreState(ctx, tx, sessionID, request); err != nil {
		return store.RunControlReserve{}, err
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.RunControlReserve{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_run_controls (session_id,cmd_id,operation,capability_version,reservation_version,pre_control_state,pre_control_state_seq,writer_connection_epoch,writer_credential_generation,writer_lease_id,deadline,status) VALUES ($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10,'pending')`, sessionID, request.CommandID, request.Operation, capability.Version, request.PreControlState, request.PreControlStateSeq, request.Writer.ConnectionEpoch, request.Writer.CredentialGeneration, request.Writer.LeaseID, now.Add(30*time.Second)); err != nil {
		return store.RunControlReserve{}, err
	}
	reservation, err := queryPostgresRunControlReservation(ctx, tx, sessionID, request.CommandID, false)
	if err != nil {
		return store.RunControlReserve{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.RunControlReserve{}, err
	}
	return store.RunControlReserve{Reservation: reservation}, nil
}

func (s *Store) RunControlFinalize(ctx context.Context, sessionID, commandID string, finalize store.RunControlFinalize) (store.RunControlReservation, error) {
	if !validConnectionID(sessionID) || commandID == "" || finalize.ReservationVersion < 1 || !validPostgresRunControlTerminalOutcome(finalize.Outcome) || finalize.Outcome == store.RunControlUnsupported || (finalize.Outcome == store.RunControlCompleted && finalize.ReasonCode != nil) || (finalize.Outcome != store.RunControlCompleted && !validPostgresSettingsReason(finalize.ReasonCode)) {
		return store.RunControlReservation{}, errors.New("invalid run-control finalization")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.RunControlReservation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.RunControlReservation{}, err
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	reservation, err := queryPostgresRunControlReservation(ctx, tx, sessionID, commandID, true)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	if reservation.ReservationVersion != finalize.ReservationVersion || reservation.Outcome != store.RunControlPending {
		return store.RunControlReservation{}, errors.New("run-control finalization is fenced")
	}
	if finalize.Outcome == store.RunControlCompleted || finalize.Outcome == store.RunControlRejected {
		if finalize.Writer == nil || *finalize.Writer != reservation.Writer || !reservation.Deadline.After(now) {
			return store.RunControlReservation{}, errors.New("run-control writer finalization is fenced")
		}
		capability, err := queryPostgresRunControlCapability(ctx, tx, sessionID, true)
		if err != nil || capability.Version != reservation.CapabilityVersion || capability.Writer != *finalize.Writer {
			return store.RunControlReservation{}, errors.New("run-control capability changed")
		}
		if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, *finalize.Writer); err != nil {
			return store.RunControlReservation{}, err
		}
	} else if finalize.Writer != nil || (finalize.Outcome == store.RunControlTimeout && reservation.Deadline.After(now)) {
		return store.RunControlReservation{}, errors.New("run-control unbound finalization is fenced")
	}
	if finalize.Outcome == store.RunControlCompleted {
		if err := validatePostgresRunControlPreState(ctx, tx, sessionID, store.RunControlRequest{Operation: reservation.Operation, PreControlState: reservation.PreControlState, PreControlStateSeq: reservation.PreControlStateSeq}); err != nil {
			return store.RunControlReservation{}, err
		}
		payload := `{"state":"ready"}`
		if reservation.Operation == store.RunControlStop {
			payload = `{"state":"ended","reason":"user_stop"}`
		}
		if _, err := appendPostgresRunControlEvent(ctx, tx, sessionID, "session.state", []byte(payload), now); err != nil {
			return store.RunControlReservation{}, err
		}
	}
	completionState := (*string)(nil)
	if finalize.Outcome == store.RunControlCompleted {
		value := "ready"
		if reservation.Operation == store.RunControlStop {
			value = "ended"
		}
		completionState = &value
	}
	payload, err := json.Marshal(struct {
		CommandID       string                    `json:"cmd_id"`
		Operation       store.RunControlOperation `json:"operation"`
		Outcome         store.RunControlOutcome   `json:"outcome"`
		CompletionState *string                   `json:"completion_state"`
		ReasonCode      *string                   `json:"reason_code"`
	}{reservation.CommandID, reservation.Operation, finalize.Outcome, completionState, finalize.ReasonCode})
	if err != nil {
		return store.RunControlReservation{}, err
	}
	terminalSeq, err := appendPostgresRunControlEvent(ctx, tx, sessionID, "session.run.outcome", payload, now)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE session_run_controls SET status=$1,terminal_event_seq=$2,updated_at=clock_timestamp() WHERE session_id=$3 AND cmd_id=$4 AND reservation_version=$5 AND status='pending' AND terminal_event_seq IS NULL`, finalize.Outcome, terminalSeq, sessionID, commandID, finalize.ReservationVersion)
	if err != nil || result.RowsAffected() != 1 {
		return store.RunControlReservation{}, errors.New("run-control finalization lost race")
	}
	reservation, err = queryPostgresRunControlReservation(ctx, tx, sessionID, commandID, false)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.RunControlReservation{}, err
	}
	return reservation, nil
}

func (s *Store) RecoverRunControl(ctx context.Context, sessionID, commandID, reason string) (store.RunControlReservation, error) {
	if !validConnectionID(sessionID) || commandID == "" || (reason != "adapter_disconnected" && reason != "recovery_unconfirmed") {
		return store.RunControlReservation{}, errors.New("invalid run-control recovery")
	}
	reservation, err := s.RunControl(ctx, sessionID, commandID)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	return s.RunControlFinalize(ctx, sessionID, commandID, store.RunControlFinalize{ReservationVersion: reservation.ReservationVersion, Outcome: store.RunControlOutcomeUnknown, ReasonCode: &reason})
}

func (s *Store) RunControl(ctx context.Context, sessionID, commandID string) (store.RunControlReservation, error) {
	return queryPostgresRunControlReservation(ctx, s.pool, sessionID, commandID, false)
}
func (s *Store) PendingRunControls(ctx context.Context, sessionID string) ([]store.RunControlReservation, error) {
	rows, err := s.pool.Query(ctx, `SELECT cmd_id FROM session_run_controls WHERE session_id=$1 AND status='pending' ORDER BY reservation_version`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var commandIDs []string
	for rows.Next() {
		var commandID string
		if err := rows.Scan(&commandID); err != nil {
			return nil, err
		}
		commandIDs = append(commandIDs, commandID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	reservations := make([]store.RunControlReservation, 0, len(commandIDs))
	for _, commandID := range commandIDs {
		item, err := s.RunControl(ctx, sessionID, commandID)
		if err != nil {
			return nil, err
		}
		reservations = append(reservations, item)
	}
	return reservations, nil
}

func (s *Store) PublishFileReferenceCapability(ctx context.Context, sessionID string, update store.FileReferenceCapabilityUpdate) (store.FileReferenceCapability, error) {
	if !validPostgresFileReferenceCapabilityUpdate(sessionID, update) {
		return store.FileReferenceCapability{}, errors.New("invalid file-reference capability")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.FileReferenceCapability{}, err
	}
	defer tx.Rollback(ctx)
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.FileReferenceCapability{}, err
	}
	if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, update.Writer); err != nil {
		return store.FileReferenceCapability{}, err
	}
	var eventType string
	if err := tx.QueryRow(ctx, `SELECT type FROM session_events WHERE session_id=$1 AND seq=$2`, sessionID, update.EventSeq).Scan(&eventType); err != nil || eventType != "session.file_references.capabilities" {
		return store.FileReferenceCapability{}, errors.New("file-reference capability event is invalid")
	}
	result, err := tx.Exec(ctx, `INSERT INTO session_file_reference_capabilities (session_id,capability_event_seq,capability_fingerprint,writer_connection_epoch,writer_credential_generation,writer_lease_id) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(session_id) DO UPDATE SET capability_event_seq=EXCLUDED.capability_event_seq,capability_fingerprint=EXCLUDED.capability_fingerprint,writer_connection_epoch=EXCLUDED.writer_connection_epoch,writer_credential_generation=EXCLUDED.writer_credential_generation,writer_lease_id=EXCLUDED.writer_lease_id,updated_at=clock_timestamp() WHERE session_file_reference_capabilities.capability_event_seq < EXCLUDED.capability_event_seq`, sessionID, update.EventSeq, update.Fingerprint, update.Writer.ConnectionEpoch, update.Writer.CredentialGeneration, update.Writer.LeaseID)
	if err != nil || result.RowsAffected() != 1 {
		return store.FileReferenceCapability{}, errors.New("file-reference capability is stale")
	}
	capability, err := queryPostgresFileReferenceCapability(ctx, tx, sessionID, false)
	if err != nil {
		return store.FileReferenceCapability{}, err
	}
	return capability, tx.Commit(ctx)
}

func (s *Store) CommitFileReferenceCommand(ctx context.Context, sessionID string, message store.PendingEvent, request store.FileReferenceCommandRequest) (store.FileReferenceCommandReserve, error) {
	if !validPostgresFileReferenceRequest(sessionID, request) || message.Type != "session.message" || !json.Valid(message.Payload) {
		return store.FileReferenceCommandReserve{}, errors.New("invalid file-reference command")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	defer tx.Rollback(ctx)
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	existing, err := queryPostgresFileReferenceCommand(ctx, tx, sessionID, request.CommandID, true)
	if err == nil {
		if existing.MessageID != request.MessageID || existing.CapabilityFingerprint != request.CapabilityFingerprint || existing.RequestFingerprint != request.RequestFingerprint || existing.ReferenceCount != request.ReferenceCount {
			return store.FileReferenceCommandReserve{}, errors.New("file-reference command ID is reused")
		}
		return store.FileReferenceCommandReserve{Command: existing, Duplicate: true}, tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return store.FileReferenceCommandReserve{}, err
	}
	capability, err := queryPostgresFileReferenceCapability(ctx, tx, sessionID, true)
	if err != nil || capability.Fingerprint != request.CapabilityFingerprint || validatePostgresLiveSettingsWriter(ctx, tx, sessionID, capability.Writer) != nil {
		return store.FileReferenceCommandReserve{}, errors.New("file-reference capability is stale")
	}
	if _, err := appendPostgresRunControlEvent(ctx, tx, sessionID, message.Type, message.Payload, message.Time); err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_file_reference_commands (session_id,cmd_id,message_id,capability_fingerprint,request_fingerprint,reference_count,reservation_version,delivery_deadline,status) VALUES ($1,$2,$3,$4,$5,$6,1,$7,'delivery_pending')`, sessionID, request.CommandID, request.MessageID, request.CapabilityFingerprint, request.RequestFingerprint, request.ReferenceCount, now.Add(10*time.Minute)); err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	command, err := queryPostgresFileReferenceCommand(ctx, tx, sessionID, request.CommandID, false)
	if err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	return store.FileReferenceCommandReserve{Command: command}, tx.Commit(ctx)
}

func (s *Store) AcknowledgeFileReferenceDelivery(ctx context.Context, sessionID, commandID string, version int64, writer store.FileReferenceWriter) (store.FileReferenceCommand, error) {
	if !validConnectionID(sessionID) || !validPostgresSettingsWriter(writer) || version < 1 {
		return store.FileReferenceCommand{}, errors.New("invalid file-reference delivery")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	defer tx.Rollback(ctx)
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.FileReferenceCommand{}, err
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, writer); err != nil {
		return store.FileReferenceCommand{}, err
	}
	capability, err := queryPostgresFileReferenceCapability(ctx, tx, sessionID, true)
	if err != nil || capability.Writer != writer {
		return store.FileReferenceCommand{}, errors.New("file-reference writer is fenced")
	}
	result, err := tx.Exec(ctx, `UPDATE session_file_reference_commands SET writer_connection_epoch=$1,writer_credential_generation=$2,writer_lease_id=$3,status='pending',updated_at=clock_timestamp() WHERE session_id=$4 AND cmd_id=$5 AND reservation_version=$6 AND status='delivery_pending' AND delivery_deadline>$7`, writer.ConnectionEpoch, writer.CredentialGeneration, writer.LeaseID, sessionID, commandID, version, now)
	if err != nil || result.RowsAffected() != 1 {
		return store.FileReferenceCommand{}, errors.New("file-reference delivery is fenced")
	}
	command, err := queryPostgresFileReferenceCommand(ctx, tx, sessionID, commandID, false)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	return command, tx.Commit(ctx)
}

func (s *Store) FinalizeFileReferenceCommand(ctx context.Context, sessionID, commandID string, finalize store.FileReferenceCommandFinalize) (store.FileReferenceCommand, error) {
	if !validConnectionID(sessionID) || commandID == "" || finalize.ReservationVersion < 1 || (finalize.Outcome != store.FileReferenceDelivered && finalize.Outcome != store.FileReferenceRejected && finalize.Outcome != store.FileReferenceOutcomeUnknown) {
		return store.FileReferenceCommand{}, errors.New("invalid file-reference finalization")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	defer tx.Rollback(ctx)
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.FileReferenceCommand{}, err
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	command, err := queryPostgresFileReferenceCommand(ctx, tx, sessionID, commandID, true)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if command.ReservationVersion != finalize.ReservationVersion || command.Status != store.FileReferencePending || !store.ValidFileReferenceTerminal(finalize.Outcome, command.ReferenceCount, finalize.ReasonCode, finalize.ReferenceIndex) {
		return store.FileReferenceCommand{}, errors.New("file-reference finalization is fenced")
	}
	if finalize.Outcome != store.FileReferenceOutcomeUnknown {
		if finalize.Writer == nil || command.Writer == nil || *finalize.Writer != *command.Writer {
			return store.FileReferenceCommand{}, errors.New("file-reference writer finalization is fenced")
		}
		if err := validatePostgresLiveSettingsWriter(ctx, tx, sessionID, *finalize.Writer); err != nil {
			return store.FileReferenceCommand{}, err
		}
		capability, err := queryPostgresFileReferenceCapability(ctx, tx, sessionID, true)
		if err != nil || capability.Writer != *finalize.Writer || capability.Fingerprint != command.CapabilityFingerprint {
			return store.FileReferenceCommand{}, errors.New("file-reference capability is fenced")
		}
	}
	payload, err := json.Marshal(struct {
		MessageID      string                           `json:"message_id"`
		CommandID      string                           `json:"cmd_id"`
		Outcome        store.FileReferenceCommandStatus `json:"outcome"`
		ReferenceIndex *int                             `json:"reference_index"`
		Reason         *string                          `json:"reason"`
	}{command.MessageID, command.CommandID, finalize.Outcome, finalize.ReferenceIndex, finalize.ReasonCode})
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	seq, err := appendPostgresRunControlEvent(ctx, tx, sessionID, "session.file_references.outcome", payload, now)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE session_file_reference_commands SET status=$1,terminal_event_seq=$2,updated_at=clock_timestamp() WHERE session_id=$3 AND cmd_id=$4 AND reservation_version=$5 AND status='pending' AND terminal_event_seq IS NULL`, finalize.Outcome, seq, sessionID, commandID, finalize.ReservationVersion)
	if err != nil || result.RowsAffected() != 1 {
		return store.FileReferenceCommand{}, errors.New("file-reference terminal update is fenced")
	}
	command, err = queryPostgresFileReferenceCommand(ctx, tx, sessionID, commandID, false)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	return command, tx.Commit(ctx)
}
func (s *Store) RecoverFileReferenceCommand(ctx context.Context, sessionID, commandID, reason string) (store.FileReferenceCommand, error) {
	command, err := s.FileReferenceCommand(ctx, sessionID, commandID)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if command.Status != store.FileReferenceDeliveryPending {
		return s.FinalizeFileReferenceCommand(ctx, sessionID, commandID, store.FileReferenceCommandFinalize{ReservationVersion: command.ReservationVersion, Outcome: store.FileReferenceOutcomeUnknown, ReasonCode: &reason})
	}
	if reason != "adapter_deadline" || !validConnectionID(sessionID) || commandID == "" {
		return store.FileReferenceCommand{}, errors.New("invalid file-reference delivery recovery")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	defer tx.Rollback(ctx)
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.FileReferenceCommand{}, err
	}
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	command, err = queryPostgresFileReferenceCommand(ctx, tx, sessionID, commandID, true)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if command.Status != store.FileReferenceDeliveryPending || command.DeliveryDeadline.After(now) {
		return store.FileReferenceCommand{}, errors.New("file-reference delivery recovery is fenced")
	}
	payload, err := json.Marshal(struct {
		MessageID      string                           `json:"message_id"`
		CommandID      string                           `json:"cmd_id"`
		Outcome        store.FileReferenceCommandStatus `json:"outcome"`
		ReferenceIndex *int                             `json:"reference_index"`
		Reason         *string                          `json:"reason"`
	}{command.MessageID, command.CommandID, store.FileReferenceOutcomeUnknown, nil, &reason})
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	seq, err := appendPostgresRunControlEvent(ctx, tx, sessionID, "session.file_references.outcome", payload, now)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE session_file_reference_commands SET status='outcome_unknown',terminal_event_seq=$1,updated_at=clock_timestamp() WHERE session_id=$2 AND cmd_id=$3 AND reservation_version=$4 AND status='delivery_pending' AND delivery_deadline<=$5 AND terminal_event_seq IS NULL`, seq, sessionID, commandID, command.ReservationVersion, now)
	if err != nil || result.RowsAffected() != 1 {
		return store.FileReferenceCommand{}, errors.New("file-reference delivery recovery is fenced")
	}
	command, err = queryPostgresFileReferenceCommand(ctx, tx, sessionID, commandID, false)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	return command, tx.Commit(ctx)
}
func (s *Store) FileReferenceCommand(ctx context.Context, sessionID, commandID string) (store.FileReferenceCommand, error) {
	return queryPostgresFileReferenceCommand(ctx, s.pool, sessionID, commandID, false)
}
func (s *Store) PendingFileReferenceCommands(ctx context.Context, sessionID string) ([]store.FileReferenceCommand, error) {
	rows, err := s.pool.Query(ctx, `SELECT cmd_id FROM session_file_reference_commands WHERE session_id=$1 AND status IN ('delivery_pending','pending') ORDER BY reservation_version`, sessionID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	commands := make([]store.FileReferenceCommand, 0, len(ids))
	for _, id := range ids {
		command, err := s.FileReferenceCommand(ctx, sessionID, id)
		if err != nil {
			return nil, err
		}
		commands = append(commands, command)
	}
	return commands, nil
}

type postgresSettingsQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryPostgresRunControlCapability(ctx context.Context, querier postgresSettingsQuerier, sessionID string, lock bool) (store.RunControlCapability, error) {
	query := `SELECT session_id,capability_event_seq,capability_version,interrupt_supported,stop_supported,writer_connection_epoch,writer_credential_generation,writer_lease_id FROM session_run_control_capabilities WHERE session_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	var capability store.RunControlCapability
	if err := querier.QueryRow(ctx, query, sessionID).Scan(&capability.SessionID, &capability.EventSeq, &capability.Version, &capability.InterruptSupported, &capability.StopSupported, &capability.Writer.ConnectionEpoch, &capability.Writer.CredentialGeneration, &capability.Writer.LeaseID); err != nil {
		return store.RunControlCapability{}, err
	}
	if !validPostgresRunControlCapabilityUpdate(capability.SessionID, store.RunControlCapabilityUpdate{EventSeq: capability.EventSeq, InterruptSupported: capability.InterruptSupported, StopSupported: capability.StopSupported, Writer: capability.Writer}) || capability.Version < 1 {
		return store.RunControlCapability{}, errors.New("run-control capability row is invalid")
	}
	return capability, nil
}

func queryPostgresRunControlReservation(ctx context.Context, querier postgresSettingsQuerier, sessionID, commandID string, lock bool) (store.RunControlReservation, error) {
	query := `SELECT session_id,cmd_id,operation,capability_version,reservation_version,pre_control_state,pre_control_state_seq,writer_connection_epoch,writer_credential_generation,writer_lease_id,deadline,status,terminal_event_seq FROM session_run_controls WHERE session_id=$1 AND cmd_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var reservation store.RunControlReservation
	var terminal pgtype.Int8
	var status string
	if err := querier.QueryRow(ctx, query, sessionID, commandID).Scan(&reservation.SessionID, &reservation.CommandID, &reservation.Operation, &reservation.CapabilityVersion, &reservation.ReservationVersion, &reservation.PreControlState, &reservation.PreControlStateSeq, &reservation.Writer.ConnectionEpoch, &reservation.Writer.CredentialGeneration, &reservation.Writer.LeaseID, &reservation.Deadline, &status, &terminal); err != nil {
		return store.RunControlReservation{}, err
	}
	reservation.Outcome = store.RunControlOutcome(status)
	if terminal.Valid {
		value := terminal.Int64
		reservation.TerminalEventSeq = &value
	}
	if !validPostgresRunControlReservation(reservation) {
		return store.RunControlReservation{}, errors.New("run-control reservation row is invalid")
	}
	return reservation, nil
}

func verifyPostgresRunControlCapabilityEvent(ctx context.Context, querier postgresSettingsQuerier, sessionID string, eventSeq int64) error {
	var eventType string
	if eventSeq < 1 || querier.QueryRow(ctx, `SELECT type FROM session_events WHERE session_id=$1 AND seq=$2`, sessionID, eventSeq).Scan(&eventType) != nil || eventType != "session.run.capabilities" {
		return errors.New("run-control capability event is not durable")
	}
	return nil
}

func validatePostgresRunControlPreState(ctx context.Context, tx pgx.Tx, sessionID string, request store.RunControlRequest) error {
	var latest int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(seq),0) FROM session_events WHERE session_id=$1 AND type='session.state'`, sessionID).Scan(&latest); err != nil || latest != request.PreControlStateSeq {
		return errors.New("run-control pre-control state is stale")
	}
	var eventType string
	var payload []byte
	if err := tx.QueryRow(ctx, `SELECT type,payload FROM session_events WHERE session_id=$1 AND seq=$2`, sessionID, request.PreControlStateSeq).Scan(&eventType, &payload); err != nil || eventType != "session.state" {
		return errors.New("run-control pre-control state is not durable")
	}
	var state struct {
		State string `json:"state"`
	}
	if json.Unmarshal(payload, &state) != nil || state.State != request.PreControlState || !validPostgresRunControlState(request.Operation, state.State) {
		return errors.New("run-control pre-control state is invalid")
	}
	return nil
}

func appendPostgresRunControlEvent(ctx context.Context, tx pgx.Tx, sessionID, eventType string, payload []byte, now time.Time) (int64, error) {
	latest, err := db.New(tx).LatestSessionEventSeq(ctx, sessionID)
	if err != nil {
		return 0, err
	}
	seq := latest + 1
	if _, err := tx.Exec(ctx, `INSERT INTO session_events (session_id,seq,type,payload,created_at) VALUES ($1,$2,$3,$4::jsonb,$5)`, sessionID, seq, eventType, string(payload), now); err != nil {
		return 0, err
	}
	return seq, nil
}

func fencePostgresRunControlsAfterWriterReplacement(ctx context.Context, tx pgx.Tx, sessionID string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM session_run_control_capabilities WHERE session_id=$1`, sessionID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT cmd_id,operation,reservation_version FROM session_run_controls WHERE session_id=$1 AND status='pending' FOR UPDATE`, sessionID)
	if err != nil {
		return err
	}
	type pendingRunControl struct {
		commandID string
		operation store.RunControlOperation
		version   int64
	}
	var pending []pendingRunControl
	for rows.Next() {
		var commandID string
		var operation store.RunControlOperation
		var version int64
		if err := rows.Scan(&commandID, &operation, &version); err != nil {
			return err
		}
		pending = append(pending, pendingRunControl{commandID, operation, version})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return err
	}
	for _, command := range pending {
		reason := "adapter_disconnected"
		payload, err := json.Marshal(struct {
			CommandID       string                    `json:"cmd_id"`
			Operation       store.RunControlOperation `json:"operation"`
			Outcome         store.RunControlOutcome   `json:"outcome"`
			CompletionState *string                   `json:"completion_state"`
			ReasonCode      *string                   `json:"reason_code"`
		}{command.commandID, command.operation, store.RunControlOutcomeUnknown, nil, &reason})
		if err != nil {
			return err
		}
		seq, err := appendPostgresRunControlEvent(ctx, tx, sessionID, "session.run.outcome", payload, now)
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `UPDATE session_run_controls SET status='outcome_unknown',terminal_event_seq=$1,updated_at=clock_timestamp() WHERE session_id=$2 AND cmd_id=$3 AND reservation_version=$4 AND status='pending'`, seq, sessionID, command.commandID, command.version)
		if err != nil || result.RowsAffected() != 1 {
			return errors.New("run-control replacement fence lost race")
		}
	}
	return fencePostgresFileReferenceCommandsAfterWriterReplacement(ctx, tx, sessionID)
}

func fencePostgresFileReferenceCommandsAfterWriterReplacement(ctx context.Context, tx pgx.Tx, sessionID string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM session_file_reference_capabilities WHERE session_id=$1`, sessionID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT cmd_id,message_id FROM session_file_reference_commands WHERE session_id=$1 AND status IN ('delivery_pending','pending') FOR UPDATE`, sessionID)
	if err != nil {
		return err
	}
	type pendingFileReference struct{ commandID, messageID string }
	var pending []pendingFileReference
	for rows.Next() {
		var commandID, messageID string
		if err := rows.Scan(&commandID, &messageID); err != nil {
			return err
		}
		pending = append(pending, pendingFileReference{commandID, messageID})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	now, err := postgresSettingsNow(ctx, tx)
	if err != nil {
		return err
	}
	for _, command := range pending {
		reason := "writer_lost"
		payload, err := json.Marshal(struct {
			MessageID      string  `json:"message_id"`
			CommandID      string  `json:"cmd_id"`
			Outcome        string  `json:"outcome"`
			ReferenceIndex *int    `json:"reference_index"`
			Reason         *string `json:"reason"`
		}{command.messageID, command.commandID, "outcome_unknown", nil, &reason})
		if err != nil {
			return err
		}
		seq, err := appendPostgresRunControlEvent(ctx, tx, sessionID, "session.file_references.outcome", payload, now)
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `UPDATE session_file_reference_commands SET status='outcome_unknown',terminal_event_seq=$1,updated_at=clock_timestamp() WHERE session_id=$2 AND cmd_id=$3 AND status IN ('delivery_pending','pending')`, seq, sessionID, command.commandID)
		if err != nil || result.RowsAffected() != 1 {
			return errors.New("file-reference replacement fence lost race")
		}
	}
	return nil
}

func validPostgresRunControlCapabilityUpdate(sessionID string, update store.RunControlCapabilityUpdate) bool {
	return validConnectionID(sessionID) && update.EventSeq > 0 && validPostgresSettingsWriter(update.Writer)
}
func validPostgresRunControlRequest(sessionID string, request store.RunControlRequest) bool {
	return validConnectionID(sessionID) && validPostgresSettingsID(request.CommandID) && validPostgresRunControlOperation(request.Operation) && request.PreControlStateSeq > 0 && validPostgresSettingsWriter(request.Writer)
}
func validPostgresRunControlOperation(operation store.RunControlOperation) bool {
	return operation == store.RunControlInterrupt || operation == store.RunControlStop
}
func validPostgresRunControlState(operation store.RunControlOperation, state string) bool {
	if operation == store.RunControlInterrupt {
		return state == "busy"
	}
	return state == "starting" || state == "ready" || state == "busy" || state == "waiting_permission" || state == "recovering"
}
func validPostgresRunControlTerminalOutcome(outcome store.RunControlOutcome) bool {
	return outcome == store.RunControlCompleted || outcome == store.RunControlRejected || outcome == store.RunControlTimeout || outcome == store.RunControlUnsupported || outcome == store.RunControlOutcomeUnknown
}
func validPostgresRunControlReservation(reservation store.RunControlReservation) bool {
	terminal := reservation.Outcome != store.RunControlPending
	return validPostgresRunControlRequest(reservation.SessionID, store.RunControlRequest{CommandID: reservation.CommandID, Operation: reservation.Operation, PreControlState: reservation.PreControlState, PreControlStateSeq: reservation.PreControlStateSeq, Writer: reservation.Writer}) && reservation.CapabilityVersion > 0 && reservation.ReservationVersion > 0 && !reservation.Deadline.IsZero() && (reservation.Outcome == store.RunControlPending || validPostgresRunControlTerminalOutcome(reservation.Outcome)) && ((terminal && reservation.TerminalEventSeq != nil) || (!terminal && reservation.TerminalEventSeq == nil))
}

func postgresSettingsNow(ctx context.Context, querier postgresSettingsQuerier) (time.Time, error) {
	var now time.Time
	if err := querier.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read settings Store clock: %w", err)
	}
	return now, nil
}

func verifyPostgresSettingsCapabilityEvent(ctx context.Context, querier postgresSettingsQuerier, sessionID string, eventSeq int64) error {
	if eventSeq < 1 {
		return errors.New("settings capability event reference is invalid")
	}
	var eventType string
	if err := querier.QueryRow(ctx, `SELECT type FROM session_events WHERE session_id=$1 AND seq=$2`, sessionID, eventSeq).Scan(&eventType); err != nil || eventType != "session.settings.capabilities" {
		return errors.New("settings capability event is not durable")
	}
	return nil
}

func validatePostgresCurrentSettingsWriter(ctx context.Context, querier postgresSettingsQuerier, sessionID string, writer store.SettingsWriter) error {
	var current store.SettingsWriter
	if err := querier.QueryRow(ctx, `SELECT writer_connection_epoch,writer_credential_generation,writer_lease_id FROM session_settings_capabilities WHERE session_id=$1 FOR UPDATE`, sessionID).Scan(&current.ConnectionEpoch, &current.CredentialGeneration, &current.LeaseID); err != nil || current != writer {
		return errors.New("settings writer is no longer current")
	}
	return validatePostgresLiveSettingsWriter(ctx, querier, sessionID, writer)
}

func validatePostgresLiveSettingsWriter(ctx context.Context, querier postgresSettingsQuerier, sessionID string, writer store.SettingsWriter) error {
	var current bool
	err := querier.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM session_settings_live_writers AS writer
JOIN session_adapter_connections AS connection ON connection.session_id=writer.session_id
WHERE writer.session_id=$1 AND writer.connection_epoch=$2 AND writer.credential_generation=$3 AND writer.writer_lease_id=$4
  AND connection.connection_epoch=writer.connection_epoch AND connection.active_credential_generation=writer.credential_generation
  AND connection.active_credential_expires_at>clock_timestamp() AND connection.revoked_at IS NULL AND connection.terminal_at IS NULL
)`, sessionID, writer.ConnectionEpoch, writer.CredentialGeneration, writer.LeaseID).Scan(&current)
	if err != nil || !current {
		return errors.New("settings writer has no live authority")
	}
	return nil
}

func validPostgresFileReferenceCapabilityUpdate(sessionID string, update store.FileReferenceCapabilityUpdate) bool {
	return validConnectionID(sessionID) && update.EventSeq > 0 && validPostgresSettingsFingerprint(update.Fingerprint) && validPostgresSettingsWriter(update.Writer)
}
func validPostgresFileReferenceRequest(sessionID string, request store.FileReferenceCommandRequest) bool {
	return validConnectionID(sessionID) && validPostgresSettingsID(request.CommandID) && validPostgresSettingsID(request.MessageID) && validPostgresSettingsFingerprint(request.CapabilityFingerprint) && validPostgresSettingsFingerprint(request.RequestFingerprint) && request.ReferenceCount >= 1 && request.ReferenceCount <= 8
}
func queryPostgresFileReferenceCapability(ctx context.Context, querier postgresSettingsQuerier, sessionID string, lock bool) (store.FileReferenceCapability, error) {
	query := `SELECT session_id,capability_event_seq,capability_fingerprint,writer_connection_epoch,writer_credential_generation,writer_lease_id FROM session_file_reference_capabilities WHERE session_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	var capability store.FileReferenceCapability
	err := querier.QueryRow(ctx, query, sessionID).Scan(&capability.SessionID, &capability.EventSeq, &capability.Fingerprint, &capability.Writer.ConnectionEpoch, &capability.Writer.CredentialGeneration, &capability.Writer.LeaseID)
	if err != nil || !validPostgresFileReferenceCapabilityUpdate(capability.SessionID, store.FileReferenceCapabilityUpdate{EventSeq: capability.EventSeq, Fingerprint: capability.Fingerprint, Writer: capability.Writer}) {
		return store.FileReferenceCapability{}, errors.New("file-reference capability row is invalid")
	}
	return capability, nil
}
func queryPostgresFileReferenceCommand(ctx context.Context, querier postgresSettingsQuerier, sessionID, commandID string, lock bool) (store.FileReferenceCommand, error) {
	query := `SELECT session_id,cmd_id,message_id,capability_fingerprint,request_fingerprint,reference_count,reservation_version,delivery_deadline,writer_connection_epoch,writer_credential_generation,writer_lease_id,status,terminal_event_seq FROM session_file_reference_commands WHERE session_id=$1 AND cmd_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var command store.FileReferenceCommand
	var epoch, generation pgtype.Int8
	var lease pgtype.Text
	var terminal pgtype.Int8
	var status string
	var deadline pgtype.Timestamptz
	err := querier.QueryRow(ctx, query, sessionID, commandID).Scan(&command.SessionID, &command.CommandID, &command.MessageID, &command.CapabilityFingerprint, &command.RequestFingerprint, &command.ReferenceCount, &command.ReservationVersion, &deadline, &epoch, &generation, &lease, &status, &terminal)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	command.Status = store.FileReferenceCommandStatus(status)
	command.DeliveryDeadline = deadline.Time
	if epoch.Valid && generation.Valid && lease.Valid {
		command.Writer = &store.FileReferenceWriter{ConnectionEpoch: epoch.Int64, CredentialGeneration: generation.Int64, LeaseID: lease.String}
	}
	if terminal.Valid {
		value := terminal.Int64
		command.TerminalEventSeq = &value
	}
	return command, nil
}

func queryPostgresSettingsCapability(ctx context.Context, querier postgresSettingsQuerier, sessionID string, lock bool) (store.SettingsCapability, error) {
	query := `SELECT session_id,capability_event_seq,fingerprint,effective_model_id,effective_permission_mode_id,capability_version,writer_connection_epoch,writer_credential_generation,writer_lease_id FROM session_settings_capabilities WHERE session_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	var capability store.SettingsCapability
	if err := querier.QueryRow(ctx, query, sessionID).Scan(&capability.SessionID, &capability.EventSeq, &capability.Fingerprint, &capability.EffectiveModelID, &capability.EffectivePermissionModeID, &capability.Version, &capability.Writer.ConnectionEpoch, &capability.Writer.CredentialGeneration, &capability.Writer.LeaseID); err != nil {
		return store.SettingsCapability{}, err
	}
	if !validConnectionID(capability.SessionID) || !validPostgresSettingsCapability(store.SettingsCapabilityUpdate{EventSeq: capability.EventSeq, Fingerprint: capability.Fingerprint, EffectiveModelID: capability.EffectiveModelID, EffectivePermissionModeID: capability.EffectivePermissionModeID, Writer: capability.Writer}) || capability.Version < 1 {
		return store.SettingsCapability{}, errors.New("settings capability row is invalid")
	}
	return capability, nil
}

func queryPostgresSettingsCommand(ctx context.Context, querier postgresSettingsQuerier, sessionID, commandID string, lock bool) (store.SettingsCommand, error) {
	query := `SELECT session_id,cmd_id,request_fingerprint,requested_model_id,requested_permission_mode_id,reservation_version,delivery_deadline,operation_deadline,writer_connection_epoch,writer_credential_generation,writer_lease_id,reserved_capability_event_seq,reserved_fingerprint,reserved_effective_model_id,reserved_effective_permission_mode_id,status,terminal_event_seq FROM session_settings_commands WHERE session_id=$1 AND cmd_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var command store.SettingsCommand
	var modelID, permissionID pgtype.Text
	var operationDeadline pgtype.Timestamptz
	var terminalSeq pgtype.Int8
	var status string
	if err := querier.QueryRow(ctx, query, sessionID, commandID).Scan(&command.SessionID, &command.CommandID, &command.RequestFingerprint, &modelID, &permissionID, &command.ReservationVersion, &command.DeliveryDeadline, &operationDeadline, &command.Writer.ConnectionEpoch, &command.Writer.CredentialGeneration, &command.Writer.LeaseID, &command.ReservedCapability.EventSeq, &command.ReservedCapability.Fingerprint, &command.ReservedCapability.EffectiveModelID, &command.ReservedCapability.EffectivePermissionModeID, &status, &terminalSeq); err != nil {
		return store.SettingsCommand{}, err
	}
	command.Status = store.SettingsCommandStatus(status)
	command.ReservedCapability.SessionID = sessionID
	command.ReservedCapability.Writer = command.Writer
	if modelID.Valid {
		value := modelID.String
		command.RequestedModelID = &value
	}
	if permissionID.Valid {
		value := permissionID.String
		command.RequestedPermissionModeID = &value
	}
	if operationDeadline.Valid {
		value := operationDeadline.Time
		command.OperationDeadline = &value
	}
	if terminalSeq.Valid {
		value := terminalSeq.Int64
		command.TerminalEventSeq = &value
	}
	if !validPostgresSettingsCommand(command) {
		return store.SettingsCommand{}, errors.New("settings command row is invalid")
	}
	return command, nil
}

func validPostgresSettingsRequest(sessionID string, request store.SettingsCommandRequest) bool {
	return validConnectionID(sessionID) && validPostgresSettingsID(request.CommandID) && validPostgresSettingsFingerprint(request.RequestFingerprint) && (request.RequestedModelID != nil || request.RequestedPermissionModeID != nil) && (request.RequestedModelID == nil || validPostgresSettingsID(*request.RequestedModelID)) && (request.RequestedPermissionModeID == nil || validPostgresSettingsID(*request.RequestedPermissionModeID)) && validPostgresSettingsWriter(request.Writer)
}
func validPostgresSettingsCommand(command store.SettingsCommand) bool {
	terminal := !validPostgresSettingsNonterminal(command.Status)
	return validPostgresSettingsRequest(command.SessionID, store.SettingsCommandRequest{CommandID: command.CommandID, RequestFingerprint: command.RequestFingerprint, RequestedModelID: command.RequestedModelID, RequestedPermissionModeID: command.RequestedPermissionModeID, Writer: command.Writer}) && command.ReservationVersion > 0 && !command.DeliveryDeadline.IsZero() && validPostgresSettingsStatus(command.Status) && command.ReservedCapability.EventSeq > 0 && validPostgresSettingsFingerprint(command.ReservedCapability.Fingerprint) && validPostgresSettingsID(command.ReservedCapability.EffectiveModelID) && validPostgresSettingsID(command.ReservedCapability.EffectivePermissionModeID) && command.ReservedCapability.Writer == command.Writer && ((terminal && command.TerminalEventSeq != nil) || (!terminal && command.TerminalEventSeq == nil))
}
func validPostgresSettingsWriter(writer store.SettingsWriter) bool {
	return writer.ConnectionEpoch > 0 && writer.CredentialGeneration > 0 && writer.LeaseID != "" && len(writer.LeaseID) <= 255
}
func validPostgresSettingsID(value string) bool {
	if len(value) < 1 || len(value) > 128 || ((value[0] < 'A' || value[0] > 'Z') && (value[0] < 'a' || value[0] > 'z') && (value[0] < '0' || value[0] > '9')) {
		return false
	}
	for _, char := range value[1:] {
		if (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '.' && char != '_' && char != ':' && char != '/' && char != '-' {
			return false
		}
	}
	return true
}
func validPostgresSettingsFingerprint(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[7:] {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
func validPostgresSettingsStatus(status store.SettingsCommandStatus) bool {
	switch status {
	case store.SettingsCommandDeliveryPending, store.SettingsCommandPending, store.SettingsCommandRecoveryPending, store.SettingsCommandApplied, store.SettingsCommandRejected, store.SettingsCommandTimeout, store.SettingsCommandUnsupported, store.SettingsCommandStaleCapability, store.SettingsCommandOutcomeUnknown, store.SettingsCommandMismatched:
		return true
	default:
		return false
	}
}
func validPostgresSettingsNonterminal(status store.SettingsCommandStatus) bool {
	return status == store.SettingsCommandDeliveryPending || status == store.SettingsCommandPending || status == store.SettingsCommandRecoveryPending
}
func validPostgresSettingsTerminal(status store.SettingsCommandStatus) bool {
	return validPostgresSettingsStatus(status) && !validPostgresSettingsNonterminal(status)
}
func samePostgresSettingsOptionalID(left, right *string) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}
func samePostgresSettingsCapability(left, right store.SettingsCapability) bool {
	return left.SessionID == right.SessionID && left.EventSeq == right.EventSeq && left.Fingerprint == right.Fingerprint && left.EffectiveModelID == right.EffectiveModelID && left.EffectivePermissionModeID == right.EffectivePermissionModeID && left.Version == right.Version && left.Writer == right.Writer
}
func validPostgresSettingsReason(reason *string) bool {
	if reason == nil || len(*reason) < 1 || len(*reason) > 64 || (*reason)[0] < 'a' || (*reason)[0] > 'z' {
		return false
	}
	for _, char := range *reason {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

func validPostgresSettingsFinalize(sessionID, commandID string, finalize store.SettingsCommandFinalize) bool {
	return validConnectionID(sessionID) && validPostgresSettingsID(commandID) && finalize.ReservationVersion > 0 && validPostgresSettingsNonterminal(finalize.ExpectedStatus) && validPostgresSettingsTerminal(finalize.Outcome) && validPostgresSettingsCapability(store.SettingsCapabilityUpdate{EventSeq: finalize.EffectiveCapability.EventSeq, Fingerprint: finalize.EffectiveCapability.Fingerprint, EffectiveModelID: finalize.EffectiveCapability.EffectiveModelID, EffectivePermissionModeID: finalize.EffectiveCapability.EffectivePermissionModeID, Writer: finalize.EffectiveCapability.Writer}) && (finalize.EffectiveCapability.SessionID == "" || finalize.EffectiveCapability.SessionID == sessionID)
}
func validatePostgresSettingsFinalization(command store.SettingsCommand, capability store.SettingsCapability, finalize store.SettingsCommandFinalize, now time.Time) error {
	if finalize.Outcome == store.SettingsCommandApplied {
		if finalize.ReasonCode != nil || (command.RequestedModelID != nil && *command.RequestedModelID != capability.EffectiveModelID) || (command.RequestedPermissionModeID != nil && *command.RequestedPermissionModeID != capability.EffectivePermissionModeID) || (command.RequestedModelID == nil && command.ReservedCapability.EffectiveModelID != capability.EffectiveModelID) || (command.RequestedPermissionModeID == nil && command.ReservedCapability.EffectivePermissionModeID != capability.EffectivePermissionModeID) {
			return errors.New("applied settings finalization does not match the request")
		}
	} else if !validPostgresSettingsReason(finalize.ReasonCode) {
		return errors.New("non-applied settings finalization requires a bounded reason")
	}
	switch finalize.ExpectedStatus {
	case store.SettingsCommandDeliveryPending:
		if finalize.Writer != nil || finalize.Outcome != store.SettingsCommandRejected || finalize.ReasonCode == nil || *finalize.ReasonCode != "adapter_delivery_failed" || command.DeliveryDeadline.After(now) {
			return errors.New("delivery-pending settings command may only reject")
		}
	case store.SettingsCommandPending:
		if finalize.Writer != nil && (command.OperationDeadline == nil || !command.OperationDeadline.After(now)) {
			return errors.New("settings operation deadline has elapsed")
		}
		if finalize.Writer == nil && (finalize.Outcome != store.SettingsCommandTimeout || command.OperationDeadline == nil || command.OperationDeadline.After(now)) {
			return errors.New("unbound pending settings finalization requires an elapsed operation deadline")
		}
	case store.SettingsCommandRecoveryPending:
		if finalize.Writer != nil || finalize.Outcome != store.SettingsCommandOutcomeUnknown || capability.Writer == command.Writer || capability.EventSeq <= command.ReservedCapability.EventSeq {
			return errors.New("recovery-pending settings command may only finalize unknown without an old writer")
		}
	default:
		return errors.New("settings finalization expected status is not nonterminal")
	}
	return nil
}

func (s *Store) ListPendingCommands(ctx context.Context, sessionID string, authority store.CommandAuthority) ([]store.PendingCommand, error) {
	tx, queries, err := s.beginCommandMutation(ctx, sessionID, authority)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := queries.ListPendingCommandsForDelivery(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list pending commands: %w", err)
	}
	storeNow, err := queries.CommandStoreNow(ctx)
	if err != nil {
		return nil, fmt.Errorf("read pending command Store clock: %w", err)
	}
	commands := make([]store.PendingCommand, 0, len(rows))
	for _, row := range rows {
		command := pendingCommand(row)
		if command.SessionID != sessionID || command.Type != "session.send" || command.EventSeq < 1 ||
			(command.Status != store.PendingCommandPending && command.Status != store.PendingCommandReceived) ||
			!command.ExpiresAt.After(storeNow.Time) {
			return nil, errors.New("pending command row is invalid")
		}
		event, err := queries.NextSessionEvent(ctx, db.NextSessionEventParams{SessionID: sessionID, Seq: command.EventSeq - 1})
		if err != nil || event.Seq != command.EventSeq || validatePendingCommandInput(store.PendingEvent{
			Type: event.Type, Time: event.CreatedAt.Time, Payload: event.Payload,
		}, store.PendingCommandRequest{
			CommandID: command.CommandID, Type: command.Type, ExpiresAt: command.ExpiresAt,
		}, storeNow.Time) != nil {
			return nil, errors.New("pending command event is invalid")
		}
		commands = append(commands, command)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit pending command listing: %w", err)
	}
	return commands, nil
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

func (s *Store) ResolvePendingCommandUnknown(ctx context.Context, sessionID string, commandID string) (store.PendingCommand, error) {
	if s.pool == nil {
		return store.PendingCommand{}, errors.New("postgres event store pool is nil")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("begin unknown pending command resolution: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	updated, err := queries.ResolvePendingCommandUnknown(ctx, db.ResolvePendingCommandUnknownParams{SessionID: sessionID, CmdID: commandID})
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("resolve pending command outcome unknown: %w", err)
	}
	operation := "command"
	if err := upsertAttentionLedger(ctx, queries, sessionID, &store.AttentionBlocker{Kind: store.AttentionBlockerOutcomeUnknown, Operation: &operation}, nil); err != nil {
		return store.PendingCommand{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.PendingCommand{}, fmt.Errorf("commit pending command outcome unknown: %w", err)
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
		EventType: event.Type, Payload: event.Payload, CreatedAt: pgtype.Timestamptz{Time: event.Time, Valid: true},
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

func (s *Store) ValidateWarmAttachTargetActivation(ctx context.Context, sessionID string, activation store.WarmAttachTargetActivation) error {
	if s == nil || !validConnectionID(sessionID) || activation.Generation < 1 || activation.ExpiresAt.IsZero() {
		return errors.New("invalid warm attach target activation")
	}
	statement := `SELECT 1 FROM session_adapter_connections WHERE session_id=$1 AND active_credential_generation=$2 AND credential_generation_high_watermark=$2 AND active_credential_expires_at=$3 AND connection_epoch=0 AND accepted_fence=0 AND pending_credential_generation IS NULL AND prior_recovery_credential_generation IS NULL AND rotation_id IS NULL AND revoked_at IS NULL AND terminal_at IS NULL AND active_credential_expires_at>clock_timestamp()`
	var err error
	if s.connectionTx != nil {
		statement += ` FOR UPDATE`
		err = s.connectionTx.QueryRow(ctx, statement, sessionID, activation.Generation, activation.ExpiresAt).Scan(new(int))
	} else if s.pool != nil {
		err = s.pool.QueryRow(ctx, statement, sessionID, activation.Generation, activation.ExpiresAt).Scan(new(int))
	} else {
		err = errors.New("postgres event store pool is nil")
	}
	if err != nil {
		return errors.New("warm attach target activation is expired or fenced")
	}
	return nil
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
	if s.connectionTx != nil {
		queries := db.New(s.connectionTx)
		if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
			return store.AdapterConnection{}, err
		}
		row, err := queries.TerminateAdapterConnectionBeforeHello(ctx, db.TerminateAdapterConnectionBeforeHelloParams{SessionID: sessionID, ExpectedActiveGeneration: termination.ExpectedActiveCredentialGeneration})
		if err != nil {
			return store.AdapterConnection{}, fmt.Errorf("terminate pre-hello adapter connection: %w", err)
		}
		if err := fencePostgresRunControlsAfterWriterReplacement(ctx, s.connectionTx, sessionID); err != nil {
			return store.AdapterConnection{}, err
		}
		return adapterConnection(db.SessionAdapterConnection(row)), nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.AdapterConnection{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.TerminateAdapterConnectionBeforeHello(ctx, db.TerminateAdapterConnectionBeforeHelloParams{SessionID: sessionID, ExpectedActiveGeneration: termination.ExpectedActiveCredentialGeneration})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("terminate pre-hello adapter connection: %w", err)
	}
	if err := fencePostgresRunControlsAfterWriterReplacement(ctx, tx, sessionID); err != nil {
		return store.AdapterConnection{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.AdapterConnection{}, err
	}
	return adapterConnection(db.SessionAdapterConnection(row)), nil
}

func (s *Store) AcceptAdapterHello(ctx context.Context, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || hello.CredentialGeneration < 1 {
		return store.AdapterConnection{}, errors.New("invalid adapter hello")
	}
	if hello.WriterLeaseID != "" && len(hello.WriterLeaseID) > 255 {
		return store.AdapterConnection{}, errors.New("invalid adapter writer lease")
	}
	return s.acceptAdapterHelloWithWriterLease(ctx, sessionID, hello)
}
func (s *Store) acceptAdapterHelloWithWriterLease(ctx context.Context, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	if s.connectionTx != nil {
		return acceptAdapterHelloWithWriterLeaseTx(ctx, s.connectionTx, sessionID, hello)
	}
	if s.pool == nil {
		return store.AdapterConnection{}, errors.New("postgres event store pool is nil")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("begin adapter hello lease transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	connection, err := acceptAdapterHelloWithWriterLeaseTx(ctx, tx, sessionID, hello)
	if err != nil {
		return store.AdapterConnection{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.AdapterConnection{}, fmt.Errorf("commit adapter hello lease transaction: %w", err)
	}
	return connection, nil
}
func acceptAdapterHelloWithWriterLeaseTx(ctx context.Context, tx pgx.Tx, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return store.AdapterConnection{}, err
	}
	row, err := queries.AcceptAdapterHello(ctx, db.AcceptAdapterHelloParams{SessionID: sessionID, CredentialGeneration: hello.CredentialGeneration})
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("accept adapter hello: %w", err)
	}
	connection := adapterConnection(row)
	if hello.WriterLeaseID != "" {
		if _, err := tx.Exec(ctx, `INSERT INTO session_settings_live_writers (session_id,connection_epoch,credential_generation,writer_lease_id)
VALUES ($1,$2,$3,$4)
ON CONFLICT (session_id) DO UPDATE SET connection_epoch=EXCLUDED.connection_epoch,credential_generation=EXCLUDED.credential_generation,writer_lease_id=EXCLUDED.writer_lease_id`, sessionID, connection.ConnectionEpoch, connection.ActiveCredentialGeneration, hello.WriterLeaseID); err != nil {
			return store.AdapterConnection{}, fmt.Errorf("bind adapter writer lease: %w", err)
		}
	}
	if err := fencePostgresRunControlsAfterWriterReplacement(ctx, tx, sessionID); err != nil {
		return store.AdapterConnection{}, err
	}
	return connection, nil
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

func (s *Store) IssueAdapterConnectionAuthorityReceipt(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, writer store.SettingsWriter) (store.ConnectionAuthorityReceipt, error) {
	if !validConnectionID(sessionID) || writer.LeaseID == "" || writer.ConnectionEpoch != admission.ConnectionEpoch || writer.CredentialGeneration != admission.CredentialGeneration {
		return store.ConnectionAuthorityReceipt{}, errors.New("invalid adapter connection authority receipt")
	}
	if s.connectionTx != nil {
		return s.issueAdapterConnectionAuthorityReceipt(ctx, sessionID, admission, writer)
	}
	var receipt store.ConnectionAuthorityReceipt
	err := s.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
		scoped, ok := tx.(*Store)
		if !ok {
			return errors.New("postgres adapter receipt transaction is unavailable")
		}
		var err error
		receipt, err = scoped.issueAdapterConnectionAuthorityReceipt(ctx, sessionID, admission, writer)
		return err
	})
	if err != nil {
		return store.ConnectionAuthorityReceipt{}, err
	}
	return receipt, nil
}

func (s *Store) issueAdapterConnectionAuthorityReceipt(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, writer store.SettingsWriter) (store.ConnectionAuthorityReceipt, error) {
	connection, err := s.ValidateAdapterAdmission(ctx, sessionID, admission)
	if err != nil || s.connectionTx == nil || validatePostgresLiveSettingsWriter(ctx, s.connectionTx, sessionID, writer) != nil {
		return store.ConnectionAuthorityReceipt{}, errors.New("adapter connection authority is not live")
	}
	return store.ConnectionAuthorityReceipt{
		SessionID: sessionID, ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: connection.ActiveCredentialGeneration,
		AcceptedFence: connection.AcceptedFence, WriterLeaseID: writer.LeaseID, ExpiresAt: connection.ActiveCredentialExpiresAt,
	}, nil
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
	request.ExpiresAt = postgresTimestamp(request.ExpiresAt)
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
	return current.Identity == request.Identity && current.Fingerprint == request.Fingerprint && samePostgresTimestamp(current.ExpiresAt, request.ExpiresAt) &&
		current.Outcome == request.Outcome && ((current.IssuedCredentialGeneration == nil && request.IssuedCredentialGeneration == nil) ||
		(current.IssuedCredentialGeneration != nil && request.IssuedCredentialGeneration != nil && *current.IssuedCredentialGeneration == *request.IssuedCredentialGeneration))
}

func (s *Store) CommitWarmAttach(ctx context.Context, request store.WarmAttachRequest) (store.WarmAttachCommit, error) {
	if s.pool == nil {
		return store.WarmAttachCommit{}, errors.New("postgres event store pool is nil")
	}
	request = normalizeWarmAttachRequest(request)
	if err := validateWarmAttachRequest(request); err != nil {
		return store.WarmAttachCommit{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("begin warm attach transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	storeNow, err := queries.WarmAttachStoreNow(ctx)
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("read warm attach Store clock: %w", err)
	}
	if !validWarmAttachExpiry(request, storeNow.Time) {
		return store.WarmAttachCommit{}, errors.New("warm attach expiry is outside the Store-clock window")
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(request.Attachment.Identity.TargetSessionID)); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("lock warm attach target stream: %w", err)
	}
	if existing, lookupErr := queries.AttachAttemptByJTIHash(ctx, request.Attempt.Identity.JTIHash[:]); lookupErr == nil {
		commit, duplicateErr := warmAttachDuplicate(ctx, queries, attachAttempt(existing), request)
		if duplicateErr != nil {
			return store.WarmAttachCommit{}, duplicateErr
		}
		if err := tx.Commit(ctx); err != nil {
			return store.WarmAttachCommit{}, fmt.Errorf("commit warm attach duplicate: %w", err)
		}
		return commit, nil
	} else if !errors.Is(lookupErr, pgx.ErrNoRows) {
		return store.WarmAttachCommit{}, fmt.Errorf("load existing warm attach: %w", lookupErr)
	}
	if _, err := queries.LockWarmAttachBootstrap(ctx, db.LockWarmAttachBootstrapParams{
		SessionID: request.Attempt.Identity.BootstrapSessionID, CredentialGeneration: request.BootstrapAdmission.CredentialGeneration,
		ConnectionEpoch: request.BootstrapAdmission.ConnectionEpoch, AcceptedFence: request.BootstrapAdmission.AcceptedFence,
		GrantFence: request.BootstrapAdmission.GrantFence,
	}); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("lock warm attach bootstrap admission: %w", err)
	}
	if _, err := queries.LockWarmAttachTarget(ctx, request.Attachment.Identity.TargetSessionID); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("lock warm attach target: %w", err)
	}
	if _, err := queries.InitializeAdapterConnection(ctx, db.InitializeAdapterConnectionParams{
		SessionID: request.Attachment.Identity.TargetSessionID, ActiveGeneration: request.TargetActivation.Generation,
		ActiveExpiresAt: pgtype.Timestamptz{Time: request.TargetActivation.ExpiresAt, Valid: true},
	}); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("initialize warm attach target credential: %w", err)
	}
	if _, err := queries.AttachmentByTarget(ctx, request.Attachment.Identity.TargetSessionID); err == nil {
		return store.WarmAttachCommit{}, errors.New("warm attach target already has an attachment")
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return store.WarmAttachCommit{}, fmt.Errorf("load warm attach target attachment: %w", err)
	}
	storeNow, err = queries.WarmAttachStoreNow(ctx)
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("revalidate warm attach Store clock: %w", err)
	}
	if !validWarmAttachExpiry(request, storeNow.Time) {
		return store.WarmAttachCommit{}, errors.New("warm attach expiry is outside the Store-clock window")
	}
	attemptRow, err := queries.InsertAttachAttempt(ctx, attachAttemptParams(request.Attempt))
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("insert warm attach attempt: %w", err)
	}
	attachmentRow, err := queries.InsertAttachment(ctx, db.InsertAttachmentParams{
		AttachID: request.Attachment.Identity.AttachID, BootstrapSessionID: request.Attachment.Identity.BootstrapSessionID,
		TargetSessionID: request.Attachment.Identity.TargetSessionID, ExpiresAt: pgtype.Timestamptz{Time: request.Attachment.ExpiresAt, Valid: true},
		TargetCredentialLineageRef: request.Attachment.Identity.TargetCredentialLineageRef,
	})
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("insert warm attach attachment: %w", err)
	}
	seq, terminal, err := appendEventsLocked(ctx, queries, request.Attachment.Identity.TargetSessionID, []store.PendingEvent{{
		Type: "session.message", Time: storeNow.Time, Payload: warmAttachEventPayload(request.FirstDelivery),
	}})
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("append warm attach reference event: %w", err)
	}
	if terminal {
		return store.WarmAttachCommit{}, errors.New("warm attach reference event must not be terminal")
	}
	commandRow, err := queries.InsertWarmAttachPendingCommand(ctx, db.InsertWarmAttachPendingCommandParams{
		SessionID: request.Attachment.Identity.TargetSessionID, CmdID: request.FirstDelivery.CommandID, EventSeq: seq,
		ExpiresAt: pgtype.Timestamptz{Time: request.FirstDelivery.ExpiresAt, Valid: true},
	})
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("insert warm attach pending command: %w", err)
	}
	created := attachment(attachmentRow)
	if err := upsertAttentionLedger(ctx, queries, created.Identity.TargetSessionID, attentionBlockerForAttachment(created, nil), &storeNow.Time); err != nil {
		return store.WarmAttachCommit{}, err
	}
	summary, err := warmAttachSummary(ctx, queries, created.Identity.TargetSessionID)
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("commit warm attach: %w", err)
	}
	return store.WarmAttachCommit{Attempt: attachAttempt(attemptRow), Attachment: created, TargetActivation: request.TargetActivation, Outbox: store.WarmAttachOutbox{
		TargetSessionID: commandRow.SessionID, CommandID: commandRow.CmdID, EventSeq: commandRow.EventSeq,
		ReferenceID: request.FirstDelivery.ReferenceID, ReferenceDigest: request.FirstDelivery.ReferenceDigest, ExpiresAt: commandRow.ExpiresAt.Time,
	}, Summary: summary}, nil
}

func (s *Store) ExpireWarmAttach(ctx context.Context, attachID string, expectedDeliveryVersion int64) (store.WarmAttachExpiry, error) {
	if s.pool == nil {
		return store.WarmAttachExpiry{}, errors.New("postgres event store pool is nil")
	}
	if !validConnectionID(attachID) || expectedDeliveryVersion < 0 {
		return store.WarmAttachExpiry{}, errors.New("invalid warm attach expiry")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.WarmAttachExpiry{}, fmt.Errorf("begin warm attach expiry: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	currentRow, err := queries.LockAttachment(ctx, attachID)
	if err != nil {
		return store.WarmAttachExpiry{}, fmt.Errorf("lock warm attach expiry: %w", err)
	}
	current := attachment(currentRow)
	storeNow, err := queries.WarmAttachStoreNow(ctx)
	if err != nil {
		return store.WarmAttachExpiry{}, fmt.Errorf("read warm attach expiry clock: %w", err)
	}
	if current.DeliveryVersion != expectedDeliveryVersion || current.Status != store.AttachmentJoinPending || current.ExpiresAt == nil || !current.ExpiresAt.Before(storeNow.Time) {
		return store.WarmAttachExpiry{}, errors.New("warm attach is not expirable")
	}
	deliveryState, blocker := store.AttachmentDeliveryPending, &store.AttentionBlocker{Kind: store.AttentionBlockerReauthorizationRequired}
	if current.DeliveryState != store.AttachmentDeliveryPending {
		deliveryState = store.AttachmentDeliveryOutcomeUnknown
		operation := "credential_handoff"
		blocker = &store.AttentionBlocker{Kind: store.AttentionBlockerOutcomeUnknown, Operation: &operation}
	}
	updatedRow, err := queries.UpdateAttachment(ctx, db.UpdateAttachmentParams{
		Status: string(store.AttachmentReauthorizationRequired), DeliveryState: string(deliveryState),
		AttachID: attachID, ExpectedVersion: expectedDeliveryVersion,
	})
	if err != nil {
		return store.WarmAttachExpiry{}, fmt.Errorf("expire warm attach attachment: %w", err)
	}
	updated := attachment(updatedRow)
	if err := upsertAttentionLedger(ctx, queries, updated.Identity.TargetSessionID, blocker, nil); err != nil {
		return store.WarmAttachExpiry{}, err
	}
	summary, err := warmAttachSummary(ctx, queries, updated.Identity.TargetSessionID)
	if err != nil {
		return store.WarmAttachExpiry{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.WarmAttachExpiry{}, fmt.Errorf("commit warm attach expiry: %w", err)
	}
	return store.WarmAttachExpiry{Attachment: updated, Summary: summary}, nil
}

func validateWarmAttachRequest(request store.WarmAttachRequest) error {
	if err := validateAttachAttempt(request.Attempt); err != nil {
		return err
	}
	if request.Attempt.Outcome != store.AttachAttemptAccepted || request.Attempt.IssuedCredentialGeneration == nil ||
		*request.Attempt.IssuedCredentialGeneration != request.TargetActivation.Generation ||
		request.Attachment.Identity.AttachID != request.Attempt.Identity.AttachID ||
		request.Attachment.Identity.BootstrapSessionID != request.Attempt.Identity.BootstrapSessionID ||
		request.Attachment.Identity.TargetSessionID != request.Attempt.Identity.TargetSessionID ||
		validateAttachmentIdentity(request.Attachment.Identity) != nil ||
		request.BootstrapAdmission.CredentialGeneration < 1 || request.BootstrapAdmission.ConnectionEpoch < 1 ||
		request.BootstrapAdmission.AcceptedFence < 1 || request.BootstrapAdmission.GrantFence <= request.BootstrapAdmission.AcceptedFence ||
		!validAttachmentText(request.FirstDelivery.CommandID, 256) || !validAttachmentText(request.FirstDelivery.ReferenceID, 255) ||
		request.FirstDelivery.ReferenceDigest == ([32]byte{}) || request.TargetActivation.Generation < 1 || request.TargetActivation.ExpiresAt.IsZero() || request.Attachment.ExpiresAt.IsZero() ||
		!samePostgresTimestamp(request.TargetActivation.ExpiresAt, request.Attachment.ExpiresAt) ||
		!samePostgresTimestamp(request.FirstDelivery.ExpiresAt, request.Attachment.ExpiresAt) {
		return errors.New("invalid warm attach request")
	}
	return nil
}

func validWarmAttachExpiry(request store.WarmAttachRequest, storeNow time.Time) bool {
	return request.Attempt.ExpiresAt.After(storeNow) && request.Attempt.ExpiresAt.Before(storeNow.Add(maxAttachAttemptTTL).Add(time.Nanosecond)) &&
		validAttachmentExpiry(request.Attachment.ExpiresAt, storeNow) && !request.Attachment.ExpiresAt.After(request.Attempt.ExpiresAt)
}

func attachAttemptParams(request store.AttachAttemptRequest) db.InsertAttachAttemptParams {
	return db.InsertAttachAttemptParams{
		AttemptJtiHash: request.Identity.JTIHash[:], AttachID: request.Identity.AttachID, BootstrapSessionID: request.Identity.BootstrapSessionID,
		TargetSessionID: request.Identity.TargetSessionID, Provider: request.Identity.Provider, FingerprintDomain: request.Fingerprint.Domain,
		FingerprintVersion: int32(request.Fingerprint.Version), FingerprintDigest: request.Fingerprint.Digest[:], FingerprintKeyVersion: int32(request.Fingerprint.KeyVersion),
		ExpiresAt: pgtype.Timestamptz{Time: request.ExpiresAt, Valid: true}, AdmissionOutcome: string(request.Outcome),
		IssuedCredentialGeneration: nullableInt64(request.IssuedCredentialGeneration),
	}
}

func warmAttachEventPayload(delivery store.WarmAttachFirstDelivery) []byte {
	payload, _ := json.Marshal(struct {
		Role            string `json:"role"`
		ReferenceID     string `json:"reference_id"`
		ReferenceDigest string `json:"reference_digest"`
	}{Role: "user", ReferenceID: delivery.ReferenceID, ReferenceDigest: hex.EncodeToString(delivery.ReferenceDigest[:])})
	return payload
}

func warmAttachDuplicate(ctx context.Context, queries *db.Queries, attempt store.AttachAttempt, request store.WarmAttachRequest) (store.WarmAttachCommit, error) {
	if !sameAttachAttempt(attempt, request.Attempt) {
		return store.WarmAttachCommit{}, errors.New("warm attach is immutable")
	}
	attachmentRow, err := queries.AttachmentByID(ctx, request.Attachment.Identity.AttachID)
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("load existing warm attach attachment: %w", err)
	}
	attachment := attachment(attachmentRow)
	if attachment.Identity != request.Attachment.Identity {
		return store.WarmAttachCommit{}, errors.New("warm attach attachment is immutable")
	}
	connectionRow, err := queries.AdapterConnectionByID(ctx, request.Attachment.Identity.TargetSessionID)
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("load warm attach target credential: %w", err)
	}
	connection := adapterConnection(connectionRow)
	if connection.SessionID != request.Attachment.Identity.TargetSessionID || connection.ConnectionEpoch != 0 || connection.AcceptedFence != 0 ||
		connection.ActiveCredentialGeneration != request.TargetActivation.Generation || connection.CredentialGenerationHighWatermark != request.TargetActivation.Generation ||
		!samePostgresTimestamp(connection.ActiveCredentialExpiresAt, request.TargetActivation.ExpiresAt) || connection.PendingCredentialGeneration != nil ||
		connection.PriorRecoveryGeneration != nil || connection.RotationID != nil || connection.RevokedAt != nil || connection.TerminalAt != nil {
		return store.WarmAttachCommit{}, errors.New("warm attach target credential is immutable")
	}
	commandRow, err := queries.PendingCommandByID(ctx, db.PendingCommandByIDParams{SessionID: attachment.Identity.TargetSessionID, CmdID: request.FirstDelivery.CommandID})
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("load existing warm attach outbox: %w", err)
	}
	event, err := queries.WarmAttachEventByTargetSeq(ctx, db.WarmAttachEventByTargetSeqParams{SessionID: attachment.Identity.TargetSessionID, EventSeq: commandRow.EventSeq})
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("load existing warm attach event: %w", err)
	}
	if commandRow.Type != "session.send" || !samePostgresTimestamp(commandRow.ExpiresAt.Time, request.FirstDelivery.ExpiresAt) ||
		!sameWarmAttachEvent(event.Type, event.Payload, request.FirstDelivery) {
		return store.WarmAttachCommit{}, errors.New("warm attach first delivery is immutable")
	}
	summary, err := warmAttachSummary(ctx, queries, attachment.Identity.TargetSessionID)
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	return store.WarmAttachCommit{Attempt: attempt, Attachment: attachment, Outbox: store.WarmAttachOutbox{
		TargetSessionID: commandRow.SessionID, CommandID: commandRow.CmdID, EventSeq: commandRow.EventSeq,
		ReferenceID: request.FirstDelivery.ReferenceID, ReferenceDigest: request.FirstDelivery.ReferenceDigest, ExpiresAt: commandRow.ExpiresAt.Time,
	}, TargetActivation: request.TargetActivation, Summary: summary, Duplicate: true}, nil
}

func sameWarmAttachEvent(eventType string, payload []byte, delivery store.WarmAttachFirstDelivery) bool {
	var event struct {
		Role            string `json:"role"`
		ReferenceID     string `json:"reference_id"`
		ReferenceDigest string `json:"reference_digest"`
	}
	return eventType == "session.message" && json.Unmarshal(payload, &event) == nil && event.Role == "user" &&
		event.ReferenceID == delivery.ReferenceID && event.ReferenceDigest == hex.EncodeToString(delivery.ReferenceDigest[:])
}

func warmAttachSummary(ctx context.Context, queries *db.Queries, sessionID string) (store.SessionAttentionSummary, error) {
	rows, err := queries.AttentionSnapshot(ctx, []string{sessionID})
	if err != nil {
		return store.SessionAttentionSummary{}, fmt.Errorf("load warm attach attention summary: %w", err)
	}
	if len(rows) != 1 {
		return store.SessionAttentionSummary{}, errors.New("warm attach attention summary is missing")
	}
	return attentionSummary(rows[0])
}

func (s *Store) ReserveWorkspaceLease(ctx context.Context, reserve store.WorkspaceLeaseReserve) (store.WorkspaceLease, error) {
	if s.pool == nil {
		return store.WorkspaceLease{}, errors.New("postgres event store pool is nil")
	}
	reserve = normalizeWorkspaceLeaseReserve(reserve)
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

// RecordProviderStartAdmission uses one PostgreSQL transaction to lock the
// exact Adapter connection and its sole reserved writer lease before recording
// start_received. The Hub derives the lease from durable tuple truth; the
// Adapter cannot nominate a workspace or bypass quarantine.
func (s *Store) RecordProviderStartAdmission(ctx context.Context, request store.ProviderStartAdmission) (store.WorkspaceLease, error) {
	return s.WithProviderStartAdmission(ctx, request, nil)
}

// WithProviderStartAdmission keeps the Store's connection and writer-lease
// locks through the physical start proof. Its callback cannot inspect or
// mutate Store state, keeping workspace selection Store-owned.
func (s *Store) WithProviderStartAdmission(ctx context.Context, request store.ProviderStartAdmission, callback func(context.Context) error) (store.WorkspaceLease, error) {
	if s.pool == nil || !validConnectionID(request.SessionID) || request.Writer.LeaseID == "" ||
		request.Writer.ConnectionEpoch != request.Admission.ConnectionEpoch || request.Writer.CredentialGeneration != request.Admission.CredentialGeneration {
		return store.WorkspaceLease{}, errors.New("invalid provider start admission")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("begin provider start admission: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	bound := &Store{connectionTx: tx}
	if _, err := bound.ValidateAdapterAdmission(ctx, request.SessionID, request.Admission); err != nil {
		return store.WorkspaceLease{}, errors.New("provider start authority lost")
	}
	rows, err := tx.Query(ctx, `SELECT workspace_key, version, worker_id, status FROM session_workspace_leases
WHERE session_id=$1 AND connection_epoch=$2 AND credential_generation=$3 AND lease_id=$4 AND status IN ('reserved', 'start_received')
	  AND reservation_expires_at > clock_timestamp()
  AND (child_scope_expires_at IS NULL OR child_scope_expires_at > clock_timestamp())
FOR UPDATE`, request.SessionID, request.Writer.ConnectionEpoch, request.Writer.CredentialGeneration, request.Writer.LeaseID)
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("lock provider workspace lease: %w", err)
	}
	defer rows.Close()
	var key string
	var version int64
	var workerID string
	var status string
	if !rows.Next() || rows.Scan(&key, &version, &workerID, &status) != nil || rows.Next() || rows.Err() != nil {
		return store.WorkspaceLease{}, errors.New("provider start admission is unavailable")
	}
	rows.Close()
	if rows.Err() != nil {
		return store.WorkspaceLease{}, errors.New("provider start admission is unavailable")
	}
	if status == string(store.WorkspaceLeaseStartReceived) && !request.ReAdmission {
		return store.WorkspaceLease{}, errors.New("provider start admission is unavailable")
	}
	if callback != nil {
		if err := callback(ctx); err != nil {
			return store.WorkspaceLease{}, fmt.Errorf("provider start callback: %w", err)
		}
	}
	var leaseCurrent bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
	SELECT 1 FROM session_workspace_leases
	WHERE workspace_key=$1 AND session_id=$2 AND connection_epoch=$3 AND credential_generation=$4 AND lease_id=$5 AND status=$6
	  AND reservation_expires_at > clock_timestamp()
	  AND (child_scope_expires_at IS NULL OR child_scope_expires_at > clock_timestamp())
)`, key, request.SessionID, request.Writer.ConnectionEpoch, request.Writer.CredentialGeneration, request.Writer.LeaseID, status).Scan(&leaseCurrent); err != nil || !leaseCurrent {
		return store.WorkspaceLease{}, errors.New("provider start admission is unavailable")
	}
	var row db.SessionWorkspaceLease
	if status == string(store.WorkspaceLeaseReserved) {
		row, err = db.New(tx).RecordWorkspaceStartReceived(ctx, db.RecordWorkspaceStartReceivedParams{
			WorkspaceKey: key, ExpectedVersion: version, WorkerID: workerID, SessionID: request.SessionID,
			ConnectionEpoch: request.Writer.ConnectionEpoch, CredentialGeneration: request.Writer.CredentialGeneration, LeaseID: request.Writer.LeaseID,
		})
		if err != nil {
			return store.WorkspaceLease{}, errors.New("provider start admission is unavailable")
		}
	} else {
		row, err = db.New(tx).WorkspaceLeaseByKey(ctx, key)
		if err != nil || row.Status != string(store.WorkspaceLeaseStartReceived) {
			return store.WorkspaceLease{}, errors.New("provider start admission is unavailable")
		}
	}
	if _, err := bound.ValidateAdapterAdmission(ctx, request.SessionID, request.Admission); err != nil {
		return store.WorkspaceLease{}, errors.New("provider start authority lost")
	}
	lease, err := workspaceLease(row)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("commit provider start admission: %w", err)
	}
	return lease, nil
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
	if lease.Key != reserve.Key || lease.Owner != reserve.Owner || !samePostgresTimestamp(lease.ExpiresAt, reserve.ExpiresAt) {
		return false
	}
	left, right := lease.ChildScope, reserve.ChildScope
	return (left == nil && right == nil) || (left != nil && right != nil && left.ParentKey == right.ParentKey &&
		left.CapabilityDigest == right.CapabilityDigest && samePostgresTimestamp(left.ExpiresAt, right.ExpiresAt))
}

// PostgreSQL timestamptz is stored at microsecond precision. Identity and
// idempotency comparisons must use the same durable representation.
func samePostgresTimestamp(left, right time.Time) bool {
	return left.UTC().Truncate(time.Microsecond).Equal(right.UTC().Truncate(time.Microsecond))
}

func nullableInt64(value *int64) pgtype.Int8 {
	if value == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *value, Valid: true}
}

// PostgreSQL stores timestamptz values at microsecond precision. Normalize
// request timestamps before validation and idempotency comparisons so an exact
// retry cannot look like a distinct later expiry after its first write.
func postgresTimestamp(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func normalizeWarmAttachRequest(request store.WarmAttachRequest) store.WarmAttachRequest {
	request.Attempt.ExpiresAt = postgresTimestamp(request.Attempt.ExpiresAt)
	request.Attachment.ExpiresAt = postgresTimestamp(request.Attachment.ExpiresAt)
	request.TargetActivation.ExpiresAt = postgresTimestamp(request.TargetActivation.ExpiresAt)
	request.FirstDelivery.ExpiresAt = postgresTimestamp(request.FirstDelivery.ExpiresAt)
	return request
}

func normalizeWorkspaceLeaseReserve(reserve store.WorkspaceLeaseReserve) store.WorkspaceLeaseReserve {
	reserve.ExpiresAt = postgresTimestamp(reserve.ExpiresAt)
	if reserve.ChildScope != nil {
		childScope := *reserve.ChildScope
		childScope.ExpiresAt = postgresTimestamp(childScope.ExpiresAt)
		reserve.ChildScope = &childScope
	}
	return reserve
}

func normalizeAttachmentUpdate(update store.AttachmentUpdate) store.AttachmentUpdate {
	if update.ExpiresAt != nil {
		expiresAt := postgresTimestamp(*update.ExpiresAt)
		update.ExpiresAt = &expiresAt
	}
	if update.Blocker != nil && update.Blocker.ExpiresAt != nil {
		blocker := *update.Blocker
		expiresAt := postgresTimestamp(*blocker.ExpiresAt)
		blocker.ExpiresAt = &expiresAt
		update.Blocker = &blocker
	}
	return update
}

func (s *Store) CreateAttachment(ctx context.Context, request store.AttachmentCreate) (store.AttachmentCommit, error) {
	if s.pool == nil {
		return store.AttachmentCommit{}, errors.New("postgres event store pool is nil")
	}
	request.ExpiresAt = postgresTimestamp(request.ExpiresAt)
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
	update = normalizeAttachmentUpdate(update)
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
		if current.ExpiresAt != nil && update.ExpiresAt.UTC().Truncate(time.Microsecond).After(current.ExpiresAt.UTC().Truncate(time.Microsecond)) {
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
		if expiry && (blocker.ExpiresAt == nil || update.ExpiresAt == nil || !samePostgresTimestamp(*blocker.ExpiresAt, *update.ExpiresAt)) {
			return false
		}
		if blockingSession && (blocker.BlockingSessionID == nil || update.BlockingSessionID == nil || *blocker.BlockingSessionID != *update.BlockingSessionID) {
			return false
		}
		return true
	}

	valid := false
	switch update.Status {
	case store.AttachmentJoinPending:
		valid = current.Status == store.AttachmentJoinPending && update.QueueReason == nil && update.ExpiresAt != nil && current.ExpiresAt != nil && samePostgresTimestamp(*update.ExpiresAt, *current.ExpiresAt) && update.BlockingSessionID == nil && update.Blocker == nil && (update.DeliveryState == store.AttachmentDeliveryPending || update.DeliveryState == store.AttachmentDeliveryReceived || update.DeliveryState == store.AttachmentDeliveryCompleted)
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
	return value != nil && (*value == "start" || *value == "command" || *value == "credential_handoff")
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
