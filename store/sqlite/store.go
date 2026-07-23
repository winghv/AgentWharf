package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/winghv/agentwharf/store"
	sqliteDriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	maxHistoryPageSize          = 100
	maxEventPayloadSize         = 64 * 1024
	maxAttachAttemptTTL         = 5 * time.Minute
	maxAttachAttemptCleanupRows = 128
)

type Store struct {
	db               *sql.DB
	fenceDB          *sql.DB
	connectionTx     *sql.Tx
	attentionPageNow func(context.Context, *sql.DB) (int64, error)
	closed           atomic.Bool
}

func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite event store: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	fencePath := path + ".fences"
	if _, err := db.ExecContext(ctx, `ATTACH DATABASE ? AS fence_store`, fencePath); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("attach sqlite fence store: %w", err)
	}
	fenceDB, err := sql.Open("sqlite", fencePath)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite fence store: %w", err)
	}
	fenceDB.SetMaxOpenConns(1)
	fenceDB.SetMaxIdleConns(1)
	st := &Store{db: db, fenceDB: fenceDB}
	if err := st.init(ctx); err != nil {
		_ = st.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Backups require all Store handles closed; online copies are unsupported, and the main DB plus .fences form one unit.
	var next int64
	syncErr := s.db.QueryRow(`SELECT side.next_fence FROM session_adapter_fence_allocator AS main, fence_store.adapter_fence_allocator AS side WHERE main.singleton = 1 AND side.singleton = 1 AND typeof(main.next_fence) = 'integer' AND typeof(side.next_fence) = 'integer' AND side.next_fence >= main.next_fence`).Scan(&next)
	if syncErr == nil {
		syncErr = syncAdapterFence(context.Background(), s.db, next-1)
	} else if errors.Is(syncErr, sql.ErrConnDone) {
		syncErr = nil
	}
	return errors.Join(syncErr, s.db.Close(), s.fenceDB.Close())
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
		nowMS, err := sqliteNowMillis(ctx, tx)
		if err != nil {
			return 0, err
		}
		terminal, err := projectSQLiteAttentionEvent(ctx, tx, sessionID, seq, ev, nowMS)
		if err != nil {
			return 0, fmt.Errorf("project attention event seq %d: %w", seq, err)
		}
		if terminal {
			if _, err := tx.ExecContext(ctx, `UPDATE session_adapter_connections SET revoked_at_ms = ?, terminal_at_ms = ?, updated_at_ms = ? WHERE session_id = ? AND terminal_at_ms IS NULL`, nowMS, nowMS, nowMS, sessionID); err != nil {
				return 0, fmt.Errorf("fence terminal attention event: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit append transaction: %w", err)
	}
	return firstSeq, nil
}

func (s *Store) AttentionSnapshot(ctx context.Context, sessionIDs []string) ([]store.SessionAttentionSummary, error) {
	if len(sessionIDs) == 0 || len(sessionIDs) > 100 {
		return nil, errors.New("attention snapshot session IDs are out of range")
	}
	seen := make(map[string]struct{}, len(sessionIDs))
	args := make([]any, len(sessionIDs))
	for index, sessionID := range sessionIDs {
		if !validConnectionID(sessionID) {
			return nil, errors.New("attention snapshot session ID is invalid")
		}
		if _, exists := seen[sessionID]; exists {
			return nil, errors.New("attention snapshot session IDs must be unique")
		}
		seen[sessionID] = struct{}{}
		args[index] = sessionID
	}
	pending, err := s.attentionMigrationPending(ctx, s.db)
	if err != nil {
		return nil, err
	}
	if pending {
		summaries := make([]store.SessionAttentionSummary, len(sessionIDs))
		for index, sessionID := range sessionIDs {
			summaries[index] = store.SessionAttentionSummary{SessionID: sessionID, State: "starting", StateOfProjection: store.AttentionProjectionIncomplete}
		}
		return summaries, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id IN (`+sqlitePlaceholders(len(args))+`) ORDER BY session_id ASC`, args...)
	if err != nil {
		return nil, fmt.Errorf("select attention snapshot: %w", err)
	}
	defer rows.Close()
	summaries := make([]store.SessionAttentionSummary, 0, len(sessionIDs))
	for rows.Next() {
		summary, err := querySQLiteAttentionSummary(ctx, rows)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate attention snapshot: %w", err)
	}
	return summaries, nil
}

func (s *Store) AttentionSummaryPage(ctx context.Context, request store.AttentionSummaryPageRequest) (store.AttentionSummaryPage, error) {
	if request.Limit < 1 || request.Limit > store.MaxAttentionSummaryPageSize {
		return store.AttentionSummaryPage{}, errors.New("attention summary page limit is out of range")
	}
	if request.AfterSessionID != "" && !validConnectionID(request.AfterSessionID) {
		return store.AttentionSummaryPage{}, errors.New("attention summary page cursor is invalid")
	}
	pending, err := s.attentionMigrationPending(ctx, s.db)
	if err != nil {
		return store.AttentionSummaryPage{}, err
	}
	if pending {
		return store.AttentionSummaryPage{}, errors.New("attention summary page is unavailable during migration")
	}
	pageNow := s.attentionPageNow
	if pageNow == nil {
		pageNow = func(ctx context.Context, db *sql.DB) (int64, error) { return sqliteNowMillis(ctx, db) }
	}
	snapshotMS, err := pageNow(ctx, s.db)
	if err != nil {
		return store.AttentionSummaryPage{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id > ? AND EXISTS (SELECT 1 FROM session_adapter_connections AS authority WHERE authority.session_id = session_attention_summaries.session_id AND authority.active_credential_expires_at_ms > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) AND authority.revoked_at_ms IS NULL AND authority.terminal_at_ms IS NULL) ORDER BY session_id ASC LIMIT ?`, request.AfterSessionID, request.Limit+1)
	if err != nil {
		return store.AttentionSummaryPage{}, fmt.Errorf("select attention summary page: %w", err)
	}
	defer rows.Close()
	page := store.AttentionSummaryPage{Summaries: make([]store.SessionAttentionSummary, 0, request.Limit), SnapshotAt: time.UnixMilli(snapshotMS).UTC()}
	for rows.Next() {
		summary, err := querySQLiteAttentionSummary(ctx, rows)
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
	if err := rows.Err(); err != nil {
		return store.AttentionSummaryPage{}, fmt.Errorf("iterate attention summary page: %w", err)
	}
	if err := rows.Close(); err != nil {
		return store.AttentionSummaryPage{}, fmt.Errorf("close attention summary page: %w", err)
	}
	return page, nil
}

func (s *Store) attentionMigrationPending(ctx context.Context, executor sqliteConnectionExecutor) (bool, error) {
	var state string
	if err := executor.QueryRowContext(ctx, `SELECT state FROM session_attention_migration WHERE singleton = 1`).Scan(&state); err != nil {
		return false, fmt.Errorf("read sqlite attention migration marker: %w", err)
	}
	if state != "pending" && state != "complete" {
		return false, errors.New("sqlite attention migration marker is invalid")
	}
	return state == "pending", nil
}

const maxSQLiteAttentionBackfillBatch = 256

type AttentionBackfillResult struct {
	Checkpoint string
	Processed  int
	Done       bool
}

func (s *Store) BackfillAttentionBatch(ctx context.Context, limit int) (AttentionBackfillResult, error) {
	if limit < 1 || limit > maxSQLiteAttentionBackfillBatch {
		return AttentionBackfillResult{}, errors.New("sqlite attention backfill batch size is out of range")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AttentionBackfillResult{}, fmt.Errorf("begin sqlite attention backfill: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var checkpoint, state string
	if err := tx.QueryRowContext(ctx, `SELECT checkpoint_session_id, state FROM session_attention_migration WHERE singleton = 1`).Scan(&checkpoint, &state); err != nil || state != "pending" {
		return AttentionBackfillResult{}, errors.New("sqlite attention migration is not pending")
	}
	rows, err := tx.QueryContext(ctx, `SELECT session_id FROM (
SELECT session_id FROM session_events UNION SELECT target_session_id FROM session_attachments UNION SELECT session_id FROM session_adapter_connections
) WHERE session_id > ? ORDER BY session_id LIMIT ?`, checkpoint, limit+1)
	if err != nil {
		return AttentionBackfillResult{}, fmt.Errorf("select sqlite attention backfill sessions: %w", err)
	}
	defer rows.Close()
	sessions := make([]string, 0, limit+1)
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil || !validConnectionID(sessionID) {
			return AttentionBackfillResult{}, errors.New("sqlite attention backfill session is invalid")
		}
		sessions = append(sessions, sessionID)
	}
	if err := rows.Err(); err != nil {
		return AttentionBackfillResult{}, fmt.Errorf("iterate sqlite attention backfill sessions: %w", err)
	}
	result := AttentionBackfillResult{Checkpoint: checkpoint, Done: len(sessions) <= limit}
	if len(sessions) > limit {
		sessions = sessions[:limit]
	}
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return AttentionBackfillResult{}, err
	}
	for _, sessionID := range sessions {
		var latest int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE session_id = ?`, sessionID).Scan(&latest); err != nil {
			return AttentionBackfillResult{}, err
		}
		summary, err := querySQLiteAttentionSummary(ctx, tx.QueryRowContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id = ?`, sessionID))
		if errors.Is(err, sql.ErrNoRows) {
			summary = store.SessionAttentionSummary{SessionID: sessionID, LatestSeq: latest, State: "starting", StateOfProjection: store.AttentionProjectionIncomplete}
		} else if err != nil {
			return AttentionBackfillResult{}, fmt.Errorf("load sqlite attention backfill summary: %w", err)
		} else {
			summary.LatestSeq = max(summary.LatestSeq, latest)
			summary.StateOfProjection = store.AttentionProjectionIncomplete
		}
		if err := upsertSQLiteAttentionSummary(ctx, tx, summary, nowMS); err != nil {
			return AttentionBackfillResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE session_adapter_connections SET revoked_at_ms = COALESCE(revoked_at_ms, ?), terminal_at_ms = COALESCE(terminal_at_ms, ?), updated_at_ms = ? WHERE session_id = ?`, nowMS, nowMS, nowMS, sessionID); err != nil {
			return AttentionBackfillResult{}, err
		}
		result.Checkpoint = sessionID
		result.Processed++
	}
	if result.Done {
		if _, err := tx.ExecContext(ctx, `UPDATE session_attention_migration SET state = 'complete', checkpoint_session_id = ? WHERE singleton = 1`, result.Checkpoint); err != nil {
			return AttentionBackfillResult{}, err
		}
	} else if _, err := tx.ExecContext(ctx, `UPDATE session_attention_migration SET checkpoint_session_id = ? WHERE singleton = 1`, result.Checkpoint); err != nil {
		return AttentionBackfillResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AttentionBackfillResult{}, fmt.Errorf("commit sqlite attention backfill: %w", err)
	}
	return result, nil
}

type sqliteAttentionProjection struct {
	state                *string
	stateObserved        bool
	permissionID         *string
	permissionDecisionID *string
	permissionChange     bool
	terminalOutcome      *string
	projectionIncomplete bool
	terminal             bool
}

func projectSQLiteAttentionEvent(ctx context.Context, tx *sql.Tx, sessionID string, seq int64, event store.PendingEvent, nowMS int64) (bool, error) {
	summary, err := querySQLiteAttentionSummary(ctx, tx.QueryRowContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id = ?`, sessionID))
	created := false
	if errors.Is(err, sql.ErrNoRows) {
		summary = store.SessionAttentionSummary{SessionID: sessionID, State: "starting", StateOfProjection: store.AttentionProjectionIncomplete}
		created = true
	} else if err != nil {
		return false, fmt.Errorf("load attention summary: %w", err)
	}
	projection := sqliteAttentionEventProjection(event)
	if summary.LatestSeq > 0 && seq != summary.LatestSeq+1 {
		projection.projectionIncomplete = true
	}
	summary.LatestSeq = seq
	if projection.state != nil {
		summary.State = *projection.state
		change := seq
		summary.LatestChangeSeq = &change
	}
	if projection.permissionChange {
		if projection.permissionID != nil {
			summary.Permission = &store.AttentionPermission{ID: *projection.permissionID, Status: store.AttentionPermissionPending}
		} else if summary.Permission != nil && projection.permissionDecisionID != nil && summary.Permission.ID == *projection.permissionDecisionID {
			summary.Permission = nil
		}
	}
	if summary.TerminalOutcome == nil && projection.terminalOutcome != nil {
		outcome := *projection.terminalOutcome
		summary.TerminalOutcome = &outcome
	}
	if summary.LatestSeq == 1 && (!projection.stateObserved || projection.state == nil) {
		projection.projectionIncomplete = true
	}
	if (!created && summary.StateOfProjection == store.AttentionProjectionIncomplete) || projection.projectionIncomplete ||
		(projection.stateObserved && projection.state == nil) {
		summary.StateOfProjection = store.AttentionProjectionIncomplete
	} else {
		summary.StateOfProjection = store.AttentionProjectionComplete
	}
	durableAt := time.UnixMilli(nowMS)
	summary.LastDurableEventAt = &durableAt
	if err := upsertSQLiteAttentionSummary(ctx, tx, summary, nowMS); err != nil {
		return false, err
	}
	return projection.terminal, nil
}
func sqliteAttentionEventProjection(event store.PendingEvent) sqliteAttentionProjection {
	projection := sqliteAttentionProjection{}
	if event.Type == "permission.request" {
		var payload struct {
			RequestID string `json:"request_id"`
		}
		if json.Unmarshal(event.Payload, &payload) != nil || !validConnectionID(payload.RequestID) {
			projection.projectionIncomplete = true
			return projection
		}
		projection.permissionID, projection.permissionChange = &payload.RequestID, true
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
		projection.permissionDecisionID, projection.permissionChange = &payload.RequestID, true
		return projection
	}
	if event.Type == "session.error" {
		state, outcome := "error", "error"
		return sqliteAttentionProjection{state: &state, stateObserved: true, terminalOutcome: &outcome, terminal: true}
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
		state := "busy"
		projection.state = &state
	case "starting", "ready", "busy", "waiting_permission", "recovering", "ended", "error":
		projection.state = &payload.State
	default:
		projection.projectionIncomplete = true
		return projection
	}
	if *projection.state == "ended" || *projection.state == "error" {
		outcome := *projection.state
		projection.terminal, projection.terminalOutcome = true, &outcome
	}
	return projection
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

func (s *Store) History(ctx context.Context, sessionID string, beforeSeq *int64, limit int) (store.HistoryPage, error) {
	if limit < 1 || limit > maxHistoryPageSize {
		return store.HistoryPage{}, fmt.Errorf("history limit must be between 1 and %d", maxHistoryPageSize)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return store.HistoryPage{}, fmt.Errorf("begin history transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var latestSeq, earliestSeq, eventCount int64
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(seq), 0), COALESCE(MIN(seq), 0), COUNT(*)
FROM session_events
WHERE session_id = ?
`, sessionID).Scan(&latestSeq, &earliestSeq, &eventCount); err != nil {
		return store.HistoryPage{}, fmt.Errorf("select history bounds: %w", err)
	}

	var rows *sql.Rows
	if beforeSeq == nil {
		rows, err = tx.QueryContext(ctx, `
SELECT session_id, seq, type, payload, event_time_ms
FROM session_events
WHERE session_id = ?
ORDER BY seq DESC
LIMIT ?
`, sessionID, limit+1)
	} else {
		rows, err = tx.QueryContext(ctx, `
SELECT session_id, seq, type, payload, event_time_ms
FROM session_events
WHERE session_id = ? AND seq < ?
ORDER BY seq DESC
LIMIT ?
`, sessionID, *beforeSeq, limit+1)
	}
	if err != nil {
		return store.HistoryPage{}, fmt.Errorf("query history events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	events := make([]store.Event, 0, limit+1)
	for rows.Next() {
		var (
			event       store.Event
			payload     []byte
			eventTimeMS int64
		)
		if err := rows.Scan(&event.SessionID, &event.Seq, &event.Type, &payload, &eventTimeMS); err != nil {
			return store.HistoryPage{}, fmt.Errorf("scan history event: %w", err)
		}
		event.Time = time.UnixMilli(eventTimeMS)
		event.Payload = append(event.Payload[:0], payload...)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return store.HistoryPage{}, fmt.Errorf("iterate history events: %w", err)
	}

	page := store.HistoryPage{
		LatestSeq:      latestSeq,
		RetentionState: store.RetentionComplete,
	}
	if eventCount > 0 && (earliestSeq > 1 || eventCount != latestSeq-earliestSeq+1) {
		page.RetentionState = store.RetentionGap
	}
	if len(events) > limit {
		nextBeforeSeq := events[limit-1].Seq
		page.NextBeforeSeq = &nextBeforeSeq
		events = events[:limit]
	}
	page.Events = make([]store.Event, len(events))
	for index := range events {
		page.Events[len(events)-1-index] = events[index]
	}
	return page, nil
}

func (s *Store) CommitPendingCommand(ctx context.Context, sessionID string, authority store.CommandAuthority, event store.PendingEvent, request store.PendingCommandRequest) (store.PendingCommandCommit, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("begin pending command transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.PendingCommandCommit{}, err
	}
	if err := validatePendingCommandInput(event, request, time.UnixMilli(nowMS)); err != nil {
		return store.PendingCommandCommit{}, err
	}
	if err := validateCommandAuthority(ctx, tx, sessionID, authority, nowMS); err != nil {
		return store.PendingCommandCommit{}, err
	}

	existing, err := queryPendingCommand(ctx, tx, sessionID, request.CommandID, nowMS)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return store.PendingCommandCommit{}, fmt.Errorf("commit duplicate pending command lookup: %w", err)
		}
		return store.PendingCommandCommit{Command: existing, Duplicate: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.PendingCommandCommit{}, fmt.Errorf("select pending command: %w", err)
	}

	var latest int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE session_id = ?`, sessionID).Scan(&latest); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("select pending command event seq: %w", err)
	}
	seq := latest + 1
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?)
`, sessionID, seq, event.Type, []byte(event.Payload), event.Time.UnixMilli(), nowMS); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("append pending command event: %w", err)
	}
	if terminal, err := projectSQLiteAttentionEvent(ctx, tx, sessionID, seq, event, nowMS); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("project pending command attention event: %w", err)
	} else if terminal {
		return store.PendingCommandCommit{}, errors.New("pending command event must not be terminal")
	}

	nowMS, err = sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.PendingCommandCommit{}, err
	}
	if err := validatePendingCommandInput(event, request, time.UnixMilli(nowMS)); err != nil {
		return store.PendingCommandCommit{}, err
	}
	if err := validateCommandAuthority(ctx, tx, sessionID, authority, nowMS); err != nil {
		return store.PendingCommandCommit{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_pending_commands (
    session_id, cmd_id, type, event_seq, status, expires_at_ns, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
`, sessionID, request.CommandID, request.Type, seq, request.ExpiresAt.UnixNano(), nowMS, nowMS); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("insert pending command: %w", err)
	}
	if err := upsertSQLiteAttentionLedger(ctx, tx, sessionID, nil, &nowMS); err != nil {
		return store.PendingCommandCommit{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("commit pending command: %w", err)
	}
	return store.PendingCommandCommit{Command: store.PendingCommand{
		SessionID: sessionID, CommandID: request.CommandID, Type: request.Type, EventSeq: seq,
		Status: store.PendingCommandPending, ExpiresAt: request.ExpiresAt,
	}}, nil
}

func (s *Store) PublishSettingsCapability(ctx context.Context, sessionID string, update store.SettingsCapabilityUpdate) (store.SettingsCapability, error) {
	if !validSettingsCapabilityUpdate(sessionID, update) {
		return store.SettingsCapability{}, errors.New("invalid settings capability")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.SettingsCapability{}, fmt.Errorf("begin settings capability transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.SettingsCapability{}, err
	}
	if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: update.Writer.ConnectionEpoch, CredentialGeneration: update.Writer.CredentialGeneration}); err != nil {
		return store.SettingsCapability{}, err
	}
	if err := validateLiveSettingsWriter(ctx, tx, sessionID, update.Writer); err != nil {
		return store.SettingsCapability{}, err
	}
	if err := verifySettingsCapabilityEvent(ctx, tx, sessionID, update.EventSeq); err != nil {
		return store.SettingsCapability{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_settings_capabilities
(session_id, capability_event_seq, fingerprint, effective_model_id, effective_permission_mode_id, capability_version, writer_connection_epoch, writer_credential_generation, writer_lease_id, created_at_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET capability_event_seq=excluded.capability_event_seq, fingerprint=excluded.fingerprint, effective_model_id=excluded.effective_model_id,
 effective_permission_mode_id=excluded.effective_permission_mode_id, capability_version=session_settings_capabilities.capability_version+1,
 writer_connection_epoch=excluded.writer_connection_epoch, writer_credential_generation=excluded.writer_credential_generation,
 writer_lease_id=excluded.writer_lease_id, updated_at_ms=excluded.updated_at_ms
`, sessionID, update.EventSeq, update.Fingerprint, update.EffectiveModelID, update.EffectivePermissionModeID, update.Writer.ConnectionEpoch, update.Writer.CredentialGeneration, update.Writer.LeaseID, nowMS, nowMS); err != nil {
		return store.SettingsCapability{}, fmt.Errorf("upsert settings capability: %w", err)
	}
	capability, err := querySettingsCapability(ctx, tx, sessionID)
	if err != nil {
		return store.SettingsCapability{}, err
	}
	return capability, tx.Commit()
}

func (s *Store) SettingsCommandReserve(ctx context.Context, sessionID string, request store.SettingsCommandRequest) (store.SettingsCommandReserve, error) {
	if !validSettingsCommandRequest(sessionID, request) {
		return store.SettingsCommandReserve{}, errors.New("invalid settings command reservation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("begin settings reservation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.SettingsCommandReserve{}, err
	}
	authority := store.CommandAuthority{ConnectionEpoch: request.Writer.ConnectionEpoch, CredentialGeneration: request.Writer.CredentialGeneration}
	if err := lockCommandAuthority(ctx, tx, sessionID, authority); err != nil {
		return store.SettingsCommandReserve{}, err
	}
	if err := validateCurrentSettingsWriter(ctx, tx, sessionID, request.Writer); err != nil {
		return store.SettingsCommandReserve{}, err
	}
	capability, err := querySettingsCapability(ctx, tx, sessionID)
	if err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("select settings capability: %w", err)
	}
	existing, err := querySettingsCommand(ctx, tx, sessionID, request.CommandID)
	if err == nil {
		if existing.RequestFingerprint != request.RequestFingerprint || !sameSettingsOptionalID(existing.RequestedModelID, request.RequestedModelID) || !sameSettingsOptionalID(existing.RequestedPermissionModeID, request.RequestedPermissionModeID) {
			return store.SettingsCommandReserve{}, errors.New("settings command ID is reused")
		}
		if err := tx.Commit(); err != nil {
			return store.SettingsCommandReserve{}, fmt.Errorf("commit duplicate settings reservation: %w", err)
		}
		return store.SettingsCommandReserve{Command: existing, Duplicate: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.SettingsCommandReserve{}, err
	}
	if capability.Fingerprint != request.RequestFingerprint || capability.Writer != request.Writer {
		return store.SettingsCommandReserve{}, errors.New("settings capability is stale or writer is fenced")
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_settings_commands
(session_id, cmd_id, request_fingerprint, requested_model_id, requested_permission_mode_id, reservation_version, delivery_deadline_ms, writer_connection_epoch, writer_credential_generation, writer_lease_id, reserved_capability_event_seq, reserved_fingerprint, reserved_effective_model_id, reserved_effective_permission_mode_id, status, created_at_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, 'delivery_pending', ?, ?)
`, sessionID, request.CommandID, request.RequestFingerprint, request.RequestedModelID, request.RequestedPermissionModeID, nowMS+5000, request.Writer.ConnectionEpoch, request.Writer.CredentialGeneration, request.Writer.LeaseID, capability.EventSeq, capability.Fingerprint, capability.EffectiveModelID, capability.EffectivePermissionModeID, nowMS, nowMS); err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("insert settings reservation: %w", err)
	}
	command, err := querySettingsCommand(ctx, tx, sessionID, request.CommandID)
	if err != nil {
		return store.SettingsCommandReserve{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.SettingsCommandReserve{}, fmt.Errorf("commit settings reservation: %w", err)
	}
	return store.SettingsCommandReserve{Command: command}, nil
}

func (s *Store) AcknowledgeSettingsCommandDelivery(ctx context.Context, sessionID, commandID string, reservationVersion int64, writer store.SettingsWriter) (store.SettingsCommand, error) {
	if !validConnectionID(sessionID) || commandID == "" || reservationVersion < 1 || !validSettingsWriter(writer) {
		return store.SettingsCommand{}, errors.New("invalid settings delivery acknowledgement")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.SettingsCommand{}, fmt.Errorf("begin settings delivery transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: writer.ConnectionEpoch, CredentialGeneration: writer.CredentialGeneration}); err != nil {
		return store.SettingsCommand{}, err
	}
	if err := validateCurrentSettingsWriter(ctx, tx, sessionID, writer); err != nil {
		return store.SettingsCommand{}, err
	}
	command, err := querySettingsCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if command.ReservationVersion != reservationVersion || command.Writer != writer || command.Status != store.SettingsCommandDeliveryPending || command.DeliveryDeadline.UnixMilli() <= nowMS {
		return store.SettingsCommand{}, errors.New("settings delivery acknowledgement is fenced")
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_settings_commands SET status='pending', operation_deadline_ms=?, updated_at_ms=? WHERE session_id=? AND cmd_id=? AND reservation_version=? AND status='delivery_pending' AND delivery_deadline_ms>? AND writer_connection_epoch=? AND writer_credential_generation=? AND writer_lease_id=?`, nowMS+30000, nowMS, sessionID, commandID, reservationVersion, nowMS, writer.ConnectionEpoch, writer.CredentialGeneration, writer.LeaseID)
	if err != nil {
		return store.SettingsCommand{}, fmt.Errorf("acknowledge settings delivery: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.SettingsCommand{}, errors.New("settings delivery acknowledgement lost race")
	}
	command, err = querySettingsCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("commit settings delivery acknowledgement: %w", err)
	}
	return command, nil
}

func (s *Store) RecoverSettingsCommand(ctx context.Context, sessionID, commandID string, priorWriter store.SettingsWriter) (store.SettingsCommand, error) {
	if !validConnectionID(sessionID) || commandID == "" || !validSettingsWriter(priorWriter) {
		return store.SettingsCommand{}, errors.New("invalid settings recovery")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.SettingsCommand{}, fmt.Errorf("begin settings recovery transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	command, err := querySettingsCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if command.Writer != priorWriter {
		return store.SettingsCommand{}, errors.New("settings recovery lost writer fence")
	}
	if command.Status == store.SettingsCommandDeliveryPending {
		if command.DeliveryDeadline.UnixMilli() > nowMS {
			return store.SettingsCommand{}, errors.New("settings delivery deadline has not elapsed")
		}
		reason := "adapter_delivery_failed"
		payload, err := store.SettingsTerminalEventPayload(command, command.ReservedCapability, store.SettingsCommandRejected, &reason)
		if err != nil {
			return store.SettingsCommand{}, err
		}
		var latest int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE session_id=?`, sessionID).Scan(&latest); err != nil {
			return store.SettingsCommand{}, err
		}
		seq := latest + 1
		if _, err := tx.ExecContext(ctx, `INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms) VALUES (?, ?, 'session.settings.effective', ?, ?, ?)`, sessionID, seq, payload, nowMS, nowMS); err != nil {
			return store.SettingsCommand{}, err
		}
		if result, err := tx.ExecContext(ctx, `UPDATE session_settings_commands SET status='rejected', terminal_event_seq=?, updated_at_ms=? WHERE session_id=? AND cmd_id=? AND status='delivery_pending' AND delivery_deadline_ms<=? AND terminal_event_seq IS NULL`, seq, nowMS, sessionID, commandID, nowMS); err != nil || result == nil {
			return store.SettingsCommand{}, errors.New("settings delivery deadline finalization lost race")
		} else if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return store.SettingsCommand{}, errors.New("settings delivery deadline finalization lost race")
		}
	} else {
		if command.Status != store.SettingsCommandPending {
			return store.SettingsCommand{}, errors.New("settings recovery requires a pending command")
		}
		capability, err := querySettingsCapability(ctx, tx, sessionID)
		if err != nil || capability.Writer == priorWriter || capability.EventSeq <= command.ReservedCapability.EventSeq {
			return store.SettingsCommand{}, errors.New("settings recovery requires a fresh replacement writer")
		}
		if err := validateLiveSettingsWriter(ctx, tx, sessionID, capability.Writer); err != nil {
			return store.SettingsCommand{}, errors.New("settings recovery replacement writer is not live")
		}
		result, err := tx.ExecContext(ctx, `UPDATE session_settings_commands SET status='recovery_pending', updated_at_ms=? WHERE session_id=? AND cmd_id=? AND status='pending' AND writer_connection_epoch=? AND writer_credential_generation=? AND writer_lease_id=?`, nowMS, sessionID, commandID, priorWriter.ConnectionEpoch, priorWriter.CredentialGeneration, priorWriter.LeaseID)
		if err != nil {
			return store.SettingsCommand{}, fmt.Errorf("fence settings writer for recovery: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return store.SettingsCommand{}, errors.New("settings recovery lost writer fence")
		}
	}
	command, err = querySettingsCommand(ctx, tx, sessionID, commandID)
	if err := tx.Commit(); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("commit settings recovery: %w", err)
	}
	return command, nil
}

func (s *Store) FinalizeSettingsCommand(ctx context.Context, sessionID, commandID string, finalize store.SettingsCommandFinalize) (store.SettingsCommand, error) {
	if !validConnectionID(sessionID) || commandID == "" || finalize.ReservationVersion < 1 || !validSettingsNonterminalStatus(finalize.ExpectedStatus) || !validSettingsTerminalStatus(finalize.Outcome) || !validSettingsCapabilityUpdate(sessionID, store.SettingsCapabilityUpdate{EventSeq: finalize.EffectiveCapability.EventSeq, Fingerprint: finalize.EffectiveCapability.Fingerprint, EffectiveModelID: finalize.EffectiveCapability.EffectiveModelID, EffectivePermissionModeID: finalize.EffectiveCapability.EffectivePermissionModeID, Writer: finalize.EffectiveCapability.Writer}) || (finalize.EffectiveCapability.SessionID != "" && finalize.EffectiveCapability.SessionID != sessionID) {
		return store.SettingsCommand{}, errors.New("invalid settings finalization")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.SettingsCommand{}, fmt.Errorf("begin settings finalization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	command, err := querySettingsCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if command.ReservationVersion != finalize.ReservationVersion || command.Status != finalize.ExpectedStatus || (finalize.Writer != nil && command.Writer != *finalize.Writer) {
		return store.SettingsCommand{}, errors.New("settings finalization is fenced")
	}
	if finalize.Writer != nil {
		if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: finalize.Writer.ConnectionEpoch, CredentialGeneration: finalize.Writer.CredentialGeneration}); err != nil {
			return store.SettingsCommand{}, err
		}
		if err := validateCurrentSettingsWriter(ctx, tx, sessionID, *finalize.Writer); err != nil {
			return store.SettingsCommand{}, err
		}
	}
	capability, err := querySettingsCapability(ctx, tx, sessionID)
	if err != nil || !sameSettingsCapability(capability, finalize.EffectiveCapability) {
		return store.SettingsCommand{}, errors.New("effective settings capability is not current")
	}
	if err := validateSettingsFinalization(command, capability, finalize, nowMS); err != nil {
		return store.SettingsCommand{}, err
	}
	payload, err := store.SettingsTerminalEventPayload(command, capability, finalize.Outcome, finalize.ReasonCode)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	var latest int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE session_id=?`, sessionID).Scan(&latest); err != nil {
		return store.SettingsCommand{}, err
	}
	seq := latest + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms) VALUES (?, ?, 'session.settings.effective', ?, ?, ?)`, sessionID, seq, payload, nowMS, nowMS); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("append settings terminal event: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_settings_commands SET status=?, terminal_event_seq=?, updated_at_ms=? WHERE session_id=? AND cmd_id=? AND reservation_version=? AND status=? AND terminal_event_seq IS NULL`, finalize.Outcome, seq, nowMS, sessionID, commandID, finalize.ReservationVersion, finalize.ExpectedStatus)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.SettingsCommand{}, errors.New("settings finalization lost race")
	}
	command, err = querySettingsCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.SettingsCommand{}, fmt.Errorf("commit settings finalization: %w", err)
	}
	return command, nil
}

func (s *Store) SettingsCommand(ctx context.Context, sessionID, commandID string) (store.SettingsCommand, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	defer func() { _ = tx.Rollback() }()
	command, err := querySettingsCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.SettingsCommand{}, err
	}
	return command, nil
}

func (s *Store) PendingSettingsCommands(ctx context.Context, sessionID string) ([]store.SettingsCommand, error) {
	if !validConnectionID(sessionID) {
		return nil, errors.New("invalid settings pending Session")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT cmd_id FROM session_settings_commands WHERE session_id=? AND status IN ('delivery_pending','pending','recovery_pending') ORDER BY reservation_version`, sessionID)
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
		command, err := querySettingsCommand(ctx, tx, sessionID, commandID)
		if err != nil {
			return nil, err
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return commands, nil
}

func (s *Store) PublishRunControlCapability(ctx context.Context, sessionID string, update store.RunControlCapabilityUpdate) (store.RunControlCapability, error) {
	if !validRunControlCapabilityUpdate(sessionID, update) {
		return store.RunControlCapability{}, errors.New("invalid run-control capability")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.RunControlCapability{}, err
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.RunControlCapability{}, err
	}
	if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: update.Writer.ConnectionEpoch, CredentialGeneration: update.Writer.CredentialGeneration}); err != nil {
		return store.RunControlCapability{}, err
	}
	if err := validateLiveSettingsWriter(ctx, tx, sessionID, update.Writer); err != nil {
		return store.RunControlCapability{}, err
	}
	if err := verifyRunControlCapabilityEvent(ctx, tx, sessionID, update.EventSeq); err != nil {
		return store.RunControlCapability{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO session_run_control_capabilities (session_id,capability_event_seq,capability_version,interrupt_supported,stop_supported,writer_connection_epoch,writer_credential_generation,writer_lease_id,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET capability_event_seq=excluded.capability_event_seq,capability_version=excluded.capability_version,interrupt_supported=excluded.interrupt_supported,stop_supported=excluded.stop_supported,writer_connection_epoch=excluded.writer_connection_epoch,writer_credential_generation=excluded.writer_credential_generation,writer_lease_id=excluded.writer_lease_id,updated_at_ms=excluded.updated_at_ms WHERE session_run_control_capabilities.capability_event_seq < excluded.capability_event_seq`, sessionID, update.EventSeq, update.EventSeq, update.InterruptSupported, update.StopSupported, update.Writer.ConnectionEpoch, update.Writer.CredentialGeneration, update.Writer.LeaseID, nowMS, nowMS)
	if err != nil {
		return store.RunControlCapability{}, fmt.Errorf("upsert run-control capability: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.RunControlCapability{}, errors.New("run-control capability event is stale")
	}
	capability, err := queryRunControlCapability(ctx, tx, sessionID)
	if err != nil {
		return store.RunControlCapability{}, err
	}
	return capability, tx.Commit()
}

func (s *Store) RunControlReserve(ctx context.Context, sessionID string, request store.RunControlRequest) (store.RunControlReserve, error) {
	if !validRunControlRequest(sessionID, request) {
		return store.RunControlReserve{}, errors.New("invalid run-control reservation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.RunControlReserve{}, err
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.RunControlReserve{}, err
	}
	if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: request.Writer.ConnectionEpoch, CredentialGeneration: request.Writer.CredentialGeneration}); err != nil {
		return store.RunControlReserve{}, err
	}
	if err := validateLiveSettingsWriter(ctx, tx, sessionID, request.Writer); err != nil {
		return store.RunControlReserve{}, err
	}
	existing, err := queryRunControlReservation(ctx, tx, sessionID, request.CommandID)
	if err == nil {
		if existing.Operation != request.Operation {
			return store.RunControlReserve{}, errors.New("run-control command ID is reused")
		}
		if err := tx.Commit(); err != nil {
			return store.RunControlReserve{}, err
		}
		return store.RunControlReserve{Reservation: existing, Duplicate: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.RunControlReserve{}, err
	}
	capability, err := queryRunControlCapability(ctx, tx, sessionID)
	if err != nil || capability.Writer != request.Writer {
		return store.RunControlReserve{}, errors.New("run-control capability is stale or writer is fenced")
	}
	if (request.Operation == store.RunControlInterrupt && !capability.InterruptSupported) || (request.Operation == store.RunControlStop && !capability.StopSupported) {
		return store.RunControlReserve{}, errors.New("run-control operation is unsupported")
	}
	if err := validateRunControlPreState(ctx, tx, sessionID, request); err != nil {
		return store.RunControlReserve{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_run_controls (session_id,cmd_id,operation,capability_version,reservation_version,pre_control_state,pre_control_state_seq,writer_connection_epoch,writer_credential_generation,writer_lease_id,deadline_ms,status,created_at_ms,updated_at_ms) VALUES (?,?,?, ?,1,?,?,?,?,?,?,'pending',?,?)`, sessionID, request.CommandID, request.Operation, capability.Version, request.PreControlState, request.PreControlStateSeq, request.Writer.ConnectionEpoch, request.Writer.CredentialGeneration, request.Writer.LeaseID, nowMS+30000, nowMS, nowMS); err != nil {
		return store.RunControlReserve{}, fmt.Errorf("insert run-control reservation: %w", err)
	}
	reservation, err := queryRunControlReservation(ctx, tx, sessionID, request.CommandID)
	if err != nil {
		return store.RunControlReserve{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.RunControlReserve{}, err
	}
	return store.RunControlReserve{Reservation: reservation}, nil
}

func (s *Store) RunControlFinalize(ctx context.Context, sessionID, commandID string, finalize store.RunControlFinalize) (store.RunControlReservation, error) {
	if !validConnectionID(sessionID) || commandID == "" || finalize.ReservationVersion < 1 || !validRunControlTerminalOutcome(finalize.Outcome) || finalize.Outcome == store.RunControlUnsupported || (finalize.Outcome == store.RunControlCompleted && finalize.ReasonCode != nil) || (finalize.Outcome != store.RunControlCompleted && !validSettingsReason(finalize.ReasonCode)) {
		return store.RunControlReservation{}, errors.New("invalid run-control finalization")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	reservation, err := queryRunControlReservation(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	if reservation.ReservationVersion != finalize.ReservationVersion || reservation.Outcome != store.RunControlPending {
		return store.RunControlReservation{}, errors.New("run-control finalization is fenced")
	}
	if finalize.Outcome == store.RunControlCompleted || finalize.Outcome == store.RunControlRejected {
		if finalize.Writer == nil || *finalize.Writer != reservation.Writer || reservation.Deadline.UnixMilli() <= nowMS {
			return store.RunControlReservation{}, errors.New("run-control writer finalization is fenced")
		}
		if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: finalize.Writer.ConnectionEpoch, CredentialGeneration: finalize.Writer.CredentialGeneration}); err != nil {
			return store.RunControlReservation{}, err
		}
		capability, err := queryRunControlCapability(ctx, tx, sessionID)
		if err != nil || capability.Version != reservation.CapabilityVersion || capability.Writer != *finalize.Writer {
			return store.RunControlReservation{}, errors.New("run-control capability changed")
		}
		if err := validateLiveSettingsWriter(ctx, tx, sessionID, *finalize.Writer); err != nil {
			return store.RunControlReservation{}, err
		}
	} else if finalize.Writer != nil || (finalize.Outcome == store.RunControlTimeout && reservation.Deadline.UnixMilli() > nowMS) {
		return store.RunControlReservation{}, errors.New("run-control unbound finalization is fenced")
	}
	if finalize.Outcome == store.RunControlCompleted {
		if err := validateRunControlPreState(ctx, tx, sessionID, store.RunControlRequest{Operation: reservation.Operation, PreControlState: reservation.PreControlState, PreControlStateSeq: reservation.PreControlStateSeq}); err != nil {
			return store.RunControlReservation{}, err
		}
		statePayload := `{"state":"ready"}`
		if reservation.Operation == store.RunControlStop {
			statePayload = `{"state":"ended","reason":"user_stop"}`
		}
		if _, err := appendRunControlEventTx(ctx, tx, sessionID, "session.state", statePayload, nowMS); err != nil {
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
	terminalSeq, err := appendRunControlEventTx(ctx, tx, sessionID, "session.run.outcome", string(payload), nowMS)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_run_controls SET status=?,terminal_event_seq=?,updated_at_ms=? WHERE session_id=? AND cmd_id=? AND reservation_version=? AND status='pending' AND terminal_event_seq IS NULL`, finalize.Outcome, terminalSeq, nowMS, sessionID, commandID, finalize.ReservationVersion)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.RunControlReservation{}, errors.New("run-control finalization lost race")
	}
	reservation, err = queryRunControlReservation(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	if err := tx.Commit(); err != nil {
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
	return queryRunControlReservation(ctx, s.db, sessionID, commandID)
}

func (s *Store) PendingRunControls(ctx context.Context, sessionID string) ([]store.RunControlReservation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT cmd_id FROM session_run_controls WHERE session_id=? AND status='pending' ORDER BY reservation_version`, sessionID)
	if err != nil {
		return nil, err
	}
	var commandIDs []string
	for rows.Next() {
		var commandID string
		if err := rows.Scan(&commandID); err != nil {
			_ = rows.Close()
			return nil, err
		}
		commandIDs = append(commandIDs, commandID)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reservations := make([]store.RunControlReservation, 0, len(commandIDs))
	for _, commandID := range commandIDs {
		item, err := queryRunControlReservation(ctx, s.db, sessionID, commandID)
		if err != nil {
			return nil, err
		}
		reservations = append(reservations, item)
	}
	return reservations, nil
}

func (s *Store) PublishFileReferenceCapability(ctx context.Context, sessionID string, update store.FileReferenceCapabilityUpdate) (store.FileReferenceCapability, error) {
	if !validFileReferenceCapabilityUpdate(sessionID, update) {
		return store.FileReferenceCapability{}, errors.New("invalid file-reference capability")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.FileReferenceCapability{}, err
	}
	defer tx.Rollback()
	now, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.FileReferenceCapability{}, err
	}
	if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: update.Writer.ConnectionEpoch, CredentialGeneration: update.Writer.CredentialGeneration}); err != nil {
		return store.FileReferenceCapability{}, err
	}
	if err := validateLiveSettingsWriter(ctx, tx, sessionID, update.Writer); err != nil {
		return store.FileReferenceCapability{}, err
	}
	var typ string
	if tx.QueryRowContext(ctx, `SELECT type FROM session_events WHERE session_id=? AND seq=?`, sessionID, update.EventSeq).Scan(&typ) != nil || typ != "session.file_references.capabilities" {
		return store.FileReferenceCapability{}, errors.New("file-reference capability event is invalid")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO session_file_reference_capabilities (session_id,capability_event_seq,capability_fingerprint,writer_connection_epoch,writer_credential_generation,writer_lease_id,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET capability_event_seq=excluded.capability_event_seq,capability_fingerprint=excluded.capability_fingerprint,writer_connection_epoch=excluded.writer_connection_epoch,writer_credential_generation=excluded.writer_credential_generation,writer_lease_id=excluded.writer_lease_id,updated_at_ms=excluded.updated_at_ms WHERE session_file_reference_capabilities.capability_event_seq < excluded.capability_event_seq`, sessionID, update.EventSeq, update.Fingerprint, update.Writer.ConnectionEpoch, update.Writer.CredentialGeneration, update.Writer.LeaseID, now, now)
	if err != nil {
		return store.FileReferenceCapability{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return store.FileReferenceCapability{}, errors.New("file-reference capability is stale")
	}
	capability, err := queryFileReferenceCapability(ctx, tx, sessionID)
	if err != nil {
		return store.FileReferenceCapability{}, err
	}
	return capability, tx.Commit()
}

func (s *Store) CommitFileReferenceCommand(ctx context.Context, sessionID string, message store.PendingEvent, request store.FileReferenceCommandRequest) (store.FileReferenceCommandReserve, error) {
	if !validFileReferenceRequest(sessionID, request) || message.Type != "session.message" || !json.Valid(message.Payload) {
		return store.FileReferenceCommandReserve{}, errors.New("invalid file-reference command")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	defer tx.Rollback()
	now, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	existing, err := queryFileReferenceCommand(ctx, tx, sessionID, request.CommandID)
	if err == nil {
		if existing.MessageID != request.MessageID || existing.CapabilityFingerprint != request.CapabilityFingerprint || existing.RequestFingerprint != request.RequestFingerprint || existing.ReferenceCount != request.ReferenceCount {
			return store.FileReferenceCommandReserve{}, errors.New("file-reference command ID is reused")
		}
		if err := tx.Commit(); err != nil {
			return store.FileReferenceCommandReserve{}, err
		}
		return store.FileReferenceCommandReserve{Command: existing, Duplicate: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.FileReferenceCommandReserve{}, err
	}
	capability, err := queryFileReferenceCapability(ctx, tx, sessionID)
	if err != nil || capability.Fingerprint != request.CapabilityFingerprint || validateLiveSettingsWriter(ctx, tx, sessionID, capability.Writer) != nil {
		return store.FileReferenceCommandReserve{}, errors.New("file-reference capability is stale")
	}
	if _, err := appendRunControlEventTx(ctx, tx, sessionID, message.Type, string(message.Payload), now); err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_file_reference_commands (session_id,cmd_id,message_id,capability_fingerprint,request_fingerprint,reference_count,reservation_version,delivery_deadline_ms,status,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,1,?,'delivery_pending',?,?)`, sessionID, request.CommandID, request.MessageID, request.CapabilityFingerprint, request.RequestFingerprint, request.ReferenceCount, now+600000, now, now); err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	command, err := queryFileReferenceCommand(ctx, tx, sessionID, request.CommandID)
	if err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.FileReferenceCommandReserve{}, err
	}
	return store.FileReferenceCommandReserve{Command: command}, nil
}

func (s *Store) AcknowledgeFileReferenceDelivery(ctx context.Context, sessionID, commandID string, version int64, writer store.FileReferenceWriter) (store.FileReferenceCommand, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	defer tx.Rollback()
	if !validConnectionID(sessionID) || !validSettingsWriter(writer) || version < 1 {
		return store.FileReferenceCommand{}, errors.New("invalid file-reference delivery")
	}
	now, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: writer.ConnectionEpoch, CredentialGeneration: writer.CredentialGeneration}); err != nil {
		return store.FileReferenceCommand{}, err
	}
	if err := validateLiveSettingsWriter(ctx, tx, sessionID, writer); err != nil {
		return store.FileReferenceCommand{}, err
	}
	capability, err := queryFileReferenceCapability(ctx, tx, sessionID)
	if err != nil || capability.Writer != writer {
		return store.FileReferenceCommand{}, errors.New("file-reference writer is fenced")
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_file_reference_commands SET writer_connection_epoch=?,writer_credential_generation=?,writer_lease_id=?,status='pending',updated_at_ms=? WHERE session_id=? AND cmd_id=? AND reservation_version=? AND status='delivery_pending' AND delivery_deadline_ms>?`, writer.ConnectionEpoch, writer.CredentialGeneration, writer.LeaseID, now, sessionID, commandID, version, now)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return store.FileReferenceCommand{}, errors.New("file-reference delivery is fenced")
	}
	command, err := queryFileReferenceCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	return command, tx.Commit()
}

func (s *Store) FinalizeFileReferenceCommand(ctx context.Context, sessionID, commandID string, finalize store.FileReferenceCommandFinalize) (store.FileReferenceCommand, error) {
	if !validConnectionID(sessionID) || commandID == "" || finalize.ReservationVersion < 1 || (finalize.Outcome != store.FileReferenceDelivered && finalize.Outcome != store.FileReferenceRejected && finalize.Outcome != store.FileReferenceOutcomeUnknown) {
		return store.FileReferenceCommand{}, errors.New("invalid file-reference finalization")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	defer tx.Rollback()
	now, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	command, err := queryFileReferenceCommand(ctx, tx, sessionID, commandID)
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
		if err := lockCommandAuthority(ctx, tx, sessionID, store.CommandAuthority{ConnectionEpoch: finalize.Writer.ConnectionEpoch, CredentialGeneration: finalize.Writer.CredentialGeneration}); err != nil {
			return store.FileReferenceCommand{}, err
		}
		if err := validateLiveSettingsWriter(ctx, tx, sessionID, *finalize.Writer); err != nil {
			return store.FileReferenceCommand{}, err
		}
		capability, err := queryFileReferenceCapability(ctx, tx, sessionID)
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
	seq, err := appendRunControlEventTx(ctx, tx, sessionID, "session.file_references.outcome", string(payload), now)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_file_reference_commands SET status=?,terminal_event_seq=?,updated_at_ms=? WHERE session_id=? AND cmd_id=? AND reservation_version=? AND status='pending' AND terminal_event_seq IS NULL`, finalize.Outcome, seq, now, sessionID, commandID, finalize.ReservationVersion)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return store.FileReferenceCommand{}, errors.New("file-reference terminal update is fenced")
	}
	command, err = queryFileReferenceCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	return command, tx.Commit()
}

func (s *Store) RecoverFileReferenceCommand(ctx context.Context, sessionID, commandID, reason string) (store.FileReferenceCommand, error) {
	command, err := s.FileReferenceCommand(ctx, sessionID, commandID)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if command.Status != store.FileReferenceDeliveryPending {
		return s.FinalizeFileReferenceCommand(ctx, sessionID, commandID, store.FileReferenceCommandFinalize{ReservationVersion: command.ReservationVersion, Outcome: store.FileReferenceOutcomeUnknown, ReasonCode: &reason})
	}
	if reason != "adapter_deadline" {
		return store.FileReferenceCommand{}, errors.New("file-reference delivery recovery requires adapter deadline")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	defer tx.Rollback()
	now, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	command, err = queryFileReferenceCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if command.Status != store.FileReferenceDeliveryPending || command.DeliveryDeadline.After(time.UnixMilli(now)) {
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
	seq, err := appendRunControlEventTx(ctx, tx, sessionID, "session.file_references.outcome", string(payload), now)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_file_reference_commands SET status='outcome_unknown',terminal_event_seq=?,updated_at_ms=? WHERE session_id=? AND cmd_id=? AND reservation_version=? AND status='delivery_pending' AND delivery_deadline_ms<=? AND terminal_event_seq IS NULL`, seq, now, sessionID, commandID, command.ReservationVersion, now)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return store.FileReferenceCommand{}, errors.New("file-reference delivery recovery is fenced")
	}
	command, err = queryFileReferenceCommand(ctx, tx, sessionID, commandID)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	return command, tx.Commit()
}
func (s *Store) FileReferenceCommand(ctx context.Context, sessionID, commandID string) (store.FileReferenceCommand, error) {
	return queryFileReferenceCommand(ctx, s.db, sessionID, commandID)
}
func (s *Store) PendingFileReferenceCommands(ctx context.Context, sessionID string) ([]store.FileReferenceCommand, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT cmd_id FROM session_file_reference_commands WHERE session_id=? AND status IN ('delivery_pending','pending') ORDER BY reservation_version`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var commands []store.FileReferenceCommand
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		command, err := s.FileReferenceCommand(ctx, sessionID, id)
		if err != nil {
			return nil, err
		}
		commands = append(commands, command)
	}
	return commands, rows.Err()
}

func (s *Store) ListPendingCommands(ctx context.Context, sessionID string, authority store.CommandAuthority) ([]store.PendingCommand, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin pending command listing: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := validateCommandAuthority(ctx, tx, sessionID, authority, nowMS); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT command.session_id, command.cmd_id, command.type, command.event_seq, command.status,
       command.expires_at_ns, command.created_at_ms, event.type, event.payload
FROM session_pending_commands AS command
JOIN session_events AS event
  ON event.session_id = command.session_id AND event.seq = command.event_seq
WHERE command.session_id = ?
  AND command.status IN ('pending', 'received')
  AND command.expires_at_ns > ?
  AND event.type = 'session.message'
  AND length(event.payload) BETWEEN 1 AND ?
ORDER BY command.event_seq ASC
`, sessionID, nowMS*int64(time.Millisecond), maxEventPayloadSize)
	if err != nil {
		return nil, fmt.Errorf("list pending commands: %w", err)
	}
	defer rows.Close()
	commands := make([]store.PendingCommand, 0)
	for rows.Next() {
		var command store.PendingCommand
		var status, eventType string
		var expiresAtNS, createdAtMS int64
		var eventPayload []byte
		if err := rows.Scan(&command.SessionID, &command.CommandID, &command.Type, &command.EventSeq, &status, &expiresAtNS, &createdAtMS, &eventType, &eventPayload); err != nil {
			return nil, fmt.Errorf("scan pending command: %w", err)
		}
		command.Status = store.PendingCommandStatus(status)
		command.ExpiresAt = time.Unix(0, expiresAtNS)
		var payload struct {
			Role string `json:"role"`
		}
		if command.SessionID != sessionID || command.CommandID == "" || len(command.CommandID) > 256 || command.Type != "session.send" ||
			command.EventSeq < 1 || (command.Status != store.PendingCommandPending && command.Status != store.PendingCommandReceived) ||
			createdAtMS < 1 || createdAtMS > nowMS || expiresAtNS <= createdAtMS*int64(time.Millisecond) ||
			expiresAtNS > (createdAtMS+30000)*int64(time.Millisecond) || expiresAtNS > (nowMS+30000)*int64(time.Millisecond) ||
			eventType != "session.message" || json.Unmarshal(eventPayload, &payload) != nil || payload.Role != "user" {
			return nil, errors.New("pending command row is invalid")
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending commands: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit pending command listing: %w", err)
	}
	return commands, nil
}

func (s *Store) ClaimPendingCommand(ctx context.Context, sessionID string, authority store.CommandAuthority, commandID string) (store.PendingCommandClaim, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.PendingCommandClaim{}, fmt.Errorf("begin pending command claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.PendingCommandClaim{}, err
	}
	if err := validateCommandAuthority(ctx, tx, sessionID, authority, nowMS); err != nil {
		return store.PendingCommandClaim{}, err
	}
	command, err := queryPendingCommand(ctx, tx, sessionID, commandID, nowMS)
	if err != nil {
		return store.PendingCommandClaim{}, fmt.Errorf("select claimable pending command: %w", err)
	}
	if !validPendingCommandStatus(command.Status) {
		return store.PendingCommandClaim{}, errors.New("pending command status is invalid")
	}
	if command.ExpiresAt.UnixNano() <= nowMS*int64(time.Millisecond) {
		return store.PendingCommandClaim{}, errors.New("pending command expired")
	}
	claimed := false
	if command.Status == store.PendingCommandPending {
		nowMS, err = sqliteNowMillis(ctx, tx)
		if err != nil {
			return store.PendingCommandClaim{}, err
		}
		if err := validateCommandAuthority(ctx, tx, sessionID, authority, nowMS); err != nil {
			return store.PendingCommandClaim{}, err
		}
		result, err := tx.ExecContext(ctx, `
UPDATE session_pending_commands SET status = 'received', updated_at_ms = ?
WHERE session_id = ? AND cmd_id = ? AND status = 'pending' AND expires_at_ns > ?
`, nowMS, sessionID, commandID, nowMS*int64(time.Millisecond))
		if err != nil {
			return store.PendingCommandClaim{}, fmt.Errorf("claim pending command: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return store.PendingCommandClaim{}, errors.New("pending command claim lost authority")
		}
		command.Status = store.PendingCommandReceived
		claimed = true
	}
	if err := tx.Commit(); err != nil {
		return store.PendingCommandClaim{}, fmt.Errorf("commit pending command claim: %w", err)
	}
	return store.PendingCommandClaim{Command: command, Claimed: claimed}, nil
}

func (s *Store) ResolvePendingCommand(ctx context.Context, sessionID string, authority store.CommandAuthority, commandID string, status store.PendingCommandStatus) (store.PendingCommand, error) {
	if status != store.PendingCommandCompleted && status != store.PendingCommandOutcomeUnknown {
		return store.PendingCommand{}, errors.New("invalid pending command outcome")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("begin pending command resolution: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.PendingCommand{}, err
	}
	if err := validateCommandAuthority(ctx, tx, sessionID, authority, nowMS); err != nil {
		return store.PendingCommand{}, err
	}
	command, err := queryPendingCommand(ctx, tx, sessionID, commandID, nowMS)
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("select resolvable pending command: %w", err)
	}
	if !validPendingCommandStatus(command.Status) || command.Status != store.PendingCommandReceived {
		return store.PendingCommand{}, errors.New("pending command is not received")
	}
	nowMS, err = sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.PendingCommand{}, err
	}
	if err := validateCommandAuthority(ctx, tx, sessionID, authority, nowMS); err != nil {
		return store.PendingCommand{}, err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE session_pending_commands SET status = ?, updated_at_ms = ?
WHERE session_id = ? AND cmd_id = ? AND status = 'received'
`, status, nowMS, sessionID, commandID)
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("resolve pending command: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.PendingCommand{}, errors.New("pending command resolution lost authority")
	}
	if status == store.PendingCommandOutcomeUnknown {
		operation := "command"
		if err := upsertSQLiteAttentionLedger(ctx, tx, sessionID, &store.AttentionBlocker{Kind: store.AttentionBlockerOutcomeUnknown, Operation: &operation}, nil); err != nil {
			return store.PendingCommand{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return store.PendingCommand{}, fmt.Errorf("commit pending command resolution: %w", err)
	}
	command.Status = status
	return command, nil
}

func (s *Store) ResolvePendingCommandUnknown(ctx context.Context, sessionID string, commandID string) (store.PendingCommand, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("begin unknown pending command resolution: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.PendingCommand{}, err
	}
	var command store.PendingCommand
	var status string
	var expiresAtNS, createdAtMS int64
	if err := tx.QueryRowContext(ctx, `
SELECT session_id, cmd_id, type, event_seq, status, expires_at_ns, created_at_ms
FROM session_pending_commands
WHERE session_id = ? AND cmd_id = ? AND status = 'received'
`, sessionID, commandID).Scan(&command.SessionID, &command.CommandID, &command.Type, &command.EventSeq, &status, &expiresAtNS, &createdAtMS); err != nil {
		return store.PendingCommand{}, fmt.Errorf("select received pending command: %w", err)
	}
	if command.SessionID != sessionID || command.Type != "session.send" || command.EventSeq < 1 || status != string(store.PendingCommandReceived) || createdAtMS < 1 || createdAtMS > nowMS {
		return store.PendingCommand{}, errors.New("received pending command row is invalid")
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_pending_commands SET status = 'outcome_unknown', updated_at_ms = ? WHERE session_id = ? AND cmd_id = ? AND status = 'received'`, nowMS, sessionID, commandID)
	if err != nil {
		return store.PendingCommand{}, fmt.Errorf("resolve pending command outcome unknown: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.PendingCommand{}, errors.New("pending command outcome unknown lost race")
	}
	operation := "command"
	if err := upsertSQLiteAttentionLedger(ctx, tx, sessionID, &store.AttentionBlocker{Kind: store.AttentionBlockerOutcomeUnknown, Operation: &operation}, nil); err != nil {
		return store.PendingCommand{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.PendingCommand{}, fmt.Errorf("commit pending command outcome unknown: %w", err)
	}
	command.Status = store.PendingCommandOutcomeUnknown
	command.ExpiresAt = time.Unix(0, expiresAtNS)
	return command, nil
}

func (s *Store) CommitProposedEvent(ctx context.Context, sessionID string, authority store.CommandAuthority, proposal store.ProposedEventRequest) (store.ProposedEventReceipt, error) {
	event := proposal.Event
	event.Payload = append([]byte(nil), proposal.Event.Payload...)
	if err := validateProposedEventInput(sessionID, authority, proposal.ProposalID, event); err != nil {
		return store.ProposedEventReceipt{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("begin proposed event transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.ProposedEventReceipt{}, err
	}
	if err := lockCommandAuthority(ctx, tx, sessionID, authority); err != nil {
		return store.ProposedEventReceipt{}, err
	}
	nowMS, err = sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.ProposedEventReceipt{}, err
	}

	existing, existingType, existingTime, existingPayload, err := queryProposedEvent(ctx, tx, sessionID, proposal.ProposalID, nowMS)
	if err == nil {
		if existingType != event.Type || existingTime != event.Time.UnixMilli() || !bytes.Equal(existingPayload, event.Payload) {
			return store.ProposedEventReceipt{}, errors.New("conflicting proposed event retry")
		}
		if err := tx.Commit(); err != nil {
			return store.ProposedEventReceipt{}, fmt.Errorf("commit proposed event duplicate: %w", err)
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.ProposedEventReceipt{}, fmt.Errorf("select proposed event: %w", err)
	}

	var latest int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE session_id = ?`, sessionID).Scan(&latest); err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("select proposed event sequence: %w", err)
	}
	seq := latest + 1
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?)
`, sessionID, seq, event.Type, event.Payload, event.Time.UnixMilli(), nowMS); err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("append proposed event: %w", err)
	}
	terminal, err := projectSQLiteAttentionEvent(ctx, tx, sessionID, seq, event, nowMS)
	if err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("project proposed attention event: %w", err)
	}
	nowMS, err = sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.ProposedEventReceipt{}, err
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO session_event_proposals (session_id, proposal_id, event_seq, created_at_ms)
SELECT ?, ?, ?, ? FROM session_adapter_connections
WHERE session_id = ? AND connection_epoch = ? AND active_credential_generation = ?
  AND active_credential_expires_at_ms > ? AND revoked_at_ms IS NULL AND terminal_at_ms IS NULL
`, sessionID, proposal.ProposalID, seq, nowMS, sessionID, authority.ConnectionEpoch, authority.CredentialGeneration, nowMS)
	if err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("insert proposed event receipt: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.ProposedEventReceipt{}, errors.New("proposed event lost authority")
	}
	if terminal {
		if _, err := tx.ExecContext(ctx, `UPDATE session_adapter_connections SET revoked_at_ms = ?, terminal_at_ms = ?, updated_at_ms = ? WHERE session_id = ? AND terminal_at_ms IS NULL`, nowMS, nowMS, nowMS, sessionID); err != nil {
			return store.ProposedEventReceipt{}, fmt.Errorf("fence terminal proposed event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return store.ProposedEventReceipt{}, fmt.Errorf("commit proposed event: %w", err)
	}
	return proposedEventReceipt(sessionID, proposal.ProposalID, seq), nil
}

func queryProposedEvent(ctx context.Context, tx *sql.Tx, sessionID, proposalID string, nowMS int64) (store.ProposedEventReceipt, string, int64, []byte, error) {
	var seq, createdAtMS int64
	var eventTimeMS sql.NullInt64
	var eventSessionID, eventType sql.NullString
	var eventSeq, payloadLength sql.NullInt64
	var payload []byte
	err := tx.QueryRowContext(ctx, `
SELECT proposal.event_seq, proposal.created_at_ms, event.session_id, event.seq, event.type, event.event_time_ms,
       CASE WHEN length(event.payload) BETWEEN 1 AND ? THEN event.payload END,
       length(event.payload)
FROM session_event_proposals AS proposal
LEFT JOIN session_events AS event
  ON event.session_id = proposal.session_id AND event.seq = proposal.event_seq
WHERE proposal.session_id = ? AND proposal.proposal_id = ?
`, maxEventPayloadSize, sessionID, proposalID).Scan(
		&seq, &createdAtMS, &eventSessionID, &eventSeq, &eventType, &eventTimeMS, &payload, &payloadLength,
	)
	if err != nil {
		return store.ProposedEventReceipt{}, "", 0, nil, err
	}
	if seq < 1 || createdAtMS < 1 || createdAtMS > nowMS || !eventSessionID.Valid || eventSessionID.String != sessionID ||
		!eventSeq.Valid || eventSeq.Int64 != seq || !eventType.Valid || eventType.String == "" || !eventTimeMS.Valid ||
		!payloadLength.Valid || payloadLength.Int64 < 1 || payloadLength.Int64 > maxEventPayloadSize ||
		len(payload) != int(payloadLength.Int64) || !json.Valid(payload) {
		return store.ProposedEventReceipt{}, "", 0, nil, errors.New("proposed event row is invalid")
	}
	return proposedEventReceipt(sessionID, proposalID, seq), eventType.String, eventTimeMS.Int64, append([]byte(nil), payload...), nil
}

func validateProposedEventInput(sessionID string, authority store.CommandAuthority, proposalID string, event store.PendingEvent) error {
	if sessionID == "" || len(sessionID) > 255 || proposalID == "" || len(proposalID) > 255 ||
		authority.ConnectionEpoch < 1 || authority.CredentialGeneration < 1 || event.Type == "" ||
		len(event.Payload) < 1 || len(event.Payload) > maxEventPayloadSize || !json.Valid(event.Payload) {
		return errors.New("invalid proposed event")
	}
	return nil
}

func proposedEventReceipt(sessionID, proposalID string, seq int64) store.ProposedEventReceipt {
	return store.ProposedEventReceipt{SessionID: sessionID, ProposalID: proposalID, Seq: seq, Status: store.ProposedEventAccepted}
}

func (s *Store) CommitAttachAttempt(ctx context.Context, request store.AttachAttemptRequest) (store.AttachAttemptCommit, error) {
	if s == nil || s.db == nil {
		return store.AttachAttemptCommit{}, errors.New("sqlite event store is nil")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("begin attach attempt transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("read attach attempt Store clock: %w", err)
	}
	now := time.UnixMilli(nowMS)
	if err := validateSQLiteAttachAttempt(request, now); err != nil {
		return store.AttachAttemptCommit{}, err
	}
	if err := cleanupExpiredSQLiteAttachAttempts(ctx, tx); err != nil {
		return store.AttachAttemptCommit{}, err
	}
	nowMS, err = sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("refresh attach attempt Store clock: %w", err)
	}
	if err := validateSQLiteAttachAttempt(request, time.UnixMilli(nowMS)); err != nil {
		return store.AttachAttemptCommit{}, err
	}
	result, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO session_attach_attempts
(attempt_jti_hash, attach_id, bootstrap_session_id, target_session_id, provider,
 fingerprint_domain, fingerprint_version, fingerprint_digest, fingerprint_key_version,
 expires_at_ns, admission_outcome, issued_credential_generation, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, request.Identity.JTIHash[:], request.Identity.AttachID, request.Identity.BootstrapSessionID,
		request.Identity.TargetSessionID, request.Identity.Provider, request.Fingerprint.Domain,
		request.Fingerprint.Version, request.Fingerprint.Digest[:], request.Fingerprint.KeyVersion,
		request.ExpiresAt.UnixNano(), string(request.Outcome), nullableAttachGeneration(request.IssuedCredentialGeneration), nowMS)
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("insert attach attempt: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("read attach attempt insert result: %w", err)
	}
	current, err := querySQLiteAttachAttempt(ctx, tx, request.Identity.JTIHash)
	if err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("read attach attempt: %w", err)
	}
	if changed == 1 {
		if err := tx.Commit(); err != nil {
			return store.AttachAttemptCommit{}, fmt.Errorf("commit attach attempt: %w", err)
		}
		return store.AttachAttemptCommit{Attempt: current}, nil
	}
	if !sameSQLiteAttachAttempt(current, request) {
		return store.AttachAttemptCommit{}, errors.New("attach attempt is immutable")
	}
	if err := tx.Commit(); err != nil {
		return store.AttachAttemptCommit{}, fmt.Errorf("commit duplicate attach attempt: %w", err)
	}
	return store.AttachAttemptCommit{Attempt: current, Duplicate: true}, nil
}

func (s *Store) AttachAttempt(ctx context.Context, jtiHash [32]byte) (store.AttachAttempt, error) {
	if s == nil || s.db == nil {
		return store.AttachAttempt{}, errors.New("sqlite event store is nil")
	}
	if jtiHash == ([32]byte{}) {
		return store.AttachAttempt{}, errors.New("attach attempt JTI hash is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.AttachAttempt{}, fmt.Errorf("begin attach attempt read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := cleanupExpiredSQLiteAttachAttempts(ctx, tx); err != nil {
		return store.AttachAttempt{}, err
	}
	attempt, err := querySQLiteAttachAttempt(ctx, tx, jtiHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if commitErr := tx.Commit(); commitErr != nil {
				return store.AttachAttempt{}, fmt.Errorf("commit expired attach attempt cleanup: %w", commitErr)
			}
		}
		return store.AttachAttempt{}, err
	}
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.AttachAttempt{}, fmt.Errorf("refresh attach attempt Store clock: %w", err)
	}
	if !attempt.ExpiresAt.After(time.UnixMilli(nowMS)) {
		if commitErr := tx.Commit(); commitErr != nil {
			return store.AttachAttempt{}, fmt.Errorf("commit expired attach attempt cleanup: %w", commitErr)
		}
		return store.AttachAttempt{}, sql.ErrNoRows
	}
	if err := tx.Commit(); err != nil {
		return store.AttachAttempt{}, fmt.Errorf("commit attach attempt read transaction: %w", err)
	}
	return attempt, nil
}

func cleanupExpiredSQLiteAttachAttempts(ctx context.Context, executor sqliteConnectionExecutor) error {
	_, err := executor.ExecContext(ctx, `
DELETE FROM session_attach_attempts
WHERE attempt_jti_hash IN (
    SELECT attempt_jti_hash FROM session_attach_attempts
    WHERE expires_at_ns <= CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) * 1000000
    ORDER BY expires_at_ns, attempt_jti_hash
    LIMIT ?
)
`, maxAttachAttemptCleanupRows)
	if err != nil {
		return fmt.Errorf("cleanup expired attach attempts: %w", err)
	}
	return nil
}

func querySQLiteAttachAttempt(ctx context.Context, executor sqliteConnectionExecutor, jtiHash [32]byte) (store.AttachAttempt, error) {
	var rawJTI, rawDigest []byte
	var identity store.AttachAttemptIdentity
	var fingerprint store.AttachAttemptFingerprint
	var expiresNS, createdMS int64
	var outcome string
	var issued sql.NullInt64
	err := executor.QueryRowContext(ctx, `
SELECT attempt_jti_hash, attach_id, bootstrap_session_id, target_session_id, provider,
 fingerprint_domain, fingerprint_version, fingerprint_digest, fingerprint_key_version,
 expires_at_ns, admission_outcome, issued_credential_generation, created_at_ms
FROM session_attach_attempts WHERE attempt_jti_hash = ?
`, jtiHash[:]).Scan(&rawJTI, &identity.AttachID, &identity.BootstrapSessionID, &identity.TargetSessionID,
		&identity.Provider, &fingerprint.Domain, &fingerprint.Version, &rawDigest, &fingerprint.KeyVersion,
		&expiresNS, &outcome, &issued, &createdMS)
	if err != nil {
		return store.AttachAttempt{}, err
	}
	if len(rawJTI) != len(jtiHash) || !bytes.Equal(rawJTI, jtiHash[:]) || len(rawDigest) != len(fingerprint.Digest) ||
		createdMS < 1 || expiresNS <= createdMS*int64(time.Millisecond) || identity.JTIHash != ([32]byte{}) {
		return store.AttachAttempt{}, errors.New("attach attempt row is invalid")
	}
	copy(identity.JTIHash[:], rawJTI)
	copy(fingerprint.Digest[:], rawDigest)
	attempt := store.AttachAttempt{Identity: identity, Fingerprint: fingerprint, ExpiresAt: time.Unix(0, expiresNS), Outcome: store.AttachAttemptOutcome(outcome), IssuedCredentialGeneration: nullableAttachPointer(issued)}
	if !validStoredSQLiteAttachAttempt(attempt) {
		return store.AttachAttempt{}, errors.New("attach attempt row is invalid")
	}
	return attempt, nil
}

func validateSQLiteAttachAttempt(request store.AttachAttemptRequest, now time.Time) error {
	identity, fingerprint := request.Identity, request.Fingerprint
	if identity.JTIHash == ([32]byte{}) || !validConnectionID(identity.AttachID) || !validConnectionID(identity.BootstrapSessionID) ||
		!validConnectionID(identity.TargetSessionID) || identity.BootstrapSessionID == identity.TargetSessionID || len(identity.Provider) == 0 || len(identity.Provider) > 128 ||
		fingerprint.Domain != "agentwharf.attach-request.v1" || fingerprint.Version != 1 || fingerprint.Digest == ([32]byte{}) ||
		fingerprint.KeyVersion < 1 || fingerprint.KeyVersion > int64(^uint32(0)>>1) || request.ExpiresAt.IsZero() || !request.ExpiresAt.After(now) || request.ExpiresAt.After(now.Add(maxAttachAttemptTTL)) {
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

func validStoredSQLiteAttachAttempt(attempt store.AttachAttempt) bool {
	if attempt.Identity.JTIHash == ([32]byte{}) || !validConnectionID(attempt.Identity.AttachID) || !validConnectionID(attempt.Identity.BootstrapSessionID) ||
		!validConnectionID(attempt.Identity.TargetSessionID) || attempt.Identity.BootstrapSessionID == attempt.Identity.TargetSessionID || len(attempt.Identity.Provider) == 0 || len(attempt.Identity.Provider) > 128 ||
		attempt.Fingerprint.Domain != "agentwharf.attach-request.v1" || attempt.Fingerprint.Version != 1 || attempt.Fingerprint.Digest == ([32]byte{}) || attempt.Fingerprint.KeyVersion < 1 || attempt.Fingerprint.KeyVersion > int64(^uint32(0)>>1) ||
		attempt.ExpiresAt.IsZero() || attempt.ExpiresAt.UnixNano() < 1 {
		return false
	}
	return (attempt.Outcome == store.AttachAttemptAccepted && attempt.IssuedCredentialGeneration != nil && *attempt.IssuedCredentialGeneration > 0) ||
		(attempt.Outcome == store.AttachAttemptRejected && attempt.IssuedCredentialGeneration == nil)
}

func nullableAttachGeneration(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableAttachPointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

func sameSQLiteAttachAttempt(current store.AttachAttempt, request store.AttachAttemptRequest) bool {
	return current.Identity == request.Identity && current.Fingerprint == request.Fingerprint && current.ExpiresAt.Equal(request.ExpiresAt) &&
		current.Outcome == request.Outcome && ((current.IssuedCredentialGeneration == nil && request.IssuedCredentialGeneration == nil) ||
		(current.IssuedCredentialGeneration != nil && request.IssuedCredentialGeneration != nil && *current.IssuedCredentialGeneration == *request.IssuedCredentialGeneration))
}

func (s *Store) CommitWarmAttach(ctx context.Context, request store.WarmAttachRequest) (store.WarmAttachCommit, error) {
	if s == nil || s.db == nil {
		return store.WarmAttachCommit{}, errors.New("sqlite event store is nil")
	}
	if err := validateSQLiteWarmAttach(request); err != nil {
		return store.WarmAttachCommit{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("begin sqlite warm attach: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	if !validSQLiteWarmAttachExpiry(request, time.UnixMilli(nowMS)) {
		return store.WarmAttachCommit{}, errors.New("warm attach expiry is outside the Store-clock window")
	}
	if err := validateSQLiteAttachAttempt(request.Attempt, time.UnixMilli(nowMS)); err != nil {
		return store.WarmAttachCommit{}, err
	}
	if err := cleanupExpiredSQLiteAttachAttempts(ctx, tx); err != nil {
		return store.WarmAttachCommit{}, err
	}
	if existing, lookupErr := querySQLiteAttachAttempt(ctx, tx, request.Attempt.Identity.JTIHash); lookupErr == nil {
		nowMS, err = sqliteNowMillis(ctx, tx)
		if err != nil {
			return store.WarmAttachCommit{}, err
		}
		commit, duplicateErr := sqliteWarmAttachDuplicate(ctx, tx, existing, request, nowMS)
		if duplicateErr != nil {
			return store.WarmAttachCommit{}, duplicateErr
		}
		if err := tx.Commit(); err != nil {
			return store.WarmAttachCommit{}, fmt.Errorf("commit sqlite warm attach duplicate: %w", err)
		}
		return commit, nil
	} else if !errors.Is(lookupErr, sql.ErrNoRows) {
		return store.WarmAttachCommit{}, fmt.Errorf("load sqlite warm attach: %w", lookupErr)
	}
	bound := &Store{db: s.db, fenceDB: s.fenceDB, connectionTx: tx}
	if _, err := bound.ValidateAdapterAdmission(ctx, request.Attempt.Identity.BootstrapSessionID, request.BootstrapAdmission); err != nil {
		return store.WarmAttachCommit{}, errors.New("warm attach bootstrap admission lost")
	}
	if _, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE target_session_id = ?`, request.Attachment.Identity.TargetSessionID)); err == nil {
		return store.WarmAttachCommit{}, errors.New("warm attach target already has an attachment")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return store.WarmAttachCommit{}, fmt.Errorf("load sqlite warm attach target: %w", err)
	}
	summary, err := querySQLiteAttentionSummary(ctx, tx.QueryRowContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id = ?`, request.Attachment.Identity.TargetSessionID))
	if errors.Is(err, sql.ErrNoRows) {
		return store.WarmAttachCommit{}, errors.New("warm attach target is missing")
	}
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	if summary.StateOfProjection != store.AttentionProjectionComplete || summary.TerminalOutcome != nil || summary.State == "ended" || summary.State == "error" {
		return store.WarmAttachCommit{}, errors.New("warm attach target is not attachable")
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_adapter_connections
(session_id, connection_epoch, accepted_fence, active_credential_generation, credential_generation_high_watermark, active_credential_expires_at_ms, created_at_ms, updated_at_ms)
VALUES (?, 0, 0, ?, ?, ?, ?, ?)`, request.Attachment.Identity.TargetSessionID, request.TargetActivation.Generation,
		request.TargetActivation.Generation, request.TargetActivation.ExpiresAt.UnixMilli(), nowMS, nowMS); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("initialize sqlite warm attach target credential: %w", err)
	}
	connection, err := queryAdapterConnection(ctx, tx, request.Attachment.Identity.TargetSessionID)
	if err != nil || connection.ConnectionEpoch != 0 || connection.AcceptedFence != 0 ||
		connection.ActiveCredentialGeneration != request.TargetActivation.Generation || connection.CredentialGenerationHighWatermark != request.TargetActivation.Generation ||
		!connection.ActiveCredentialExpiresAt.Equal(time.UnixMilli(request.TargetActivation.ExpiresAt.UnixMilli())) ||
		connection.PendingCredentialGeneration != nil || connection.PriorRecoveryGeneration != nil || connection.RotationID != nil ||
		connection.RevokedAt != nil || connection.TerminalAt != nil {
		return store.WarmAttachCommit{}, errors.New("sqlite warm attach target credential conflicts with existing state")
	}
	nowMS, err = sqliteNowMillis(ctx, tx)
	if err != nil || !validSQLiteWarmAttachExpiry(request, time.UnixMilli(nowMS)) || validateSQLiteAttachAttempt(request.Attempt, time.UnixMilli(nowMS)) != nil {
		return store.WarmAttachCommit{}, errors.New("warm attach expiry is outside the Store-clock window")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_attach_attempts (attempt_jti_hash, attach_id, bootstrap_session_id, target_session_id, provider, fingerprint_domain, fingerprint_version, fingerprint_digest, fingerprint_key_version, expires_at_ns, admission_outcome, issued_credential_generation, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?, ?)`, request.Attempt.Identity.JTIHash[:], request.Attempt.Identity.AttachID, request.Attempt.Identity.BootstrapSessionID, request.Attempt.Identity.TargetSessionID, request.Attempt.Identity.Provider, request.Attempt.Fingerprint.Domain, request.Attempt.Fingerprint.Version, request.Attempt.Fingerprint.Digest[:], request.Attempt.Fingerprint.KeyVersion, request.Attempt.ExpiresAt.UnixNano(), *request.Attempt.IssuedCredentialGeneration, nowMS); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("insert sqlite warm attach attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_attachments (attach_id, bootstrap_session_id, target_session_id, status, delivery_state, delivery_version, expires_at_ns, target_credential_lineage_ref, created_at_ms, updated_at_ms) VALUES (?, ?, ?, 'join_pending', 'pending', 0, ?, ?, ?, ?)`, request.Attachment.Identity.AttachID, request.Attachment.Identity.BootstrapSessionID, request.Attachment.Identity.TargetSessionID, request.Attachment.ExpiresAt.UnixNano(), request.Attachment.Identity.TargetCredentialLineageRef, nowMS, nowMS); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("insert sqlite warm attach attachment: %w", err)
	}
	var latest int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM session_events WHERE session_id = ?`, request.Attachment.Identity.TargetSessionID).Scan(&latest); err != nil {
		return store.WarmAttachCommit{}, err
	}
	seq := latest + 1
	event := store.PendingEvent{Type: "session.message", Time: time.UnixMilli(nowMS), Payload: sqliteWarmAttachPayload(request.FirstDelivery)}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)`, request.Attachment.Identity.TargetSessionID, seq, event.Type, event.Payload, nowMS, nowMS); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("append sqlite warm attach reference: %w", err)
	}
	if terminal, err := projectSQLiteAttentionEvent(ctx, tx, request.Attachment.Identity.TargetSessionID, seq, event, nowMS); err != nil || terminal {
		return store.WarmAttachCommit{}, errors.New("sqlite warm attach reference event is invalid")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_pending_commands (session_id, cmd_id, type, event_seq, status, expires_at_ns, created_at_ms, updated_at_ms) VALUES (?, ?, 'session.send', ?, 'pending', ?, ?, ?)`, request.Attachment.Identity.TargetSessionID, request.FirstDelivery.CommandID, seq, request.FirstDelivery.ExpiresAt.UnixNano(), nowMS, nowMS); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("insert sqlite warm attach outbox: %w", err)
	}
	attachment, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, request.Attachment.Identity.AttachID))
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	if err := upsertSQLiteAttentionLedger(ctx, tx, attachment.Identity.TargetSessionID, sqliteAttentionBlockerForAttachment(attachment, nil), &nowMS); err != nil {
		return store.WarmAttachCommit{}, err
	}
	summary, err = querySQLiteAttentionSummary(ctx, tx.QueryRowContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id = ?`, attachment.Identity.TargetSessionID))
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	attempt, err := querySQLiteAttachAttempt(ctx, tx, request.Attempt.Identity.JTIHash)
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.WarmAttachCommit{}, fmt.Errorf("commit sqlite warm attach: %w", err)
	}
	return store.WarmAttachCommit{Attempt: attempt, Attachment: attachment, TargetActivation: request.TargetActivation, Outbox: store.WarmAttachOutbox{TargetSessionID: attachment.Identity.TargetSessionID, CommandID: request.FirstDelivery.CommandID, EventSeq: seq, ReferenceID: request.FirstDelivery.ReferenceID, ReferenceDigest: request.FirstDelivery.ReferenceDigest, ExpiresAt: *attachment.ExpiresAt}, Summary: summary}, nil
}

func (s *Store) ExpireWarmAttach(ctx context.Context, attachID string, expectedDeliveryVersion int64) (store.WarmAttachExpiry, error) {
	if !validConnectionID(attachID) || expectedDeliveryVersion < 0 {
		return store.WarmAttachExpiry{}, errors.New("invalid sqlite warm attach expiry")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.WarmAttachExpiry{}, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, attachID))
	if err != nil {
		return store.WarmAttachExpiry{}, err
	}
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil || current.DeliveryVersion != expectedDeliveryVersion || current.Status != store.AttachmentJoinPending || current.ExpiresAt == nil || !current.ExpiresAt.Before(time.UnixMilli(nowMS)) {
		return store.WarmAttachExpiry{}, errors.New("sqlite warm attach is not expirable")
	}
	deliveryState, blocker := store.AttachmentDeliveryPending, &store.AttentionBlocker{Kind: store.AttentionBlockerReauthorizationRequired}
	if current.DeliveryState != store.AttachmentDeliveryPending {
		deliveryState = store.AttachmentDeliveryOutcomeUnknown
		operation := "credential_handoff"
		blocker = &store.AttentionBlocker{Kind: store.AttentionBlockerOutcomeUnknown, Operation: &operation}
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_attachments SET status = 'reauthorization_required', delivery_state = ?, delivery_version = delivery_version + 1, expires_at_ns = NULL, updated_at_ms = ? WHERE attach_id = ? AND delivery_version = ?`, deliveryState, nowMS, attachID, expectedDeliveryVersion)
	if err != nil {
		return store.WarmAttachExpiry{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.WarmAttachExpiry{}, errors.New("sqlite warm attach expiry conflict")
	}
	updated, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, attachID))
	if err != nil {
		return store.WarmAttachExpiry{}, err
	}
	if err := upsertSQLiteAttentionLedger(ctx, tx, updated.Identity.TargetSessionID, blocker, nil); err != nil {
		return store.WarmAttachExpiry{}, err
	}
	summary, err := querySQLiteAttentionSummary(ctx, tx.QueryRowContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id = ?`, updated.Identity.TargetSessionID))
	if err != nil {
		return store.WarmAttachExpiry{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.WarmAttachExpiry{}, err
	}
	return store.WarmAttachExpiry{Attachment: updated, Summary: summary}, nil
}

func validateSQLiteWarmAttach(request store.WarmAttachRequest) error {
	if request.Attempt.Identity.JTIHash == ([32]byte{}) || request.Attempt.Identity.Provider == "" || request.Attempt.Fingerprint.Domain != "agentwharf.attach-request.v1" || request.Attempt.Fingerprint.Version != 1 || request.Attempt.Fingerprint.Digest == ([32]byte{}) || request.Attempt.Fingerprint.KeyVersion < 1 || request.Attempt.Outcome != store.AttachAttemptAccepted || request.Attempt.IssuedCredentialGeneration == nil || *request.Attempt.IssuedCredentialGeneration != request.TargetActivation.Generation || request.Attachment.Identity.AttachID != request.Attempt.Identity.AttachID || request.Attachment.Identity.BootstrapSessionID != request.Attempt.Identity.BootstrapSessionID || request.Attachment.Identity.TargetSessionID != request.Attempt.Identity.TargetSessionID || validateAttachmentIdentity(request.Attachment.Identity) != nil || request.TargetActivation.Generation < 1 || request.TargetActivation.ExpiresAt.IsZero() || !request.TargetActivation.ExpiresAt.Equal(request.Attachment.ExpiresAt) || request.BootstrapAdmission.CredentialGeneration < 1 || request.BootstrapAdmission.ConnectionEpoch < 1 || request.BootstrapAdmission.AcceptedFence < 1 || request.BootstrapAdmission.GrantFence <= request.BootstrapAdmission.AcceptedFence || !validAttachmentText(request.FirstDelivery.CommandID, 256) || !validAttachmentText(request.FirstDelivery.ReferenceID, 255) || request.FirstDelivery.ReferenceDigest == ([32]byte{}) || !request.FirstDelivery.ExpiresAt.Equal(request.Attachment.ExpiresAt) {
		return errors.New("invalid sqlite warm attach request")
	}
	return nil
}

func validSQLiteWarmAttachExpiry(request store.WarmAttachRequest, now time.Time) bool {
	return request.Attempt.ExpiresAt.After(now) && request.Attempt.ExpiresAt.Before(now.Add(maxAttachAttemptTTL).Add(time.Nanosecond)) && validAttachmentExpiry(request.Attachment.ExpiresAt, now) && !request.Attachment.ExpiresAt.After(request.Attempt.ExpiresAt)
}

func sqliteWarmAttachPayload(delivery store.WarmAttachFirstDelivery) []byte {
	payload, _ := json.Marshal(struct {
		Role            string `json:"role"`
		ReferenceID     string `json:"reference_id"`
		ReferenceDigest string `json:"reference_digest"`
	}{Role: "user", ReferenceID: delivery.ReferenceID, ReferenceDigest: fmt.Sprintf("%x", delivery.ReferenceDigest)})
	return payload
}

func sqliteWarmAttachDuplicate(ctx context.Context, tx *sql.Tx, attempt store.AttachAttempt, request store.WarmAttachRequest, nowMS int64) (store.WarmAttachCommit, error) {
	if !sameSQLiteAttachAttempt(attempt, request.Attempt) {
		return store.WarmAttachCommit{}, errors.New("sqlite warm attach is immutable")
	}
	attachment, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, request.Attachment.Identity.AttachID))
	if err != nil || attachment.Identity != request.Attachment.Identity {
		return store.WarmAttachCommit{}, errors.New("sqlite warm attach attachment is immutable")
	}
	connection, err := queryAdapterConnection(ctx, tx, attachment.Identity.TargetSessionID)
	if err != nil || connection.ConnectionEpoch != 0 || connection.AcceptedFence != 0 ||
		connection.ActiveCredentialGeneration != request.TargetActivation.Generation || connection.CredentialGenerationHighWatermark != request.TargetActivation.Generation ||
		!connection.ActiveCredentialExpiresAt.Equal(time.UnixMilli(request.TargetActivation.ExpiresAt.UnixMilli())) ||
		connection.PendingCredentialGeneration != nil || connection.PriorRecoveryGeneration != nil || connection.RotationID != nil ||
		connection.RevokedAt != nil || connection.TerminalAt != nil {
		return store.WarmAttachCommit{}, errors.New("sqlite warm attach target credential is immutable")
	}
	command, err := queryPendingCommand(ctx, tx, attachment.Identity.TargetSessionID, request.FirstDelivery.CommandID, nowMS)
	if err != nil || command.Type != "session.send" || !command.ExpiresAt.Equal(request.FirstDelivery.ExpiresAt) {
		return store.WarmAttachCommit{}, errors.New("sqlite warm attach outbox is immutable")
	}
	var eventType string
	var payload []byte
	if err := tx.QueryRowContext(ctx, `SELECT type, payload FROM session_events WHERE session_id = ? AND seq = ?`, attachment.Identity.TargetSessionID, command.EventSeq).Scan(&eventType, &payload); err != nil || !bytes.Equal(payload, sqliteWarmAttachPayload(request.FirstDelivery)) || eventType != "session.message" {
		return store.WarmAttachCommit{}, errors.New("sqlite warm attach reference is immutable")
	}
	summary, err := querySQLiteAttentionSummary(ctx, tx.QueryRowContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id = ?`, attachment.Identity.TargetSessionID))
	if err != nil {
		return store.WarmAttachCommit{}, err
	}
	return store.WarmAttachCommit{Attempt: attempt, Attachment: attachment, TargetActivation: request.TargetActivation, Outbox: store.WarmAttachOutbox{TargetSessionID: attachment.Identity.TargetSessionID, CommandID: command.CommandID, EventSeq: command.EventSeq, ReferenceID: request.FirstDelivery.ReferenceID, ReferenceDigest: request.FirstDelivery.ReferenceDigest, ExpiresAt: command.ExpiresAt}, Summary: summary, Duplicate: true}, nil
}

func (s *Store) ReserveWorkspaceLease(ctx context.Context, reserve store.WorkspaceLeaseReserve) (store.WorkspaceLease, error) {
	if s == nil || s.db == nil {
		return store.WorkspaceLease{}, errors.New("sqlite event store is nil")
	}
	if err := validateSQLiteWorkspaceLeaseReserve(reserve); err != nil {
		return store.WorkspaceLease{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("begin workspace lease reserve: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if reserve.ExpiresAt.UnixMilli() <= nowMS {
		return store.WorkspaceLease{}, errors.New("workspace lease expiry is not in the future")
	}
	if reserve.ChildScope != nil && reserve.ChildScope.ExpiresAt.UnixMilli() <= nowMS {
		return store.WorkspaceLease{}, errors.New("workspace lease child scope expiry is not in the future")
	}
	childParent, childDigest, childExpiry := nullableSQLiteWorkspaceChild(reserve.ChildScope)
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_workspace_leases
(workspace_key, worker_id, session_id, connection_epoch, credential_generation, lease_id, child_parent_workspace_key, child_capability_digest, child_scope_expires_at_ns, status, version, expires_at_ns, created_at_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved', 1, ?, ?, ?)`, reserve.Key[:], reserve.Owner.WorkerID, reserve.Owner.SessionID, reserve.Owner.ConnectionEpoch, reserve.Owner.CredentialGeneration, reserve.Owner.LeaseID, childParent, childDigest, childExpiry, reserve.ExpiresAt.UnixNano(), nowMS, nowMS)
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("insert workspace lease: %w", err)
	}
	lease, err := querySQLiteWorkspaceLease(ctx, tx, reserve.Key)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if lease.Status == store.WorkspaceLeaseReserved && sameSQLiteWorkspaceLeaseReserve(lease, reserve) {
		if err := tx.Commit(); err != nil {
			return store.WorkspaceLease{}, fmt.Errorf("commit workspace lease reserve: %w", err)
		}
		return lease, nil
	}
	if lease.Status != store.WorkspaceLeaseReleased {
		return store.WorkspaceLease{}, errors.New("workspace lease already has a live owner")
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_workspace_leases SET worker_id=?, session_id=?, connection_epoch=?, credential_generation=?, lease_id=?, child_parent_workspace_key=?, child_capability_digest=?, child_scope_expires_at_ns=?, status='reserved', version=version+1, expires_at_ns=?, updated_at_ms=? WHERE workspace_key=? AND status='released'`, reserve.Owner.WorkerID, reserve.Owner.SessionID, reserve.Owner.ConnectionEpoch, reserve.Owner.CredentialGeneration, reserve.Owner.LeaseID, childParent, childDigest, childExpiry, reserve.ExpiresAt.UnixNano(), nowMS, reserve.Key[:])
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("reserve released workspace lease: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return store.WorkspaceLease{}, errors.New("workspace lease reserve conflict")
	}
	lease, err = querySQLiteWorkspaceLease(ctx, tx, reserve.Key)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("commit workspace lease replacement: %w", err)
	}
	return lease, nil
}

func (s *Store) WorkspaceLease(ctx context.Context, key store.WorkspaceLeaseKey) (store.WorkspaceLease, error) {
	if s == nil || s.db == nil {
		return store.WorkspaceLease{}, errors.New("sqlite event store is nil")
	}
	if key == (store.WorkspaceLeaseKey{}) {
		return store.WorkspaceLease{}, errors.New("workspace lease key is required")
	}
	lease, err := querySQLiteWorkspaceLease(ctx, s.db, key)
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("read workspace lease: %w", err)
	}
	return lease, nil
}

func (s *Store) RecordWorkspaceStartReceived(ctx context.Context, key store.WorkspaceLeaseKey, expectedVersion int64, owner store.WorkspaceLeaseOwner) (store.WorkspaceLease, error) {
	return s.updateSQLiteWorkspaceLease(ctx, key, expectedVersion, owner, "reserved", "start_received", true, true)
}

func (s *Store) QuarantineWorkspaceLease(ctx context.Context, key store.WorkspaceLeaseKey, expectedVersion int64) (store.WorkspaceLease, error) {
	return s.updateSQLiteWorkspaceLease(ctx, key, expectedVersion, store.WorkspaceLeaseOwner{}, "reserved,start_received", "quarantined", false, false)
}

func (s *Store) ReleaseWorkspaceLeaseAfterQuiescence(ctx context.Context, key store.WorkspaceLeaseKey, expectedVersion int64, owner store.WorkspaceLeaseOwner) (store.WorkspaceLease, error) {
	return s.updateSQLiteWorkspaceLease(ctx, key, expectedVersion, owner, "reserved,start_received,quarantined", "released", true, false)
}

func (s *Store) updateSQLiteWorkspaceLease(ctx context.Context, key store.WorkspaceLeaseKey, expectedVersion int64, owner store.WorkspaceLeaseOwner, from, to string, requireOwner, requireAuthority bool) (store.WorkspaceLease, error) {
	if s == nil || s.db == nil {
		return store.WorkspaceLease{}, errors.New("sqlite event store is nil")
	}
	if key == (store.WorkspaceLeaseKey{}) || expectedVersion < 1 || (requireOwner && !validSQLiteWorkspaceLeaseOwner(owner)) {
		return store.WorkspaceLease{}, errors.New("invalid workspace lease transition")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("begin workspace lease transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if requireAuthority {
		if err := validateSQLiteWorkspaceLeaseAuthority(ctx, tx, owner, nowMS); err != nil {
			return store.WorkspaceLease{}, err
		}
	}
	ownerClause, args := "", []any{to, nowMS, key[:], expectedVersion}
	if requireOwner {
		ownerClause = " AND worker_id=? AND session_id=? AND connection_epoch=? AND credential_generation=? AND lease_id=?"
		args = append(args, owner.WorkerID, owner.SessionID, owner.ConnectionEpoch, owner.CredentialGeneration, owner.LeaseID)
	}
	expiryClause := ""
	if to == "start_received" {
		expiryClause = ` AND expires_at_ns > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) * 1000000
AND (child_scope_expires_at_ns IS NULL OR child_scope_expires_at_ns > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) * 1000000)
AND EXISTS (SELECT 1 FROM session_adapter_connections AS c JOIN session_attachments AS a ON a.target_session_id = c.session_id
            WHERE c.session_id = session_workspace_leases.session_id AND c.connection_epoch = session_workspace_leases.connection_epoch
              AND c.active_credential_generation = session_workspace_leases.credential_generation
              AND c.active_credential_expires_at_ms > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
              AND c.revoked_at_ms IS NULL AND c.terminal_at_ms IS NULL AND a.status IN ('queued', 'start_received')
              AND (a.expires_at_ns IS NULL OR a.expires_at_ns > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) * 1000000))`
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_workspace_leases SET status=?, version=version+1, updated_at_ms=? WHERE workspace_key=? AND version=?`+ownerClause+expiryClause+` AND status IN (`+sqliteWorkspaceStatuses(from)+`)`, args...)
	if err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("update workspace lease: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return store.WorkspaceLease{}, errors.New("workspace lease transition conflict")
	}
	lease, err := querySQLiteWorkspaceLease(ctx, tx, key)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.WorkspaceLease{}, fmt.Errorf("commit workspace lease transition: %w", err)
	}
	return lease, nil
}

func sqliteWorkspaceStatuses(value string) string {
	return "'" + strings.ReplaceAll(value, ",", "','") + "'"
}

func validateSQLiteWorkspaceLeaseReserve(reserve store.WorkspaceLeaseReserve) error {
	if reserve.Key == (store.WorkspaceLeaseKey{}) || !validSQLiteWorkspaceLeaseOwner(reserve.Owner) || reserve.ExpiresAt.IsZero() {
		return errors.New("invalid workspace lease reserve")
	}
	if scope := reserve.ChildScope; scope != nil && (scope.ParentKey == (store.WorkspaceLeaseKey{}) || scope.ParentKey == reserve.Key || scope.CapabilityDigest == ([32]byte{}) || scope.ExpiresAt.IsZero()) {
		return errors.New("invalid workspace child scope")
	}
	return nil
}
func validSQLiteWorkspaceLeaseOwner(owner store.WorkspaceLeaseOwner) bool {
	return validConnectionID(owner.WorkerID) && validConnectionID(owner.SessionID) && validConnectionID(owner.LeaseID) && owner.ConnectionEpoch > 0 && owner.CredentialGeneration > 0
}
func nullableSQLiteWorkspaceChild(scope *store.WorkspaceLeaseChildScope) (any, any, any) {
	if scope == nil {
		return nil, nil, nil
	}
	return scope.ParentKey[:], scope.CapabilityDigest[:], scope.ExpiresAt.UnixNano()
}
func sameSQLiteWorkspaceLeaseReserve(lease store.WorkspaceLease, reserve store.WorkspaceLeaseReserve) bool {
	return lease.Key == reserve.Key && lease.Owner == reserve.Owner && lease.ExpiresAt.Equal(reserve.ExpiresAt) && ((lease.ChildScope == nil && reserve.ChildScope == nil) || (lease.ChildScope != nil && reserve.ChildScope != nil && lease.ChildScope.ParentKey == reserve.ChildScope.ParentKey && lease.ChildScope.CapabilityDigest == reserve.ChildScope.CapabilityDigest && lease.ChildScope.ExpiresAt.Equal(reserve.ChildScope.ExpiresAt)))
}

func validateSQLiteWorkspaceLeaseAuthority(ctx context.Context, tx *sql.Tx, owner store.WorkspaceLeaseOwner, nowMS int64) error {
	var current bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM session_adapter_connections AS c JOIN session_attachments AS a ON a.target_session_id=c.session_id WHERE c.session_id=? AND c.connection_epoch=? AND c.active_credential_generation=? AND c.active_credential_expires_at_ms>? AND c.revoked_at_ms IS NULL AND c.terminal_at_ms IS NULL AND a.status IN ('queued','start_received') AND (a.expires_at_ns IS NULL OR a.expires_at_ns>?))`, owner.SessionID, owner.ConnectionEpoch, owner.CredentialGeneration, nowMS, nowMS*int64(time.Millisecond)).Scan(&current)
	if err != nil {
		return fmt.Errorf("validate workspace lease authority: %w", err)
	}
	if !current {
		return errors.New("workspace lease authority is no longer current")
	}
	return nil
}

func querySQLiteWorkspaceLease(ctx context.Context, executor sqliteConnectionExecutor, key store.WorkspaceLeaseKey) (store.WorkspaceLease, error) {
	var rawKey, rawParent, rawDigest []byte
	var worker, session, leaseID, status string
	var epoch, generation, version, expiryNS, createdMS, updatedMS int64
	var childExpiry sql.NullInt64
	err := executor.QueryRowContext(ctx, `SELECT workspace_key, worker_id, session_id, connection_epoch, credential_generation, lease_id, child_parent_workspace_key, child_capability_digest, child_scope_expires_at_ns, status, version, expires_at_ns, created_at_ms, updated_at_ms FROM session_workspace_leases WHERE workspace_key=?`, key[:]).Scan(&rawKey, &worker, &session, &epoch, &generation, &leaseID, &rawParent, &rawDigest, &childExpiry, &status, &version, &expiryNS, &createdMS, &updatedMS)
	if err != nil {
		return store.WorkspaceLease{}, err
	}
	if len(rawKey) != 32 || !bytes.Equal(rawKey, key[:]) || !validSQLiteWorkspaceLeaseOwner(store.WorkspaceLeaseOwner{WorkerID: worker, SessionID: session, ConnectionEpoch: epoch, CredentialGeneration: generation, LeaseID: leaseID}) || version < 1 || expiryNS <= createdMS*int64(time.Millisecond) || updatedMS < createdMS {
		return store.WorkspaceLease{}, errors.New("workspace lease row is invalid")
	}
	out := store.WorkspaceLease{Key: key, Owner: store.WorkspaceLeaseOwner{WorkerID: worker, SessionID: session, ConnectionEpoch: epoch, CredentialGeneration: generation, LeaseID: leaseID}, Status: store.WorkspaceLeaseStatus(status), Version: version, ExpiresAt: time.Unix(0, expiryNS)}
	if status != "reserved" && status != "start_received" && status != "quarantined" && status != "released" {
		return store.WorkspaceLease{}, errors.New("workspace lease row is invalid")
	}
	if len(rawParent) == 0 && len(rawDigest) == 0 && !childExpiry.Valid {
		return out, nil
	}
	if len(rawParent) != 32 || len(rawDigest) != 32 || !childExpiry.Valid {
		return store.WorkspaceLease{}, errors.New("workspace lease row is invalid")
	}
	var parent store.WorkspaceLeaseKey
	var digest [32]byte
	copy(parent[:], rawParent)
	copy(digest[:], rawDigest)
	if parent == (store.WorkspaceLeaseKey{}) || parent == key || digest == ([32]byte{}) || childExpiry.Int64 <= createdMS*int64(time.Millisecond) {
		return store.WorkspaceLease{}, errors.New("workspace lease child scope is invalid")
	}
	out.ChildScope = &store.WorkspaceLeaseChildScope{ParentKey: parent, CapabilityDigest: digest, ExpiresAt: time.Unix(0, childExpiry.Int64)}
	return out, nil
}

type sqliteConnectionExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func fenceRunControlsAfterWriterReplacement(ctx context.Context, executor sqliteConnectionExecutor, sessionID string, nowMS int64) error {
	if _, err := executor.ExecContext(ctx, `DELETE FROM session_run_control_capabilities WHERE session_id=?`, sessionID); err != nil {
		return err
	}
	rows, err := executor.(interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}).QueryContext(ctx, `SELECT cmd_id,operation,reservation_version FROM session_run_controls WHERE session_id=? AND status='pending'`, sessionID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var commandID string
		var operation store.RunControlOperation
		var version int64
		if err := rows.Scan(&commandID, &operation, &version); err != nil {
			return err
		}
		payload, err := json.Marshal(struct {
			CommandID       string                    `json:"cmd_id"`
			Operation       store.RunControlOperation `json:"operation"`
			Outcome         store.RunControlOutcome   `json:"outcome"`
			CompletionState *string                   `json:"completion_state"`
			ReasonCode      *string                   `json:"reason_code"`
		}{commandID, operation, store.RunControlOutcomeUnknown, nil, stringPointer("adapter_disconnected")})
		if err != nil {
			return err
		}
		seq, err := appendRunControlEventExecutor(ctx, executor, sessionID, "session.run.outcome", string(payload), nowMS)
		if err != nil {
			return err
		}
		result, err := executor.ExecContext(ctx, `UPDATE session_run_controls SET status='outcome_unknown',terminal_event_seq=?,updated_at_ms=? WHERE session_id=? AND cmd_id=? AND reservation_version=? AND status='pending'`, seq, nowMS, sessionID, commandID, version)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return errors.New("run-control replacement fence lost race")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return fenceFileReferenceCommandsAfterWriterReplacement(ctx, executor, sessionID, nowMS)
}

func fenceFileReferenceCommandsAfterWriterReplacement(ctx context.Context, executor sqliteConnectionExecutor, sessionID string, nowMS int64) error {
	if _, err := executor.ExecContext(ctx, `DELETE FROM session_file_reference_capabilities WHERE session_id=?`, sessionID); err != nil {
		return err
	}
	rows, err := executor.(interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}).QueryContext(ctx, `SELECT cmd_id,message_id FROM session_file_reference_commands WHERE session_id=? AND status IN ('delivery_pending','pending')`, sessionID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var commandID, messageID string
		if err := rows.Scan(&commandID, &messageID); err != nil {
			return err
		}
		reason := "writer_lost"
		payload, err := json.Marshal(struct {
			MessageID      string  `json:"message_id"`
			CommandID      string  `json:"cmd_id"`
			Outcome        string  `json:"outcome"`
			ReferenceIndex *int    `json:"reference_index"`
			Reason         *string `json:"reason"`
		}{messageID, commandID, "outcome_unknown", nil, &reason})
		if err != nil {
			return err
		}
		seq, err := appendRunControlEventExecutor(ctx, executor, sessionID, "session.file_references.outcome", string(payload), nowMS)
		if err != nil {
			return err
		}
		result, err := executor.ExecContext(ctx, `UPDATE session_file_reference_commands SET status='outcome_unknown',terminal_event_seq=?,updated_at_ms=? WHERE session_id=? AND cmd_id=? AND status IN ('delivery_pending','pending')`, seq, nowMS, sessionID, commandID)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return errors.New("file-reference replacement fence lost race")
		}
	}
	return rows.Err()
}

func appendRunControlEventExecutor(ctx context.Context, executor sqliteConnectionExecutor, sessionID, eventType, payload string, nowMS int64) (int64, error) {
	var latest int64
	if err := executor.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM session_events WHERE session_id=?`, sessionID).Scan(&latest); err != nil {
		return 0, err
	}
	seq := latest + 1
	if _, err := executor.ExecContext(ctx, `INSERT INTO session_events (session_id,seq,type,payload,event_time_ms,created_at_ms) VALUES (?,?,?,?,?,?)`, sessionID, seq, eventType, []byte(payload), nowMS, nowMS); err != nil {
		return 0, err
	}
	return seq, nil
}

func stringPointer(value string) *string { return &value }

func (s *Store) AllocateAdapterGrantFence(ctx context.Context) (int64, error) {
	fence, err := s.allocateAdapterFence(ctx)
	if err != nil {
		return 0, err
	}
	if err := syncAdapterFence(ctx, s.connectionExecutor(), fence); err != nil {
		return 0, err
	}
	return fence, nil
}

func (s *Store) allocateAdapterFence(ctx context.Context) (int64, error) {
	if s == nil || s.fenceDB == nil {
		return 0, errors.New("sqlite grant fence store is nil")
	}
	var fence int64
	if err := s.fenceDB.QueryRowContext(ctx, `
UPDATE adapter_fence_allocator SET next_fence = next_fence + 1
WHERE singleton = 1 AND typeof(next_fence) = 'integer' AND next_fence < 9223372036854775807 AND EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = 'adapter_fence_allocator_advance')
RETURNING next_fence - 1
`).Scan(&fence); err != nil {
		return 0, fmt.Errorf("allocate adapter grant fence: %w", err)
	}
	if fence < 1 {
		return 0, errors.New("sqlite allocated invalid grant fence")
	}
	return fence, nil
}

func syncAdapterFence(ctx context.Context, executor sqliteConnectionExecutor, fence int64) error {
	_, err := executor.ExecContext(ctx, `UPDATE session_adapter_fence_allocator SET next_fence = MAX(next_fence, ?) WHERE singleton = 1`, fence+1)
	return err
}

func (s *Store) WithAdapterConnectionTransaction(ctx context.Context, fn func(store.AdapterConnectionStore) error) error {
	if fn == nil {
		return errors.New("adapter connection transaction callback is nil")
	}
	if s.connectionTx != nil {
		return errors.New("nested adapter connection transaction is not supported")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin adapter connection transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(&Store{db: s.db, fenceDB: s.fenceDB, connectionTx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit adapter connection transaction: %w", err)
	}
	return nil
}

func (s *Store) InitializeAdapterConnection(ctx context.Context, request store.AdapterConnectionInitialize) (store.AdapterConnection, error) {
	if !validConnectionID(request.SessionID) || request.ActiveCredentialGeneration < 1 || request.ActiveCredentialExpiresAt.IsZero() {
		return store.AdapterConnection{}, errors.New("invalid adapter connection initialization")
	}
	return s.withConnectionMutation(ctx, func(executor sqliteConnectionExecutor) (store.AdapterConnection, error) {
		nowMS, err := sqliteNowMillis(ctx, executor)
		if err != nil || request.ActiveCredentialExpiresAt.UnixMilli() <= nowMS {
			return store.AdapterConnection{}, errors.New("adapter connection expiry is not in the future")
		}
		if _, err := executor.ExecContext(ctx, `INSERT OR IGNORE INTO session_adapter_connections
(session_id, connection_epoch, accepted_fence, active_credential_generation, credential_generation_high_watermark, active_credential_expires_at_ms, created_at_ms, updated_at_ms) VALUES (?, 0, 0, ?, ?, ?, ?, ?)`, request.SessionID, request.ActiveCredentialGeneration, request.ActiveCredentialGeneration,
			request.ActiveCredentialExpiresAt.UnixMilli(), nowMS, nowMS); err != nil {
			return store.AdapterConnection{}, fmt.Errorf("initialize adapter connection: %w", err)
		}
		connection, err := queryAdapterConnection(ctx, executor, request.SessionID)
		if err != nil {
			return store.AdapterConnection{}, fmt.Errorf("read initialized adapter connection: %w", err)
		}
		if connection.ConnectionEpoch != 0 || connection.AcceptedFence != 0 ||
			connection.ActiveCredentialGeneration != request.ActiveCredentialGeneration ||
			connection.CredentialGenerationHighWatermark != request.ActiveCredentialGeneration ||
			!connection.ActiveCredentialExpiresAt.Equal(time.UnixMilli(request.ActiveCredentialExpiresAt.UnixMilli())) ||
			connection.PendingCredentialGeneration != nil || connection.PriorRecoveryGeneration != nil ||
			connection.RotationID != nil || connection.RevokedAt != nil || connection.TerminalAt != nil {
			return store.AdapterConnection{}, errors.New("adapter connection initialization conflicts with existing state")
		}
		return connection, nil
	})
}

func (s *Store) ValidateWarmAttachTargetActivation(ctx context.Context, sessionID string, activation store.WarmAttachTargetActivation) error {
	if s == nil || s.db == nil || !validConnectionID(sessionID) || activation.Generation < 1 || activation.ExpiresAt.IsZero() {
		return errors.New("invalid sqlite warm attach target activation")
	}
	_, err := queryAdapterConnectionWhere(ctx, s.connectionExecutor(), `connection.session_id = ? AND connection.active_credential_generation = ? AND connection.credential_generation_high_watermark = ? AND connection.active_credential_expires_at_ms = ? AND connection.connection_epoch = 0 AND connection.accepted_fence = 0 AND connection.pending_credential_generation IS NULL AND connection.prior_recovery_credential_generation IS NULL AND connection.rotation_id IS NULL AND connection.revoked_at_ms IS NULL AND connection.terminal_at_ms IS NULL AND connection.active_credential_expires_at_ms > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`, sessionID, activation.Generation, activation.Generation, activation.ExpiresAt.UnixMilli())
	if err != nil {
		return errors.New("sqlite warm attach target activation is expired or fenced")
	}
	return nil
}

func (s *Store) RefreshAdapterCredentialBeforeHello(ctx context.Context, sessionID string, refresh store.AdapterCredentialPreHelloRefresh) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || refresh.ExpectedActiveCredentialGeneration < 1 || refresh.ActiveCredentialExpiresAt.IsZero() {
		return store.AdapterConnection{}, errors.New("invalid pre-hello adapter credential refresh")
	}
	return s.updateConnection(ctx, sessionID, `UPDATE session_adapter_connections SET active_credential_expires_at_ms = ?, updated_at_ms = ?
WHERE session_id = ? AND active_credential_generation = ? AND connection_epoch = 0 AND accepted_fence = 0 AND pending_credential_generation IS NULL AND prior_recovery_credential_generation IS NULL AND rotation_id IS NULL
AND revoked_at_ms IS NULL AND terminal_at_ms IS NULL AND ((active_credential_expires_at_ms = ? AND active_credential_expires_at_ms > ?) OR (active_credential_expires_at_ms <= ? AND ? > ? AND ? > active_credential_expires_at_ms))`, false, func(nowMS, _ int64) []any {
		expiresAtMS := refresh.ActiveCredentialExpiresAt.UnixMilli()
		return []any{expiresAtMS, nowMS, sessionID, refresh.ExpectedActiveCredentialGeneration,
			expiresAtMS, nowMS, nowMS, expiresAtMS, nowMS, expiresAtMS}
	})
}

func (s *Store) TerminateAdapterConnectionBeforeHello(ctx context.Context, sessionID string, termination store.AdapterConnectionPreHelloTermination) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || termination.ExpectedActiveCredentialGeneration < 1 {
		return store.AdapterConnection{}, errors.New("invalid pre-hello adapter connection termination")
	}
	return s.updateConnectionAfter(ctx, sessionID, `UPDATE session_adapter_connections SET revoked_at_ms = COALESCE(revoked_at_ms, ?), terminal_at_ms = COALESCE(terminal_at_ms, ?), updated_at_ms = ?
WHERE session_id = ? AND active_credential_generation = ? AND connection_epoch = 0 AND accepted_fence = 0 AND pending_credential_generation IS NULL AND prior_recovery_credential_generation IS NULL AND rotation_id IS NULL
AND ((revoked_at_ms IS NULL AND terminal_at_ms IS NULL) OR revoked_at_ms = terminal_at_ms)`, false, func(nowMS, _ int64) []any {
		return []any{nowMS, nowMS, nowMS, sessionID, termination.ExpectedActiveCredentialGeneration}
	}, func(executor sqliteConnectionExecutor, _ store.AdapterConnection, nowMS int64) error {
		return fenceRunControlsAfterWriterReplacement(ctx, executor, sessionID, nowMS)
	})
}

func (s *Store) AcceptAdapterHello(ctx context.Context, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || hello.CredentialGeneration < 1 {
		return store.AdapterConnection{}, errors.New("invalid adapter hello")
	}
	if _, err := s.AdapterConnection(ctx, sessionID); err != nil {
		return store.AdapterConnection{}, err
	}
	return s.updateConnectionAfter(ctx, sessionID, `UPDATE session_adapter_connections SET connection_epoch = connection_epoch + 1, accepted_fence = ?, updated_at_ms = ?
WHERE session_id = ? AND active_credential_generation = ? AND active_credential_expires_at_ms > ? AND revoked_at_ms IS NULL AND terminal_at_ms IS NULL`, true, func(nowMS, fence int64) []any {
		return []any{fence, nowMS, sessionID, hello.CredentialGeneration, nowMS}
	}, func(executor sqliteConnectionExecutor, connection store.AdapterConnection, nowMS int64) error {
		if hello.WriterLeaseID == "" {
			return fenceRunControlsAfterWriterReplacement(ctx, executor, sessionID, nowMS)
		}
		if len(hello.WriterLeaseID) > 255 {
			return errors.New("invalid adapter writer lease")
		}
		_, err := executor.ExecContext(ctx, `INSERT INTO session_settings_live_writers (session_id, connection_epoch, credential_generation, writer_lease_id)
VALUES (?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET connection_epoch=excluded.connection_epoch, credential_generation=excluded.credential_generation, writer_lease_id=excluded.writer_lease_id`, sessionID, connection.ConnectionEpoch, connection.ActiveCredentialGeneration, hello.WriterLeaseID)
		if err != nil {
			return err
		}
		return fenceRunControlsAfterWriterReplacement(ctx, executor, sessionID, nowMS)
	})
}
func (s *Store) ValidateAdapterAdmission(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || admission.CredentialGeneration < 1 || admission.ConnectionEpoch < 1 ||
		admission.AcceptedFence < 1 || admission.GrantFence <= admission.AcceptedFence {
		return store.AdapterConnection{}, errors.New("invalid adapter admission")
	}
	pending, err := s.attentionMigrationPending(ctx, s.connectionExecutor())
	if err != nil {
		return store.AdapterConnection{}, err
	}
	if pending {
		return store.AdapterConnection{}, errors.New("sqlite attention migration is incomplete")
	}
	if s.connectionTx != nil {
		if _, err := s.connectionTx.ExecContext(ctx, `UPDATE session_adapter_connections SET updated_at_ms=updated_at_ms WHERE session_id=?`, sessionID); err != nil {
			return store.AdapterConnection{}, fmt.Errorf("lock adapter admission: %w", err)
		}
	}
	connection, err := queryAdapterConnectionWhere(ctx, s.connectionExecutor(), `connection.session_id = ? AND connection.active_credential_generation = ?
AND connection.connection_epoch = ? AND connection.accepted_fence = ? AND ? > connection.accepted_fence AND ? < (SELECT next_fence FROM session_adapter_fence_allocator WHERE singleton = 1) AND ? < (SELECT next_fence FROM fence_store.adapter_fence_allocator WHERE singleton = 1)
AND connection.active_credential_expires_at_ms > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) AND connection.revoked_at_ms IS NULL AND connection.terminal_at_ms IS NULL`, sessionID, admission.CredentialGeneration, admission.ConnectionEpoch, admission.AcceptedFence,
		admission.GrantFence, admission.GrantFence, admission.GrantFence)
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("validate adapter admission: %w", err)
	}
	return connection, nil
}

func (s *Store) PrepareAdapterCredentialRotation(ctx context.Context, sessionID string, rotation store.AdapterCredentialRotation) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || rotation.ExpectedActiveCredentialGeneration < 1 || rotation.ExpectedEpoch < 1 ||
		rotation.PendingGeneration < 1 || !validAttachmentText(rotation.RotationID, 255) || rotation.ExpiresAt.IsZero() {
		return store.AdapterConnection{}, errors.New("invalid adapter credential rotation")
	}
	return s.updateConnection(ctx, sessionID, `UPDATE session_adapter_connections SET pending_credential_generation = ?, pending_credential_expires_at_ms = ?, rotation_id = ?, credential_generation_high_watermark = ?, updated_at_ms = ?
WHERE session_id = ? AND active_credential_generation = ? AND connection_epoch = ? AND connection_epoch > 0 AND accepted_fence > 0
AND (pending_credential_generation IS NULL OR (pending_credential_expires_at_ms IS NOT NULL AND pending_credential_expires_at_ms <= ?))
AND ? > credential_generation_high_watermark AND active_credential_expires_at_ms > ? AND ? > ? AND revoked_at_ms IS NULL AND terminal_at_ms IS NULL`, false, func(nowMS, _ int64) []any {
		expiresAtMS := rotation.ExpiresAt.UnixMilli()
		return []any{rotation.PendingGeneration, expiresAtMS, rotation.RotationID, rotation.PendingGeneration, nowMS,
			sessionID, rotation.ExpectedActiveCredentialGeneration, rotation.ExpectedEpoch,
			nowMS, rotation.PendingGeneration, nowMS, expiresAtMS, nowMS}
	})
}

func (s *Store) ActivateAdapterCredential(ctx context.Context, sessionID string, activation store.AdapterCredentialActivation) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) || activation.ExpectedActiveCredentialGeneration < 1 || activation.ExpectedEpoch < 1 ||
		activation.PendingGeneration < 1 || !validAttachmentText(activation.RotationID, 255) {
		return store.AdapterConnection{}, errors.New("invalid adapter credential activation")
	}
	if _, err := s.AdapterConnection(ctx, sessionID); err != nil {
		return store.AdapterConnection{}, err
	}
	return s.updateConnection(ctx, sessionID, `UPDATE session_adapter_connections SET prior_recovery_credential_generation = active_credential_generation, active_credential_generation = pending_credential_generation,
active_credential_expires_at_ms = pending_credential_expires_at_ms, pending_credential_generation = NULL, pending_credential_expires_at_ms = NULL, rotation_id = NULL,
connection_epoch = connection_epoch + 1, accepted_fence = ?, updated_at_ms = ? WHERE session_id = ? AND active_credential_generation = ? AND connection_epoch = ?
AND connection_epoch > 0 AND accepted_fence > 0 AND pending_credential_generation = ? AND rotation_id = ? AND active_credential_expires_at_ms > ? AND pending_credential_expires_at_ms > ?
AND revoked_at_ms IS NULL AND terminal_at_ms IS NULL`, true, func(nowMS, fence int64) []any {
		return []any{fence, nowMS, sessionID, activation.ExpectedActiveCredentialGeneration, activation.ExpectedEpoch,
			activation.PendingGeneration, activation.RotationID, nowMS, nowMS}
	})
}

func (s *Store) AdapterConnection(ctx context.Context, sessionID string) (store.AdapterConnection, error) {
	if !validConnectionID(sessionID) {
		return store.AdapterConnection{}, errors.New("invalid adapter connection session")
	}
	connection, err := queryAdapterConnection(ctx, s.connectionExecutor(), sessionID)
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("select adapter connection: %w", err)
	}
	return connection, nil
}

func (s *Store) connectionExecutor() sqliteConnectionExecutor {
	if s.connectionTx != nil {
		return s.connectionTx
	}
	return s.db
}

func (s *Store) withConnectionMutation(ctx context.Context, fn func(sqliteConnectionExecutor) (store.AdapterConnection, error)) (store.AdapterConnection, error) {
	if s.connectionTx != nil {
		return fn(s.connectionTx)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.AdapterConnection{}, fmt.Errorf("begin adapter connection mutation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	connection, err := fn(tx)
	if err != nil {
		return store.AdapterConnection{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.AdapterConnection{}, fmt.Errorf("commit adapter connection mutation: %w", err)
	}
	return connection, nil
}

func (s *Store) updateConnection(ctx context.Context, sessionID, statement string, fenced bool, args func(int64, int64) []any) (store.AdapterConnection, error) {
	return s.updateConnectionAfter(ctx, sessionID, statement, fenced, args, nil)
}

func (s *Store) updateConnectionAfter(ctx context.Context, sessionID, statement string, fenced bool, args func(int64, int64) []any, after func(sqliteConnectionExecutor, store.AdapterConnection, int64) error) (store.AdapterConnection, error) {
	return s.withConnectionMutation(ctx, func(executor sqliteConnectionExecutor) (store.AdapterConnection, error) {
		result, err := executor.ExecContext(ctx, `UPDATE session_adapter_connections SET updated_at_ms = updated_at_ms WHERE session_id = ?`, sessionID)
		if err != nil {
			return store.AdapterConnection{}, fmt.Errorf("lock adapter connection for mutation: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return store.AdapterConnection{}, errors.New("lock adapter connection for mutation")
		}
		var fence int64
		if fenced {
			fence, err = s.allocateAdapterFence(ctx)
			if err == nil {
				err = syncAdapterFence(ctx, executor, fence)
			}
			if err != nil {
				return store.AdapterConnection{}, err
			}
		}
		nowMS, err := sqliteNowMillis(ctx, executor)
		if err != nil {
			return store.AdapterConnection{}, err
		}
		if _, err := queryAdapterConnection(ctx, executor, sessionID); err != nil {
			return store.AdapterConnection{}, fmt.Errorf("validate adapter connection before mutation: %w", err)
		}
		result, err = executor.ExecContext(ctx, statement, args(nowMS, fence)...)
		if err != nil {
			return store.AdapterConnection{}, fmt.Errorf("update adapter connection: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return store.AdapterConnection{}, errors.New("adapter connection state conflict")
		}
		connection, err := queryAdapterConnection(ctx, executor, sessionID)
		if err != nil {
			return store.AdapterConnection{}, err
		}
		if after != nil {
			if err := after(executor, connection, nowMS); err != nil {
				return store.AdapterConnection{}, fmt.Errorf("bind adapter writer lease: %w", err)
			}
		}
		return connection, nil
	})
}

const adapterConnectionColumns = `connection.session_id, connection.connection_epoch, connection.accepted_fence, connection.active_credential_generation,
connection.credential_generation_high_watermark, connection.active_credential_expires_at_ms, connection.pending_credential_generation, connection.pending_credential_expires_at_ms,
connection.prior_recovery_credential_generation, connection.rotation_id, connection.revoked_at_ms, connection.terminal_at_ms, connection.created_at_ms, connection.updated_at_ms,
CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER), (SELECT next_fence FROM session_adapter_fence_allocator WHERE singleton = 1)`

func queryAdapterConnection(ctx context.Context, executor sqliteConnectionExecutor, sessionID string) (store.AdapterConnection, error) {
	return queryAdapterConnectionWhere(ctx, executor, `connection.session_id = ?`, sessionID)
}

func queryAdapterConnectionWhere(ctx context.Context, executor sqliteConnectionExecutor, predicate string, args ...any) (store.AdapterConnection, error) {
	var connection store.AdapterConnection
	var activeExpiresAtMS, createdAtMS, updatedAtMS, nowMS, nextFence int64
	var pendingGeneration, pendingExpiresAtMS, priorGeneration, revokedAtMS, terminalAtMS sql.NullInt64
	var rotationID sql.NullString
	err := executor.QueryRowContext(ctx, `SELECT `+adapterConnectionColumns+`
FROM session_adapter_connections AS connection WHERE `+predicate, args...).Scan(
		&connection.SessionID, &connection.ConnectionEpoch, &connection.AcceptedFence,
		&connection.ActiveCredentialGeneration, &connection.CredentialGenerationHighWatermark,
		&activeExpiresAtMS, &pendingGeneration, &pendingExpiresAtMS, &priorGeneration,
		&rotationID, &revokedAtMS, &terminalAtMS, &createdAtMS, &updatedAtMS, &nowMS, &nextFence,
	)
	if err != nil {
		return store.AdapterConnection{}, err
	}
	connection.ActiveCredentialExpiresAt = time.UnixMilli(activeExpiresAtMS)
	connection.PendingCredentialGeneration = nullInt64Pointer(pendingGeneration)
	connection.PendingCredentialExpiresAt = nullMilliTimePointer(pendingExpiresAtMS)
	connection.PriorRecoveryGeneration = nullInt64Pointer(priorGeneration)
	connection.RotationID = nullStringPointer(rotationID)
	connection.RevokedAt = nullMilliTimePointer(revokedAtMS)
	connection.TerminalAt = nullMilliTimePointer(terminalAtMS)
	if !validAdapterConnectionRow(connection, createdAtMS, updatedAtMS, nowMS, nextFence) {
		return store.AdapterConnection{}, errors.New("adapter connection row is invalid")
	}
	return connection, nil
}

func validAdapterConnectionRow(connection store.AdapterConnection, createdAtMS, updatedAtMS, nowMS, nextFence int64) bool {
	if !validConnectionID(connection.SessionID) || connection.ConnectionEpoch < 0 || connection.AcceptedFence < 0 ||
		(connection.ConnectionEpoch == 0) != (connection.AcceptedFence == 0) ||
		connection.ActiveCredentialGeneration < 1 || connection.CredentialGenerationHighWatermark < connection.ActiveCredentialGeneration ||
		connection.ActiveCredentialExpiresAt.UnixMilli() <= createdAtMS || createdAtMS < 1 || createdAtMS > updatedAtMS || updatedAtMS > nowMS ||
		nextFence < 1 || connection.AcceptedFence >= nextFence ||
		(connection.PendingCredentialGeneration == nil) != (connection.PendingCredentialExpiresAt == nil) ||
		(connection.PendingCredentialGeneration == nil) != (connection.RotationID == nil) {
		return false
	}
	if connection.PendingCredentialGeneration != nil && (*connection.PendingCredentialGeneration < 1 ||
		*connection.PendingCredentialGeneration > connection.CredentialGenerationHighWatermark ||
		*connection.PendingCredentialGeneration == connection.ActiveCredentialGeneration ||
		(connection.PriorRecoveryGeneration != nil && *connection.PendingCredentialGeneration == *connection.PriorRecoveryGeneration) ||
		connection.PendingCredentialExpiresAt.UnixMilli() <= createdAtMS || !validAttachmentText(*connection.RotationID, 255)) {
		return false
	}
	if connection.PriorRecoveryGeneration != nil && (*connection.PriorRecoveryGeneration < 1 ||
		*connection.PriorRecoveryGeneration > connection.CredentialGenerationHighWatermark ||
		*connection.PriorRecoveryGeneration == connection.ActiveCredentialGeneration) {
		return false
	}
	for _, timestamp := range []*time.Time{connection.RevokedAt, connection.TerminalAt} {
		if timestamp != nil && (timestamp.UnixMilli() < createdAtMS || timestamp.UnixMilli() > nowMS) {
			return false
		}
	}
	return true
}

func validConnectionID(value string) bool { return len(value) > 0 && len(value) <= 255 }

func nullInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

func sqliteNowMillis(ctx context.Context, tx interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (int64, error) {
	var nowMS int64
	if err := tx.QueryRowContext(ctx, `SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`).Scan(&nowMS); err != nil {
		return 0, fmt.Errorf("read sqlite Store clock: %w", err)
	}
	return nowMS, nil
}

func validateCommandAuthority(ctx context.Context, tx *sql.Tx, sessionID string, authority store.CommandAuthority, nowMS int64) error {
	var current bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM session_adapter_connections
    WHERE session_id = ? AND connection_epoch = ? AND active_credential_generation = ?
      AND active_credential_expires_at_ms > ? AND revoked_at_ms IS NULL AND terminal_at_ms IS NULL
)
`, sessionID, authority.ConnectionEpoch, authority.CredentialGeneration, nowMS).Scan(&current); err != nil {
		return fmt.Errorf("validate command authority: %w", err)
	}
	if !current {
		return errors.New("command authority is no longer current")
	}
	return nil
}

func lockCommandAuthority(ctx context.Context, tx *sql.Tx, sessionID string, authority store.CommandAuthority) error {
	result, err := tx.ExecContext(ctx, `
UPDATE session_adapter_connections SET updated_at_ms = updated_at_ms
WHERE session_id = ? AND connection_epoch = ? AND active_credential_generation = ?
  AND active_credential_expires_at_ms > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
  AND revoked_at_ms IS NULL AND terminal_at_ms IS NULL
`, sessionID, authority.ConnectionEpoch, authority.CredentialGeneration)
	if err != nil {
		return fmt.Errorf("lock command authority: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return errors.New("command authority is no longer current")
	}
	return nil
}

func verifySettingsCapabilityEvent(ctx context.Context, tx *sql.Tx, sessionID string, eventSeq int64) error {
	if eventSeq < 1 {
		return errors.New("settings capability event reference is invalid")
	}
	var eventType string
	if err := tx.QueryRowContext(ctx, `SELECT type FROM session_events WHERE session_id=? AND seq=?`, sessionID, eventSeq).Scan(&eventType); err != nil || eventType != "session.settings.capabilities" {
		return errors.New("settings capability event is not durable")
	}
	return nil
}

func validateCurrentSettingsWriter(ctx context.Context, tx *sql.Tx, sessionID string, writer store.SettingsWriter) error {
	var current store.SettingsWriter
	if err := tx.QueryRowContext(ctx, `SELECT writer_connection_epoch, writer_credential_generation, writer_lease_id FROM session_settings_capabilities WHERE session_id=?`, sessionID).Scan(&current.ConnectionEpoch, &current.CredentialGeneration, &current.LeaseID); err != nil || current != writer {
		return errors.New("settings writer is no longer current")
	}
	return validateLiveSettingsWriter(ctx, tx, sessionID, writer)
}
func validateLiveSettingsWriter(ctx context.Context, tx *sql.Tx, sessionID string, writer store.SettingsWriter) error {
	var current bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (
SELECT 1 FROM session_settings_live_writers AS writer
JOIN session_adapter_connections AS connection ON connection.session_id = writer.session_id
WHERE writer.session_id=? AND writer.connection_epoch=? AND writer.credential_generation=? AND writer.writer_lease_id=?
  AND connection.connection_epoch=writer.connection_epoch AND connection.active_credential_generation=writer.credential_generation
  AND connection.active_credential_expires_at_ms > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
  AND connection.revoked_at_ms IS NULL AND connection.terminal_at_ms IS NULL
)`, sessionID, writer.ConnectionEpoch, writer.CredentialGeneration, writer.LeaseID).Scan(&current)
	if err != nil || !current {
		return errors.New("settings writer has no live authority")
	}
	return nil
}

func queryPendingCommand(ctx context.Context, tx *sql.Tx, sessionID, commandID string, nowMS int64) (store.PendingCommand, error) {
	var command store.PendingCommand
	var status, eventType string
	var expiresAtNS, createdAtMS int64
	var eventPayload []byte
	err := tx.QueryRowContext(ctx, `
SELECT command.session_id, command.cmd_id, command.type, command.event_seq, command.status,
       command.expires_at_ns, command.created_at_ms, event.type, event.payload
FROM session_pending_commands AS command
JOIN session_events AS event
  ON event.session_id = command.session_id AND event.seq = command.event_seq
WHERE command.session_id = ? AND command.cmd_id = ?
  AND length(event.payload) BETWEEN 1 AND ?
`, sessionID, commandID, maxEventPayloadSize).Scan(
		&command.SessionID, &command.CommandID, &command.Type, &command.EventSeq, &status,
		&expiresAtNS, &createdAtMS, &eventType, &eventPayload,
	)
	if err != nil {
		return store.PendingCommand{}, err
	}
	command.Status = store.PendingCommandStatus(status)
	command.ExpiresAt = time.Unix(0, expiresAtNS)
	var payload struct {
		Role string `json:"role"`
	}
	if command.SessionID == "" || len(command.SessionID) > 255 || command.CommandID == "" || len(command.CommandID) > 256 ||
		command.Type != "session.send" || command.EventSeq < 1 || !validPendingCommandStatus(command.Status) ||
		createdAtMS < 1 || createdAtMS > nowMS || expiresAtNS <= createdAtMS*int64(time.Millisecond) ||
		expiresAtNS > (createdAtMS+30000)*int64(time.Millisecond) || eventType != "session.message" ||
		expiresAtNS > (nowMS+30000)*int64(time.Millisecond) || len(eventPayload) < 1 || len(eventPayload) > maxEventPayloadSize ||
		json.Unmarshal(eventPayload, &payload) != nil || payload.Role != "user" {
		return store.PendingCommand{}, errors.New("pending command row is invalid")
	}
	return command, nil
}

func validPendingCommandStatus(status store.PendingCommandStatus) bool {
	switch status {
	case store.PendingCommandPending, store.PendingCommandReceived, store.PendingCommandCompleted, store.PendingCommandOutcomeUnknown:
		return true
	default:
		return false
	}
}

func validatePendingCommandInput(event store.PendingEvent, request store.PendingCommandRequest, storeNow time.Time) error {
	var payload struct {
		Role string `json:"role"`
	}
	if request.CommandID == "" || len(request.CommandID) > 256 || request.Type != "session.send" ||
		event.Type != "session.message" || len(event.Payload) < 1 || len(event.Payload) > maxEventPayloadSize ||
		json.Unmarshal(event.Payload, &payload) != nil || payload.Role != "user" ||
		!request.ExpiresAt.After(storeNow) || request.ExpiresAt.After(storeNow.Add(30*time.Second)) {
		return errors.New("invalid pending command")
	}
	return nil
}
func querySettingsCapability(ctx context.Context, tx *sql.Tx, sessionID string) (store.SettingsCapability, error) {
	var capability store.SettingsCapability
	err := tx.QueryRowContext(ctx, `SELECT session_id, capability_event_seq, fingerprint, effective_model_id, effective_permission_mode_id, capability_version, writer_connection_epoch, writer_credential_generation, writer_lease_id FROM session_settings_capabilities WHERE session_id=?`, sessionID).Scan(
		&capability.SessionID, &capability.EventSeq, &capability.Fingerprint, &capability.EffectiveModelID, &capability.EffectivePermissionModeID,
		&capability.Version, &capability.Writer.ConnectionEpoch, &capability.Writer.CredentialGeneration, &capability.Writer.LeaseID,
	)
	if err != nil {
		return store.SettingsCapability{}, err
	}
	if !validSettingsCapabilityUpdate(capability.SessionID, store.SettingsCapabilityUpdate{EventSeq: capability.EventSeq, Fingerprint: capability.Fingerprint, EffectiveModelID: capability.EffectiveModelID, EffectivePermissionModeID: capability.EffectivePermissionModeID, Writer: capability.Writer}) || capability.Version < 1 {
		return store.SettingsCapability{}, errors.New("settings capability row is invalid")
	}
	return capability, nil
}

func querySettingsCommand(ctx context.Context, tx *sql.Tx, sessionID, commandID string) (store.SettingsCommand, error) {
	var command store.SettingsCommand
	var modelID, permissionID sql.NullString
	var deadlineMS int64
	var operationDeadline, terminalSeq sql.NullInt64
	var status string
	err := tx.QueryRowContext(ctx, `SELECT session_id, cmd_id, request_fingerprint, requested_model_id, requested_permission_mode_id, reservation_version, delivery_deadline_ms, operation_deadline_ms, writer_connection_epoch, writer_credential_generation, writer_lease_id, reserved_capability_event_seq, reserved_fingerprint, reserved_effective_model_id, reserved_effective_permission_mode_id, status, terminal_event_seq FROM session_settings_commands WHERE session_id=? AND cmd_id=?`, sessionID, commandID).Scan(
		&command.SessionID, &command.CommandID, &command.RequestFingerprint, &modelID, &permissionID, &command.ReservationVersion,
		&deadlineMS, &operationDeadline, &command.Writer.ConnectionEpoch, &command.Writer.CredentialGeneration, &command.Writer.LeaseID,
		&command.ReservedCapability.EventSeq, &command.ReservedCapability.Fingerprint, &command.ReservedCapability.EffectiveModelID, &command.ReservedCapability.EffectivePermissionModeID, &status, &terminalSeq,
	)
	if err != nil {
		return store.SettingsCommand{}, err
	}
	command.DeliveryDeadline = time.UnixMilli(deadlineMS)
	command.ReservedCapability.SessionID = sessionID
	command.ReservedCapability.Writer = command.Writer
	command.Status = store.SettingsCommandStatus(status)
	if modelID.Valid {
		command.RequestedModelID = &modelID.String
	}
	if permissionID.Valid {
		command.RequestedPermissionModeID = &permissionID.String
	}
	if operationDeadline.Valid {
		value := time.UnixMilli(operationDeadline.Int64)
		command.OperationDeadline = &value
	}
	if terminalSeq.Valid {
		value := terminalSeq.Int64
		command.TerminalEventSeq = &value
	}
	if !validSettingsCommandRow(command) {
		return store.SettingsCommand{}, errors.New("settings command row is invalid")
	}
	return command, nil
}

func validSettingsCapabilityUpdate(sessionID string, update store.SettingsCapabilityUpdate) bool {
	return validConnectionID(sessionID) && update.EventSeq > 0 && validSettingsFingerprint(update.Fingerprint) && validSettingsID(update.EffectiveModelID) && validSettingsID(update.EffectivePermissionModeID) && validSettingsWriter(update.Writer)
}

func validSettingsCommandRequest(sessionID string, request store.SettingsCommandRequest) bool {
	return validConnectionID(sessionID) && request.CommandID != "" && len(request.CommandID) <= 256 && validSettingsFingerprint(request.RequestFingerprint) &&
		(request.RequestedModelID != nil || request.RequestedPermissionModeID != nil) &&
		(request.RequestedModelID == nil || validSettingsID(*request.RequestedModelID)) &&
		(request.RequestedPermissionModeID == nil || validSettingsID(*request.RequestedPermissionModeID)) && validSettingsWriter(request.Writer)
}

func validSettingsCommandRow(command store.SettingsCommand) bool {
	terminal := command.Status != store.SettingsCommandDeliveryPending && command.Status != store.SettingsCommandPending && command.Status != store.SettingsCommandRecoveryPending
	return validSettingsCommandRequest(command.SessionID, store.SettingsCommandRequest{CommandID: command.CommandID, RequestFingerprint: command.RequestFingerprint, RequestedModelID: command.RequestedModelID, RequestedPermissionModeID: command.RequestedPermissionModeID, Writer: command.Writer}) && command.ReservationVersion > 0 && !command.DeliveryDeadline.IsZero() && validSettingsStatus(command.Status) && sameSettingsCapability(command.ReservedCapability, store.SettingsCapability{SessionID: command.SessionID, EventSeq: command.ReservedCapability.EventSeq, Fingerprint: command.ReservedCapability.Fingerprint, EffectiveModelID: command.ReservedCapability.EffectiveModelID, EffectivePermissionModeID: command.ReservedCapability.EffectivePermissionModeID, Writer: command.Writer}) && ((terminal && command.TerminalEventSeq != nil) || (!terminal && command.TerminalEventSeq == nil))
}

func validSettingsWriter(writer store.SettingsWriter) bool {
	return writer.ConnectionEpoch > 0 && writer.CredentialGeneration > 0 && writer.LeaseID != "" && len(writer.LeaseID) <= 255
}

type runControlQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type fileReferenceQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func queryFileReferenceCapability(ctx context.Context, querier fileReferenceQuerier, sessionID string) (store.FileReferenceCapability, error) {
	var capability store.FileReferenceCapability
	err := querier.QueryRowContext(ctx, `SELECT session_id,capability_event_seq,capability_fingerprint,writer_connection_epoch,writer_credential_generation,writer_lease_id FROM session_file_reference_capabilities WHERE session_id=?`, sessionID).Scan(&capability.SessionID, &capability.EventSeq, &capability.Fingerprint, &capability.Writer.ConnectionEpoch, &capability.Writer.CredentialGeneration, &capability.Writer.LeaseID)
	if err != nil || !validFileReferenceCapabilityUpdate(capability.SessionID, store.FileReferenceCapabilityUpdate{EventSeq: capability.EventSeq, Fingerprint: capability.Fingerprint, Writer: capability.Writer}) {
		return store.FileReferenceCapability{}, errors.New("file-reference capability row is invalid")
	}
	return capability, nil
}

func queryFileReferenceCommand(ctx context.Context, querier fileReferenceQuerier, sessionID, commandID string) (store.FileReferenceCommand, error) {
	var command store.FileReferenceCommand
	var epoch, generation sql.NullInt64
	var lease sql.NullString
	var terminal sql.NullInt64
	var status string
	var deadlineMS int64
	err := querier.QueryRowContext(ctx, `SELECT session_id,cmd_id,message_id,capability_fingerprint,request_fingerprint,reference_count,reservation_version,delivery_deadline_ms,writer_connection_epoch,writer_credential_generation,writer_lease_id,status,terminal_event_seq FROM session_file_reference_commands WHERE session_id=? AND cmd_id=?`, sessionID, commandID).Scan(&command.SessionID, &command.CommandID, &command.MessageID, &command.CapabilityFingerprint, &command.RequestFingerprint, &command.ReferenceCount, &command.ReservationVersion, &deadlineMS, &epoch, &generation, &lease, &status, &terminal)
	if err != nil {
		return store.FileReferenceCommand{}, err
	}
	command.Status = store.FileReferenceCommandStatus(status)
	command.DeliveryDeadline = time.UnixMilli(deadlineMS)
	if epoch.Valid && generation.Valid && lease.Valid {
		command.Writer = &store.FileReferenceWriter{ConnectionEpoch: epoch.Int64, CredentialGeneration: generation.Int64, LeaseID: lease.String}
	}
	if terminal.Valid {
		value := terminal.Int64
		command.TerminalEventSeq = &value
	}
	return command, nil
}

func validFileReferenceCapabilityUpdate(sessionID string, update store.FileReferenceCapabilityUpdate) bool {
	return validConnectionID(sessionID) && update.EventSeq > 0 && validSettingsFingerprint(update.Fingerprint) && validSettingsWriter(update.Writer)
}
func validFileReferenceRequest(sessionID string, request store.FileReferenceCommandRequest) bool {
	return validConnectionID(sessionID) && request.CommandID != "" && len(request.CommandID) <= 256 && request.MessageID != "" && len(request.MessageID) <= 256 && validSettingsFingerprint(request.CapabilityFingerprint) && validSettingsFingerprint(request.RequestFingerprint) && request.ReferenceCount >= 1 && request.ReferenceCount <= 8
}

func queryRunControlCapability(ctx context.Context, querier runControlQuerier, sessionID string) (store.RunControlCapability, error) {
	var capability store.RunControlCapability
	err := querier.QueryRowContext(ctx, `SELECT session_id,capability_event_seq,capability_version,interrupt_supported,stop_supported,writer_connection_epoch,writer_credential_generation,writer_lease_id FROM session_run_control_capabilities WHERE session_id=?`, sessionID).Scan(&capability.SessionID, &capability.EventSeq, &capability.Version, &capability.InterruptSupported, &capability.StopSupported, &capability.Writer.ConnectionEpoch, &capability.Writer.CredentialGeneration, &capability.Writer.LeaseID)
	if err != nil || !validRunControlCapabilityUpdate(capability.SessionID, store.RunControlCapabilityUpdate{EventSeq: capability.EventSeq, InterruptSupported: capability.InterruptSupported, StopSupported: capability.StopSupported, Writer: capability.Writer}) || capability.Version < 1 {
		return store.RunControlCapability{}, errors.New("run-control capability row is invalid")
	}
	return capability, nil
}

func queryRunControlReservation(ctx context.Context, querier runControlQuerier, sessionID, commandID string) (store.RunControlReservation, error) {
	var reservation store.RunControlReservation
	var deadlineMS int64
	var status string
	var terminal sql.NullInt64
	err := querier.QueryRowContext(ctx, `SELECT session_id,cmd_id,operation,capability_version,reservation_version,pre_control_state,pre_control_state_seq,writer_connection_epoch,writer_credential_generation,writer_lease_id,deadline_ms,status,terminal_event_seq FROM session_run_controls WHERE session_id=? AND cmd_id=?`, sessionID, commandID).Scan(&reservation.SessionID, &reservation.CommandID, &reservation.Operation, &reservation.CapabilityVersion, &reservation.ReservationVersion, &reservation.PreControlState, &reservation.PreControlStateSeq, &reservation.Writer.ConnectionEpoch, &reservation.Writer.CredentialGeneration, &reservation.Writer.LeaseID, &deadlineMS, &status, &terminal)
	if err != nil {
		return store.RunControlReservation{}, err
	}
	reservation.Deadline, reservation.Outcome = time.UnixMilli(deadlineMS), store.RunControlOutcome(status)
	if terminal.Valid {
		value := terminal.Int64
		reservation.TerminalEventSeq = &value
	}
	if !validRunControlReservation(reservation) {
		return store.RunControlReservation{}, errors.New("run-control reservation row is invalid")
	}
	return reservation, nil
}

func verifyRunControlCapabilityEvent(ctx context.Context, tx *sql.Tx, sessionID string, eventSeq int64) error {
	var eventType string
	if eventSeq < 1 || tx.QueryRowContext(ctx, `SELECT type FROM session_events WHERE session_id=? AND seq=?`, sessionID, eventSeq).Scan(&eventType) != nil || eventType != "session.run.capabilities" {
		return errors.New("run-control capability event is not durable")
	}
	return nil
}

func validateRunControlPreState(ctx context.Context, tx *sql.Tx, sessionID string, request store.RunControlRequest) error {
	var eventType, payload string
	var latestStateSeq int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM session_events WHERE session_id=? AND type='session.state'`, sessionID).Scan(&latestStateSeq); err != nil || latestStateSeq != request.PreControlStateSeq {
		return errors.New("run-control pre-control state is stale")
	}
	if err := tx.QueryRowContext(ctx, `SELECT type,payload FROM session_events WHERE session_id=? AND seq=?`, sessionID, request.PreControlStateSeq).Scan(&eventType, &payload); err != nil || eventType != "session.state" {
		return errors.New("run-control pre-control state is not durable")
	}
	var state struct {
		State string `json:"state"`
	}
	if json.Unmarshal([]byte(payload), &state) != nil || state.State != request.PreControlState || !validRunControlState(request.Operation, state.State) {
		return errors.New("run-control pre-control state is invalid")
	}
	return nil
}

func appendRunControlEventTx(ctx context.Context, tx *sql.Tx, sessionID, eventType, payload string, nowMS int64) (int64, error) {
	var latest int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM session_events WHERE session_id=?`, sessionID).Scan(&latest); err != nil {
		return 0, err
	}
	seq := latest + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_events (session_id,seq,type,payload,event_time_ms,created_at_ms) VALUES (?,?,?,?,?,?)`, sessionID, seq, eventType, []byte(payload), nowMS, nowMS); err != nil {
		return 0, err
	}
	return seq, nil
}

func validRunControlCapabilityUpdate(sessionID string, update store.RunControlCapabilityUpdate) bool {
	return validConnectionID(sessionID) && update.EventSeq > 0 && validSettingsWriter(update.Writer)
}
func validRunControlRequest(sessionID string, request store.RunControlRequest) bool {
	return validConnectionID(sessionID) && request.CommandID != "" && len(request.CommandID) <= 256 && validRunControlOperation(request.Operation) && request.PreControlStateSeq > 0 && validSettingsWriter(request.Writer)
}
func validRunControlOperation(operation store.RunControlOperation) bool {
	return operation == store.RunControlInterrupt || operation == store.RunControlStop
}
func validRunControlState(operation store.RunControlOperation, state string) bool {
	if operation == store.RunControlInterrupt {
		return state == "busy"
	}
	return state == "starting" || state == "ready" || state == "busy" || state == "waiting_permission" || state == "recovering"
}
func validRunControlTerminalOutcome(outcome store.RunControlOutcome) bool {
	return outcome == store.RunControlCompleted || outcome == store.RunControlRejected || outcome == store.RunControlTimeout || outcome == store.RunControlUnsupported || outcome == store.RunControlOutcomeUnknown
}
func validRunControlReservation(reservation store.RunControlReservation) bool {
	terminal := reservation.Outcome != store.RunControlPending
	return validRunControlRequest(reservation.SessionID, store.RunControlRequest{CommandID: reservation.CommandID, Operation: reservation.Operation, PreControlState: reservation.PreControlState, PreControlStateSeq: reservation.PreControlStateSeq, Writer: reservation.Writer}) && reservation.CapabilityVersion > 0 && reservation.ReservationVersion > 0 && !reservation.Deadline.IsZero() && (reservation.Outcome == store.RunControlPending || validRunControlTerminalOutcome(reservation.Outcome)) && ((terminal && reservation.TerminalEventSeq != nil) || (!terminal && reservation.TerminalEventSeq == nil))
}
func validSettingsID(value string) bool {
	if len(value) < 1 || len(value) > 128 || !((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z') || (value[0] >= '0' && value[0] <= '9')) {
		return false
	}
	for _, char := range value[1:] {
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == ':' || char == '/' || char == '-') {
			return false
		}
	}
	return true
}
func validSettingsFingerprint(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[7:] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
func validSettingsStatus(status store.SettingsCommandStatus) bool {
	switch status {
	case store.SettingsCommandDeliveryPending, store.SettingsCommandPending, store.SettingsCommandRecoveryPending, store.SettingsCommandApplied, store.SettingsCommandRejected, store.SettingsCommandTimeout, store.SettingsCommandUnsupported, store.SettingsCommandStaleCapability, store.SettingsCommandOutcomeUnknown, store.SettingsCommandMismatched:
		return true
	default:
		return false
	}
}
func validSettingsTerminalStatus(status store.SettingsCommandStatus) bool {
	return validSettingsStatus(status) && !validSettingsNonterminalStatus(status)
}
func validSettingsNonterminalStatus(status store.SettingsCommandStatus) bool {
	return status == store.SettingsCommandDeliveryPending || status == store.SettingsCommandPending || status == store.SettingsCommandRecoveryPending
}
func sameSettingsOptionalID(left, right *string) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}
func sameSettingsCapability(left, right store.SettingsCapability) bool {
	return left.SessionID == right.SessionID && left.EventSeq == right.EventSeq && left.Fingerprint == right.Fingerprint && left.EffectiveModelID == right.EffectiveModelID && left.EffectivePermissionModeID == right.EffectivePermissionModeID && left.Version == right.Version && left.Writer == right.Writer
}
func validSettingsReason(reason *string) bool {
	if reason == nil || len(*reason) < 1 || len(*reason) > 64 || (*reason)[0] < 'a' || (*reason)[0] > 'z' {
		return false
	}
	for _, char := range *reason {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_') {
			return false
		}
	}
	return true
}

func validateSettingsFinalization(command store.SettingsCommand, capability store.SettingsCapability, finalize store.SettingsCommandFinalize, nowMS int64) error {
	if finalize.Outcome == store.SettingsCommandApplied {
		if finalize.ReasonCode != nil || (command.RequestedModelID != nil && *command.RequestedModelID != capability.EffectiveModelID) || (command.RequestedPermissionModeID != nil && *command.RequestedPermissionModeID != capability.EffectivePermissionModeID) || (command.RequestedModelID == nil && command.ReservedCapability.EffectiveModelID != capability.EffectiveModelID) || (command.RequestedPermissionModeID == nil && command.ReservedCapability.EffectivePermissionModeID != capability.EffectivePermissionModeID) {
			return errors.New("applied settings finalization does not match the request")
		}
	} else if !validSettingsReason(finalize.ReasonCode) {
		return errors.New("non-applied settings finalization requires a bounded reason")
	}
	switch finalize.ExpectedStatus {
	case store.SettingsCommandDeliveryPending:
		if finalize.Writer != nil || finalize.Outcome != store.SettingsCommandRejected || finalize.ReasonCode == nil || *finalize.ReasonCode != "adapter_delivery_failed" || command.DeliveryDeadline.UnixMilli() > nowMS {
			return errors.New("delivery-pending settings command may only reject")
		}
	case store.SettingsCommandPending:
		if finalize.Writer != nil && (command.OperationDeadline == nil || command.OperationDeadline.UnixMilli() <= nowMS) {
			return errors.New("settings operation deadline has elapsed")
		}
		if finalize.Writer == nil && (finalize.Outcome != store.SettingsCommandTimeout || command.OperationDeadline == nil || command.OperationDeadline.UnixMilli() > nowMS) {
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

func (s *Store) CreateAttachment(ctx context.Context, request store.AttachmentCreate) (store.AttachmentCommit, error) {
	if err := validateAttachmentIdentity(request.Identity); err != nil {
		return store.AttachmentCommit{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("begin attachment create: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	existing, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, request.Identity.AttachID))
	if err == nil {
		if existing.Identity != request.Identity {
			return store.AttachmentCommit{}, errors.New("attachment identity is immutable")
		}
		if err := tx.Commit(); err != nil {
			return store.AttachmentCommit{}, fmt.Errorf("commit attachment no-op: %w", err)
		}
		return store.AttachmentCommit{Attachment: existing, Summary: sqliteAttachmentSummary(existing, nil), Noop: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.AttachmentCommit{}, fmt.Errorf("select attachment: %w", err)
	}
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.AttachmentCommit{}, err
	}
	if !validAttachmentExpiry(request.ExpiresAt, time.UnixMilli(nowMS)) {
		return store.AttachmentCommit{}, errors.New("attachment expiry is outside the Store-clock delivery window")
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_attachments (
    attach_id, bootstrap_session_id, target_session_id, status, delivery_state,
    delivery_version, expires_at_ns, target_credential_lineage_ref, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, 'join_pending', 'pending', 0, ?, ?, ?, ?)
`, request.Identity.AttachID, request.Identity.BootstrapSessionID, request.Identity.TargetSessionID,
		request.ExpiresAt.UnixNano(), request.Identity.TargetCredentialLineageRef, nowMS, nowMS); err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("insert attachment: %w", err)
	}
	created, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, request.Identity.AttachID))
	if err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("read created attachment: %w", err)
	}
	if err := upsertSQLiteAttentionLedger(ctx, tx, created.Identity.TargetSessionID, sqliteAttentionBlockerForAttachment(created, nil), nil); err != nil {
		return store.AttachmentCommit{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.AttachmentCommit{}, fmt.Errorf("commit attachment create: %w", err)
	}
	return store.AttachmentCommit{Attachment: created, Summary: sqliteAttachmentSummary(created, nil)}, nil
}

func (s *Store) Attachment(ctx context.Context, attachID string) (store.Attachment, error) {
	attachment, err := queryAttachment(ctx, s.db.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, attachID))
	if err != nil {
		return store.Attachment{}, fmt.Errorf("select attachment: %w", err)
	}
	return attachment, nil
}

func (s *Store) AttachmentForTarget(ctx context.Context, targetSessionID string) (store.Attachment, error) {
	attachment, err := queryAttachment(ctx, s.db.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE target_session_id = ?`, targetSessionID))
	if err != nil {
		return store.Attachment{}, fmt.Errorf("select target attachment: %w", err)
	}
	return attachment, nil
}

func (s *Store) UpdateAttachment(ctx context.Context, attachID string, expectedVersion int64, update store.AttachmentUpdate) (store.AttachmentMutation, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("begin attachment update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, attachID))
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("select attachment for update: %w", err)
	}
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return store.AttachmentMutation{}, err
	}
	if current.DeliveryVersion != expectedVersion {
		return store.AttachmentMutation{}, errors.New("stale attachment version")
	}
	if err := validateAttachmentUpdate(current, update, time.UnixMilli(nowMS)); err != nil {
		return store.AttachmentMutation{}, err
	}
	result, err := executeAttachmentUpdate(ctx, tx, `
UPDATE session_attachments SET
    status = ?, delivery_state = ?, delivery_version = delivery_version + 1,
    queue_reason = ?, expires_at_ns = ?, canceled_at_ms = CASE WHEN ? = 'canceled' THEN ? ELSE NULL END,
    blocking_session_id = ?, updated_at_ms = ?
WHERE attach_id = ? AND delivery_version = ?
  AND (? <> 'start_received' OR status = 'start_received'
       OR (expires_at_ns IS NOT NULL AND expires_at_ns >
           CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) * 1000000))
`, update.Status, update.DeliveryState, nullableString(update.QueueReason), nullableTimeNano(update.ExpiresAt),
		update.Status, nowMS, nullableString(update.BlockingSessionID), nowMS, attachID, expectedVersion, update.Status)
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("update attachment: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return store.AttachmentMutation{}, errors.New("attachment version conflict")
	}
	updated, err := queryAttachment(ctx, tx.QueryRowContext(ctx, `SELECT `+attachmentColumns+` FROM session_attachments WHERE attach_id = ?`, attachID))
	if err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("read updated attachment: %w", err)
	}
	if err := upsertSQLiteAttentionLedger(ctx, tx, updated.Identity.TargetSessionID, sqliteAttentionBlockerForAttachment(updated, update.Blocker), nil); err != nil {
		return store.AttachmentMutation{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.AttachmentMutation{}, fmt.Errorf("commit attachment update: %w", err)
	}
	return store.AttachmentMutation{Attachment: updated, Summary: sqliteAttachmentSummary(updated, update.Blocker)}, nil
}

func executeAttachmentUpdate(ctx context.Context, tx *sql.Tx, statement string, args ...any) (sql.Result, error) {
	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		result, err := tx.ExecContext(retryCtx, statement, args...)
		if err == nil {
			return result, nil
		}
		if retryCtx.Err() != nil {
			return nil, retryCtx.Err()
		}
		var sqliteError *sqliteDriver.Error
		if !errors.As(err, &sqliteError) || sqliteError.Code() != sqlite3.SQLITE_BUSY {
			return nil, err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-retryCtx.Done():
			timer.Stop()
			return nil, retryCtx.Err()
		case <-timer.C:
		}
	}
}

const attachmentColumns = `
attach_id, bootstrap_session_id, target_session_id, status, delivery_state, delivery_version,
queue_reason, expires_at_ns, canceled_at_ms, blocking_session_id, target_credential_lineage_ref,
created_at_ms, updated_at_ms, CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`

type attachmentRow interface {
	Scan(...any) error
}

func queryAttachment(_ context.Context, row attachmentRow) (store.Attachment, error) {
	var attachment store.Attachment
	var status, deliveryState string
	var queueReason, blockingSessionID sql.NullString
	var expiresAtNS, canceledAtMS sql.NullInt64
	var createdAtMS, updatedAtMS, nowMS int64
	err := row.Scan(
		&attachment.Identity.AttachID, &attachment.Identity.BootstrapSessionID, &attachment.Identity.TargetSessionID,
		&status, &deliveryState, &attachment.DeliveryVersion, &queueReason, &expiresAtNS, &canceledAtMS,
		&blockingSessionID, &attachment.Identity.TargetCredentialLineageRef, &createdAtMS, &updatedAtMS, &nowMS,
	)
	if err != nil {
		return store.Attachment{}, err
	}
	attachment.Status = store.AttachmentStatus(status)
	attachment.DeliveryState = store.AttachmentDeliveryState(deliveryState)
	attachment.QueueReason = nullStringPointer(queueReason)
	attachment.ExpiresAt = nullNanoTimePointer(expiresAtNS)
	attachment.CanceledAt = nullMilliTimePointer(canceledAtMS)
	attachment.BlockingSessionID = nullStringPointer(blockingSessionID)
	if validateAttachmentIdentity(attachment.Identity) != nil || attachment.DeliveryVersion < 0 ||
		createdAtMS < 1 || createdAtMS > nowMS || updatedAtMS < createdAtMS || updatedAtMS > nowMS ||
		!validAttachmentRowShape(attachment, createdAtMS, nowMS) {
		return store.Attachment{}, errors.New("attachment row is invalid")
	}
	return attachment, nil
}

func validateAttachmentIdentity(identity store.AttachmentIdentity) error {
	if !validAttachmentText(identity.AttachID, 255) || !validAttachmentText(identity.BootstrapSessionID, 255) ||
		!validAttachmentText(identity.TargetSessionID, 255) || !validAttachmentText(identity.TargetCredentialLineageRef, 255) ||
		identity.BootstrapSessionID == identity.TargetSessionID {
		return errors.New("attachment identity is invalid")
	}
	return nil
}

func validateAttachmentUpdate(current store.Attachment, update store.AttachmentUpdate, storeNow time.Time) error {
	if current.Status == store.AttachmentCanceled || (current.Status == store.AttachmentStartReceived && update.Status != store.AttachmentStartReceived) {
		return errors.New("terminal attachment status cannot be reopened")
	}
	if update.Status == store.AttachmentStartReceived && current.Status != store.AttachmentStartReceived &&
		(current.ExpiresAt == nil || !current.ExpiresAt.After(storeNow)) {
		return errors.New("expired attachment cannot record start receipt")
	}
	if update.ExpiresAt != nil && (!validAttachmentExpiry(*update.ExpiresAt, storeNow) ||
		(current.ExpiresAt != nil && update.ExpiresAt.After(*current.ExpiresAt))) {
		return errors.New("attachment expiry is invalid")
	}
	matches := func(kind store.AttachmentBlockerKind, reason, expiry, blocking, operation bool) bool {
		blocker := update.Blocker
		if blocker == nil || blocker.Kind != kind || (!reason && blocker.Reason != nil) || (!expiry && blocker.ExpiresAt != nil) ||
			(!blocking && blocker.BlockingSessionID != nil) || (!operation && blocker.Operation != nil) {
			return false
		}
		return (!reason || blocker.Reason != nil && update.QueueReason != nil && *blocker.Reason == *update.QueueReason) &&
			(!expiry || blocker.ExpiresAt != nil && update.ExpiresAt != nil && blocker.ExpiresAt.Equal(*update.ExpiresAt)) &&
			(!blocking || blocker.BlockingSessionID != nil && update.BlockingSessionID != nil && *blocker.BlockingSessionID == *update.BlockingSessionID)
	}
	valid := false
	switch update.Status {
	case store.AttachmentJoinPending:
		valid = current.Status == store.AttachmentJoinPending && update.QueueReason == nil && update.ExpiresAt != nil && current.ExpiresAt != nil && update.ExpiresAt.Equal(*current.ExpiresAt) && update.BlockingSessionID == nil && update.Blocker == nil && (update.DeliveryState == store.AttachmentDeliveryPending || update.DeliveryState == store.AttachmentDeliveryReceived || update.DeliveryState == store.AttachmentDeliveryCompleted)
	case store.AttachmentQueued:
		valid = update.DeliveryState == store.AttachmentDeliveryPending && update.QueueReason != nil && validAttachmentText(*update.QueueReason, 128) &&
			update.ExpiresAt != nil && update.BlockingSessionID != nil && validAttachmentText(*update.BlockingSessionID, 255) &&
			*update.BlockingSessionID != current.Identity.TargetSessionID && matches(store.AttachmentBlockerQueued, true, true, true, false)
	case store.AttachmentStartReceived:
		if update.QueueReason == nil && update.ExpiresAt == nil && update.BlockingSessionID == nil {
			valid = update.DeliveryState == store.AttachmentDeliveryOutcomeUnknown && matches(store.AttachmentBlockerOutcomeUnknown, false, false, false, true) && validAttachmentOperation(update.Blocker.Operation) ||
				(update.DeliveryState == store.AttachmentDeliveryReceived || update.DeliveryState == store.AttachmentDeliveryCompleted) && update.Blocker == nil
		}
	case store.AttachmentReauthorizationRequired:
		if update.QueueReason == nil && update.ExpiresAt == nil && update.BlockingSessionID == nil {
			valid = update.DeliveryState == store.AttachmentDeliveryOutcomeUnknown && matches(store.AttachmentBlockerOutcomeUnknown, false, false, false, true) && validAttachmentOperation(update.Blocker.Operation) ||
				update.DeliveryState == store.AttachmentDeliveryPending && matches(store.AttachmentBlockerReauthorizationRequired, false, false, false, false)
		}
	case store.AttachmentCanceled:
		valid = update.DeliveryState == store.AttachmentDeliveryPending && update.QueueReason == nil && update.ExpiresAt == nil &&
			update.BlockingSessionID == nil && matches(store.AttachmentBlockerNewRunRequired, false, false, false, false)
	}
	if !valid {
		return errors.New("invalid attachment update")
	}
	return nil
}

func validAttachmentRowShape(attachment store.Attachment, createdAtMS, nowMS int64) bool {
	if attachment.QueueReason != nil && !validAttachmentText(*attachment.QueueReason, 128) ||
		attachment.BlockingSessionID != nil && !validAttachmentText(*attachment.BlockingSessionID, 255) ||
		attachment.ExpiresAt != nil && (attachment.ExpiresAt.UnixNano() <= createdAtMS*int64(time.Millisecond) ||
			attachment.ExpiresAt.UnixNano() > (createdAtMS+30000)*int64(time.Millisecond)) ||
		attachment.CanceledAt != nil && (attachment.CanceledAt.UnixMilli() < createdAtMS || attachment.CanceledAt.UnixMilli() > nowMS) {
		return false
	}
	switch attachment.Status {
	case store.AttachmentJoinPending:
		return (attachment.DeliveryState == store.AttachmentDeliveryPending || attachment.DeliveryState == store.AttachmentDeliveryReceived || attachment.DeliveryState == store.AttachmentDeliveryCompleted) && attachment.QueueReason == nil && attachment.ExpiresAt != nil && attachment.CanceledAt == nil && attachment.BlockingSessionID == nil
	case store.AttachmentQueued:
		return attachment.DeliveryState == store.AttachmentDeliveryPending && attachment.QueueReason != nil && attachment.ExpiresAt != nil && attachment.CanceledAt == nil && attachment.BlockingSessionID != nil
	case store.AttachmentStartReceived:
		return (attachment.DeliveryState == store.AttachmentDeliveryReceived || attachment.DeliveryState == store.AttachmentDeliveryCompleted || attachment.DeliveryState == store.AttachmentDeliveryOutcomeUnknown) && attachment.QueueReason == nil && attachment.ExpiresAt == nil && attachment.CanceledAt == nil && attachment.BlockingSessionID == nil
	case store.AttachmentReauthorizationRequired:
		return (attachment.DeliveryState == store.AttachmentDeliveryPending || attachment.DeliveryState == store.AttachmentDeliveryOutcomeUnknown) && attachment.QueueReason == nil && attachment.ExpiresAt == nil && attachment.CanceledAt == nil && attachment.BlockingSessionID == nil
	case store.AttachmentCanceled:
		return attachment.DeliveryState == store.AttachmentDeliveryPending && attachment.QueueReason == nil && attachment.ExpiresAt == nil && attachment.CanceledAt != nil && attachment.BlockingSessionID == nil
	default:
		return false
	}
}

func validAttachmentExpiry(expiresAt, storeNow time.Time) bool {
	return expiresAt.After(storeNow) && !expiresAt.After(storeNow.Add(30*time.Second))
}

func validAttachmentText(value string, limit int) bool { return len(value) > 0 && len(value) <= limit }

func validAttachmentOperation(value *string) bool {
	return value != nil && (*value == "start" || *value == "command" || *value == "credential_handoff")
}

func sqliteAttachmentSummary(attachment store.Attachment, blocker *store.AttachmentBlocker) store.AttachmentSummary {
	return store.AttachmentSummary{AttachID: attachment.Identity.AttachID, TargetSessionID: attachment.Identity.TargetSessionID,
		DeliveryVersion: attachment.DeliveryVersion, ExpiresAt: cloneAttachmentTime(attachment.ExpiresAt), Blocker: cloneSQLiteAttachmentBlocker(blocker)}
}

func cloneAttachmentTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneSQLiteAttachmentBlocker(value *store.AttachmentBlocker) *store.AttachmentBlocker {
	if value == nil {
		return nil
	}
	copy := *value
	copy.Reason = cloneAttachmentString(value.Reason)
	copy.ExpiresAt = cloneAttachmentTime(value.ExpiresAt)
	copy.BlockingSessionID = cloneAttachmentString(value.BlockingSessionID)
	copy.Operation = cloneAttachmentString(value.Operation)
	return &copy
}

func cloneAttachmentString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func sqliteAttentionBlockerForAttachment(attachment store.Attachment, explicit *store.AttachmentBlocker) *store.AttentionBlocker {
	if explicit != nil {
		return &store.AttentionBlocker{Kind: string(explicit.Kind), Reason: cloneAttachmentString(explicit.Reason),
			ExpiresAt: cloneAttachmentTime(explicit.ExpiresAt), BlockingSessionID: cloneAttachmentString(explicit.BlockingSessionID), Operation: cloneAttachmentString(explicit.Operation)}
	}
	if attachment.Status != store.AttachmentJoinPending {
		return nil
	}
	reason := "join_pending"
	return &store.AttentionBlocker{Kind: store.AttentionBlockerQueued, Reason: &reason, ExpiresAt: cloneAttachmentTime(attachment.ExpiresAt)}
}

const sqliteAttentionSummaryColumns = `
session_id, latest_seq, state, permission_id, permission_status, terminal_outcome, latest_change_seq,
blocker_kind, blocker_reason, blocker_expires_at_ns, blocking_session_id, blocker_operation,
summary_version, last_durable_event_at_ms, last_client_command_at_ms, projection_state, created_at_ms, updated_at_ms`

type sqliteAttentionSummaryRow interface{ Scan(...any) error }

func querySQLiteAttentionSummary(_ context.Context, row sqliteAttentionSummaryRow) (store.SessionAttentionSummary, error) {
	var summary store.SessionAttentionSummary
	var permissionID, permissionStatus, terminalOutcome sql.NullString
	var latestChangeSeq sql.NullInt64
	var blockerKind, blockerReason, blockingSessionID, blockerOperation sql.NullString
	var blockerExpiresAtNS, lastDurableAtMS, lastClientAtMS sql.NullInt64
	var createdAtMS, updatedAtMS int64
	if err := row.Scan(&summary.SessionID, &summary.LatestSeq, &summary.State, &permissionID, &permissionStatus, &terminalOutcome, &latestChangeSeq,
		&blockerKind, &blockerReason, &blockerExpiresAtNS, &blockingSessionID, &blockerOperation,
		&summary.SummaryVersion, &lastDurableAtMS, &lastClientAtMS, &summary.StateOfProjection, &createdAtMS, &updatedAtMS); err != nil {
		return store.SessionAttentionSummary{}, err
	}
	if !validConnectionID(summary.SessionID) || summary.LatestSeq < 0 || summary.SummaryVersion < 0 ||
		(summary.StateOfProjection != store.AttentionProjectionComplete && summary.StateOfProjection != store.AttentionProjectionIncomplete) ||
		createdAtMS < 1 || updatedAtMS < createdAtMS {
		return store.SessionAttentionSummary{}, errors.New("attention summary row is invalid")
	}
	if permissionID.Valid != permissionStatus.Valid || (permissionID.Valid && (permissionStatus.String != store.AttentionPermissionPending || !validConnectionID(permissionID.String))) {
		return store.SessionAttentionSummary{}, errors.New("attention permission row is invalid")
	}
	if permissionID.Valid {
		summary.Permission = &store.AttentionPermission{ID: permissionID.String, Status: permissionStatus.String}
	}
	summary.TerminalOutcome = nullStringPointer(terminalOutcome)
	summary.LatestChangeSeq = nullInt64Pointer(latestChangeSeq)
	if blockerKind.Valid {
		summary.Blocker = &store.AttentionBlocker{Kind: blockerKind.String, Reason: nullStringPointer(blockerReason),
			ExpiresAt: nullNanoTimePointer(blockerExpiresAtNS), BlockingSessionID: nullStringPointer(blockingSessionID), Operation: nullStringPointer(blockerOperation)}
	} else if blockerReason.Valid || blockerExpiresAtNS.Valid || blockingSessionID.Valid || blockerOperation.Valid {
		return store.SessionAttentionSummary{}, errors.New("attention blocker row is invalid")
	}
	summary.LastDurableEventAt = nullMilliTimePointer(lastDurableAtMS)
	summary.LastClientCommandAt = nullMilliTimePointer(lastClientAtMS)
	return summary, nil
}
func upsertSQLiteAttentionSummary(ctx context.Context, tx *sql.Tx, summary store.SessionAttentionSummary, nowMS int64) error {
	if !validConnectionID(summary.SessionID) || summary.LatestSeq < 0 || summary.SummaryVersion < 0 ||
		(summary.StateOfProjection != store.AttentionProjectionComplete && summary.StateOfProjection != store.AttentionProjectionIncomplete) {
		return errors.New("invalid attention summary")
	}
	var permissionID, permissionStatus any
	if summary.Permission != nil {
		permissionID, permissionStatus = summary.Permission.ID, summary.Permission.Status
	}
	var blockerKind, blockerReason, blockerExpiry, blockingSession, blockerOperation any
	if summary.Blocker != nil {
		blockerKind, blockerReason, blockerExpiry = summary.Blocker.Kind, nullableString(summary.Blocker.Reason), nullableTimeNano(summary.Blocker.ExpiresAt)
		blockingSession, blockerOperation = nullableString(summary.Blocker.BlockingSessionID), nullableString(summary.Blocker.Operation)
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO session_attention_summaries (
    session_id, latest_seq, state, permission_id, permission_status, terminal_outcome, latest_change_seq,
    blocker_kind, blocker_reason, blocker_expires_at_ns, blocking_session_id, blocker_operation,
    summary_version, last_durable_event_at_ms, last_client_command_at_ms, projection_state, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (session_id) DO UPDATE SET
    latest_seq = excluded.latest_seq, state = excluded.state, permission_id = excluded.permission_id,
    permission_status = excluded.permission_status, terminal_outcome = excluded.terminal_outcome,
    latest_change_seq = excluded.latest_change_seq, blocker_kind = excluded.blocker_kind,
    blocker_reason = excluded.blocker_reason, blocker_expires_at_ns = excluded.blocker_expires_at_ns,
    blocking_session_id = excluded.blocking_session_id, blocker_operation = excluded.blocker_operation,
    summary_version = excluded.summary_version, last_durable_event_at_ms = excluded.last_durable_event_at_ms,
    last_client_command_at_ms = excluded.last_client_command_at_ms, projection_state = excluded.projection_state,
    updated_at_ms = excluded.updated_at_ms
`, summary.SessionID, summary.LatestSeq, summary.State, permissionID, permissionStatus, nullableString(summary.TerminalOutcome), nullableInt64Pointer(summary.LatestChangeSeq),
		blockerKind, blockerReason, blockerExpiry, blockingSession, blockerOperation, summary.SummaryVersion,
		nullableMilliTime(summary.LastDurableEventAt), nullableMilliTime(summary.LastClientCommandAt), summary.StateOfProjection, nowMS, nowMS)
	if err != nil {
		return fmt.Errorf("upsert attention summary: %w", err)
	}
	return nil
}
func upsertSQLiteAttentionLedger(ctx context.Context, tx *sql.Tx, sessionID string, blocker *store.AttentionBlocker, clientAtMS *int64) error {
	summary, err := querySQLiteAttentionSummary(ctx, tx.QueryRowContext(ctx, `SELECT `+sqliteAttentionSummaryColumns+` FROM session_attention_summaries WHERE session_id = ?`, sessionID))
	if errors.Is(err, sql.ErrNoRows) {
		summary = store.SessionAttentionSummary{SessionID: sessionID, State: "starting", SummaryVersion: 1, StateOfProjection: store.AttentionProjectionIncomplete}
	} else if err != nil {
		return fmt.Errorf("load attention ledger: %w", err)
	} else {
		summary.SummaryVersion++
	}
	summary.Blocker = cloneSQLiteAttentionBlocker(blocker)
	if clientAtMS != nil {
		activity := time.UnixMilli(*clientAtMS)
		summary.LastClientCommandAt = &activity
	}
	nowMS, err := sqliteNowMillis(ctx, tx)
	if err != nil {
		return err
	}
	return upsertSQLiteAttentionSummary(ctx, tx, summary, nowMS)
}

func sqlitePlaceholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func cloneSQLiteAttentionBlocker(value *store.AttentionBlocker) *store.AttentionBlocker {
	if value == nil {
		return nil
	}
	copy := *value
	copy.Reason = cloneAttachmentString(value.Reason)
	copy.ExpiresAt = cloneAttachmentTime(value.ExpiresAt)
	copy.BlockingSessionID = cloneAttachmentString(value.BlockingSessionID)
	copy.Operation = cloneAttachmentString(value.Operation)
	return &copy
}

func nullableInt64Pointer(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableMilliTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UnixMilli()
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableTimeNano(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UnixNano()
}

func nullStringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	copy := value.String
	return &copy
}

func nullNanoTimePointer(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	copy := time.Unix(0, value.Int64)
	return &copy
}

func nullMilliTimePointer(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	copy := time.UnixMilli(value.Int64)
	return &copy
}

func (s *Store) init(ctx context.Context) error {
	if _, err := s.fenceDB.ExecContext(ctx, `PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000`); err != nil {
		return fmt.Errorf("configure sqlite fence store: %w", err)
	}
	pragmas := []string{
		`PRAGMA main.journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA foreign_keys = ON`,
	}
	for _, pragma := range pragmas {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure sqlite event store %q: %w", pragma, err)
		}
	}
	var fenceTables int
	if err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('session_adapter_fence_allocator', 'session_adapter_fence_identity')) + (SELECT count(*) FROM fence_store.sqlite_master WHERE type = 'table' AND name IN ('adapter_fence_allocator', 'adapter_fence_identity'))`).Scan(&fenceTables); err != nil || (fenceTables != 0 && fenceTables != 4) {
		return errors.New("sqlite fence store schema is incomplete")
	}
	var legacyStore bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'session_events')`).Scan(&legacyStore); err != nil {
		return fmt.Errorf("inspect sqlite event schema: %w", err)
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

CREATE TABLE IF NOT EXISTS session_attention_summaries (session_id TEXT PRIMARY KEY CHECK (length(session_id) BETWEEN 1 AND 255), latest_seq INTEGER NOT NULL DEFAULT 0 CHECK (latest_seq >= 0),
    state TEXT NOT NULL CHECK (state IN ('starting', 'ready', 'busy', 'waiting_permission', 'recovering', 'ended', 'error')), permission_id TEXT, permission_status TEXT, terminal_outcome TEXT, latest_change_seq INTEGER, blocker_kind TEXT, blocker_reason TEXT, blocker_expires_at_ns INTEGER, blocking_session_id TEXT, blocker_operation TEXT, summary_version INTEGER NOT NULL DEFAULT 0 CHECK (summary_version >= 0),
    last_durable_event_at_ms INTEGER, last_client_command_at_ms INTEGER, projection_state TEXT NOT NULL CHECK (projection_state IN ('complete', 'incomplete')), created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, CHECK (latest_change_seq IS NULL OR (latest_change_seq > 0 AND latest_change_seq <= latest_seq)),
    CHECK ((permission_id IS NULL AND permission_status IS NULL) OR (length(permission_id) BETWEEN 1 AND 255 AND permission_status = 'pending')), CHECK (terminal_outcome IS NULL OR length(terminal_outcome) BETWEEN 1 AND 128), CHECK (blocking_session_id IS NULL OR length(blocking_session_id) BETWEEN 1 AND 255),
    CHECK (blocker_reason IS NULL OR length(blocker_reason) BETWEEN 1 AND 128), CHECK (blocker_operation IS NULL OR length(blocker_operation) BETWEEN 1 AND 128),
    CHECK ((blocker_kind IS NULL AND blocker_reason IS NULL AND blocker_expires_at_ns IS NULL AND blocking_session_id IS NULL AND blocker_operation IS NULL)
        OR (blocker_kind = 'queued' AND blocker_operation IS NULL)
        OR (blocker_kind = 'outcome_unknown' AND blocker_reason IS NULL AND blocker_expires_at_ns IS NULL AND blocking_session_id IS NULL)
        OR (blocker_kind IN ('reauthorization_required', 'new_run_required') AND blocker_reason IS NULL AND blocker_expires_at_ns IS NULL AND blocking_session_id IS NULL AND blocker_operation IS NULL))
);
CREATE INDEX IF NOT EXISTS session_attention_summaries_projection_state_session_idx ON session_attention_summaries (projection_state, session_id);

CREATE TABLE IF NOT EXISTS session_attention_migration (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1), state TEXT NOT NULL CHECK (state IN ('pending', 'complete')),
    checkpoint_session_id TEXT NOT NULL DEFAULT '' CHECK (length(checkpoint_session_id) <= 255)
);

CREATE TABLE IF NOT EXISTS session_adapter_connections (
    session_id TEXT PRIMARY KEY CHECK (length(session_id) BETWEEN 1 AND 255),
    connection_epoch INTEGER NOT NULL DEFAULT 0 CHECK (connection_epoch >= 0),
    accepted_fence INTEGER NOT NULL DEFAULT 0 CHECK (accepted_fence >= 0),
    active_credential_generation INTEGER NOT NULL CHECK (active_credential_generation > 0),
    credential_generation_high_watermark INTEGER NOT NULL CHECK (credential_generation_high_watermark > 0),
    active_credential_expires_at_ms INTEGER NOT NULL,
    pending_credential_generation INTEGER CHECK (pending_credential_generation IS NULL OR pending_credential_generation > 0),
    pending_credential_expires_at_ms INTEGER,
    prior_recovery_credential_generation INTEGER CHECK (prior_recovery_credential_generation IS NULL OR prior_recovery_credential_generation > 0),
    rotation_id TEXT CHECK (rotation_id IS NULL OR length(rotation_id) BETWEEN 1 AND 255),
    revoked_at_ms INTEGER,
    terminal_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (active_credential_expires_at_ms > created_at_ms),
    CHECK ((pending_credential_generation IS NULL AND pending_credential_expires_at_ms IS NULL AND rotation_id IS NULL)
        OR (pending_credential_generation IS NOT NULL AND pending_credential_expires_at_ms IS NOT NULL AND rotation_id IS NOT NULL)),
    CHECK (active_credential_generation <= credential_generation_high_watermark),
    CHECK (pending_credential_generation IS NULL OR pending_credential_generation <= credential_generation_high_watermark),
    CHECK (prior_recovery_credential_generation IS NULL OR prior_recovery_credential_generation <= credential_generation_high_watermark),
    CHECK (pending_credential_generation IS NULL OR pending_credential_generation <> active_credential_generation),
    CHECK (prior_recovery_credential_generation IS NULL OR prior_recovery_credential_generation <> active_credential_generation),
    CHECK (pending_credential_generation IS NULL OR prior_recovery_credential_generation IS NULL OR pending_credential_generation <> prior_recovery_credential_generation),
    CHECK (revoked_at_ms IS NULL OR revoked_at_ms >= created_at_ms),
    CHECK (terminal_at_ms IS NULL OR terminal_at_ms >= created_at_ms)
);

CREATE INDEX IF NOT EXISTS session_adapter_connections_active_expiry_idx
ON session_adapter_connections (active_credential_expires_at_ms);
CREATE TABLE IF NOT EXISTS session_settings_live_writers (
    session_id TEXT PRIMARY KEY CHECK (length(session_id) BETWEEN 1 AND 255),
    connection_epoch INTEGER NOT NULL CHECK (connection_epoch > 0),
    credential_generation INTEGER NOT NULL CHECK (credential_generation > 0),
    writer_lease_id TEXT NOT NULL CHECK (length(writer_lease_id) BETWEEN 1 AND 255)
);
CREATE TABLE IF NOT EXISTS session_pending_commands (
    session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
    cmd_id TEXT NOT NULL CHECK (length(cmd_id) BETWEEN 1 AND 256),
    type TEXT NOT NULL CHECK (type = 'session.send'),
    event_seq INTEGER NOT NULL CHECK (event_seq > 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'received', 'completed', 'outcome_unknown')),
    expires_at_ns INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, cmd_id),
    FOREIGN KEY (session_id, event_seq) REFERENCES session_events(session_id, seq),
    CHECK (expires_at_ns > created_at_ms * 1000000),
    CHECK (expires_at_ns <= (created_at_ms + 30000) * 1000000)
);

CREATE INDEX IF NOT EXISTS session_pending_commands_status_expiry_idx
ON session_pending_commands (status, expires_at_ns);

CREATE TABLE IF NOT EXISTS session_settings_capabilities (
    session_id TEXT PRIMARY KEY CHECK (length(session_id) BETWEEN 1 AND 255),
    capability_event_seq INTEGER NOT NULL,
    fingerprint TEXT NOT NULL CHECK (length(fingerprint) = 71 AND substr(fingerprint, 1, 7) = 'sha256:'),
    effective_model_id TEXT NOT NULL CHECK (length(effective_model_id) BETWEEN 1 AND 128),
    effective_permission_mode_id TEXT NOT NULL CHECK (length(effective_permission_mode_id) BETWEEN 1 AND 128),
    capability_version INTEGER NOT NULL CHECK (capability_version > 0),
    writer_connection_epoch INTEGER NOT NULL CHECK (writer_connection_epoch > 0),
    writer_credential_generation INTEGER NOT NULL CHECK (writer_credential_generation > 0),
    writer_lease_id TEXT NOT NULL CHECK (length(writer_lease_id) BETWEEN 1 AND 255),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (session_id, capability_event_seq) REFERENCES session_events(session_id, seq)
);

CREATE TABLE IF NOT EXISTS session_settings_commands (
    session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
    cmd_id TEXT NOT NULL CHECK (length(cmd_id) BETWEEN 1 AND 256),
    request_fingerprint TEXT NOT NULL CHECK (length(request_fingerprint) = 71 AND substr(request_fingerprint, 1, 7) = 'sha256:'),
    requested_model_id TEXT CHECK (requested_model_id IS NULL OR length(requested_model_id) BETWEEN 1 AND 128),
    requested_permission_mode_id TEXT CHECK (requested_permission_mode_id IS NULL OR length(requested_permission_mode_id) BETWEEN 1 AND 128),
    reservation_version INTEGER NOT NULL CHECK (reservation_version > 0),
    delivery_deadline_ms INTEGER NOT NULL,
    operation_deadline_ms INTEGER,
    writer_connection_epoch INTEGER NOT NULL CHECK (writer_connection_epoch > 0),
    writer_credential_generation INTEGER NOT NULL CHECK (writer_credential_generation > 0),
    writer_lease_id TEXT NOT NULL CHECK (length(writer_lease_id) BETWEEN 1 AND 255),
    reserved_capability_event_seq INTEGER NOT NULL,
    reserved_fingerprint TEXT NOT NULL CHECK (length(reserved_fingerprint) = 71 AND substr(reserved_fingerprint, 1, 7) = 'sha256:'),
    reserved_effective_model_id TEXT NOT NULL CHECK (length(reserved_effective_model_id) BETWEEN 1 AND 128),
    reserved_effective_permission_mode_id TEXT NOT NULL CHECK (length(reserved_effective_permission_mode_id) BETWEEN 1 AND 128),
    status TEXT NOT NULL CHECK (status IN ('delivery_pending', 'pending', 'recovery_pending', 'applied', 'rejected', 'timeout', 'unsupported', 'stale_capability', 'outcome_unknown', 'mismatched_effective')),
    terminal_event_seq INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, cmd_id),
    FOREIGN KEY (session_id, terminal_event_seq) REFERENCES session_events(session_id, seq),
    FOREIGN KEY (session_id, reserved_capability_event_seq) REFERENCES session_events(session_id, seq),
    CHECK (requested_model_id IS NOT NULL OR requested_permission_mode_id IS NOT NULL),
    CHECK ((status = 'delivery_pending' AND operation_deadline_ms IS NULL AND terminal_event_seq IS NULL)
        OR (status IN ('pending', 'recovery_pending') AND operation_deadline_ms IS NOT NULL AND terminal_event_seq IS NULL)
        OR (status IN ('applied', 'rejected', 'timeout', 'unsupported', 'stale_capability', 'outcome_unknown', 'mismatched_effective') AND terminal_event_seq IS NOT NULL)),
    CHECK (delivery_deadline_ms > created_at_ms AND delivery_deadline_ms <= created_at_ms + 5000),
    CHECK (operation_deadline_ms IS NULL OR (operation_deadline_ms > created_at_ms AND operation_deadline_ms <= created_at_ms + 35000))
);

CREATE UNIQUE INDEX IF NOT EXISTS session_settings_commands_one_nonterminal_idx
ON session_settings_commands (session_id)
WHERE status IN ('delivery_pending', 'pending', 'recovery_pending');

CREATE TABLE IF NOT EXISTS session_run_control_capabilities (
    session_id TEXT PRIMARY KEY CHECK (length(session_id) BETWEEN 1 AND 255),
    capability_event_seq INTEGER NOT NULL,
    capability_version INTEGER NOT NULL CHECK (capability_version > 0),
    interrupt_supported INTEGER NOT NULL CHECK (interrupt_supported IN (0,1)),
    stop_supported INTEGER NOT NULL CHECK (stop_supported IN (0,1)),
    writer_connection_epoch INTEGER NOT NULL CHECK (writer_connection_epoch > 0),
    writer_credential_generation INTEGER NOT NULL CHECK (writer_credential_generation > 0),
    writer_lease_id TEXT NOT NULL CHECK (length(writer_lease_id) BETWEEN 1 AND 255),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (session_id, capability_event_seq) REFERENCES session_events(session_id, seq)
);
CREATE TABLE IF NOT EXISTS session_run_controls (
    session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
    cmd_id TEXT NOT NULL CHECK (length(cmd_id) BETWEEN 1 AND 256),
    operation TEXT NOT NULL CHECK (operation IN ('interrupt','stop')),
    capability_version INTEGER NOT NULL CHECK (capability_version > 0),
    reservation_version INTEGER NOT NULL CHECK (reservation_version > 0),
    pre_control_state TEXT NOT NULL CHECK (pre_control_state IN ('starting','ready','busy','waiting_permission','recovering')),
    pre_control_state_seq INTEGER NOT NULL CHECK (pre_control_state_seq > 0),
    writer_connection_epoch INTEGER NOT NULL CHECK (writer_connection_epoch > 0),
    writer_credential_generation INTEGER NOT NULL CHECK (writer_credential_generation > 0),
    writer_lease_id TEXT NOT NULL CHECK (length(writer_lease_id) BETWEEN 1 AND 255),
    deadline_ms INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','completed','rejected','timeout','unsupported','outcome_unknown')),
    terminal_event_seq INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id,cmd_id),
    FOREIGN KEY (session_id,terminal_event_seq) REFERENCES session_events(session_id,seq),
    CHECK (deadline_ms > created_at_ms AND deadline_ms <= created_at_ms + 30000),
    CHECK ((operation = 'interrupt' AND pre_control_state = 'busy') OR (operation = 'stop' AND pre_control_state IN ('starting','ready','busy','waiting_permission','recovering'))),
    CHECK ((status = 'pending' AND terminal_event_seq IS NULL) OR (status <> 'pending' AND terminal_event_seq IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS session_run_controls_one_pending_idx ON session_run_controls (session_id) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS session_file_reference_capabilities (
    session_id TEXT PRIMARY KEY CHECK (length(session_id) BETWEEN 1 AND 255),
    capability_event_seq INTEGER NOT NULL,
    capability_fingerprint TEXT NOT NULL CHECK (length(capability_fingerprint) = 71),
    writer_connection_epoch INTEGER NOT NULL CHECK (writer_connection_epoch > 0),
    writer_credential_generation INTEGER NOT NULL CHECK (writer_credential_generation > 0),
    writer_lease_id TEXT NOT NULL CHECK (length(writer_lease_id) BETWEEN 1 AND 255),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (session_id, capability_event_seq) REFERENCES session_events(session_id, seq)
);
CREATE TABLE IF NOT EXISTS session_file_reference_commands (
    session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
    cmd_id TEXT NOT NULL CHECK (length(cmd_id) BETWEEN 1 AND 256),
    message_id TEXT NOT NULL CHECK (length(message_id) BETWEEN 1 AND 256),
    capability_fingerprint TEXT NOT NULL CHECK (length(capability_fingerprint) = 71),
    request_fingerprint TEXT NOT NULL CHECK (length(request_fingerprint) = 71),
    reference_count INTEGER NOT NULL CHECK (reference_count BETWEEN 1 AND 8),
    reservation_version INTEGER NOT NULL CHECK (reservation_version > 0),
    delivery_deadline_ms INTEGER NOT NULL,
    writer_connection_epoch INTEGER,
    writer_credential_generation INTEGER,
    writer_lease_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('delivery_pending', 'pending', 'delivered', 'rejected', 'outcome_unknown')),
    terminal_event_seq INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, cmd_id),
    FOREIGN KEY (session_id, terminal_event_seq) REFERENCES session_events(session_id, seq),
    CHECK (delivery_deadline_ms > created_at_ms AND delivery_deadline_ms <= created_at_ms + 600000),
    CHECK ((status = 'delivery_pending' AND writer_connection_epoch IS NULL AND writer_credential_generation IS NULL AND writer_lease_id IS NULL AND terminal_event_seq IS NULL) OR (status = 'pending' AND writer_connection_epoch IS NOT NULL AND writer_credential_generation IS NOT NULL AND writer_lease_id IS NOT NULL AND terminal_event_seq IS NULL) OR (status IN ('delivered', 'rejected', 'outcome_unknown') AND terminal_event_seq IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS session_file_reference_commands_one_nonterminal_idx ON session_file_reference_commands (session_id) WHERE status IN ('delivery_pending', 'pending');

CREATE TABLE IF NOT EXISTS session_event_proposals (
    session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
    proposal_id TEXT NOT NULL CHECK (length(proposal_id) BETWEEN 1 AND 255),
    event_seq INTEGER NOT NULL CHECK (event_seq > 0),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms > 0),
    PRIMARY KEY (session_id, proposal_id),
    UNIQUE (session_id, event_seq),
    FOREIGN KEY (session_id, event_seq) REFERENCES session_events(session_id, seq)
);

CREATE TABLE IF NOT EXISTS session_attach_attempts (
    attempt_jti_hash BLOB PRIMARY KEY CHECK (length(attempt_jti_hash) = 32),
    attach_id TEXT NOT NULL CHECK (length(attach_id) BETWEEN 1 AND 255),
    bootstrap_session_id TEXT NOT NULL CHECK (length(bootstrap_session_id) BETWEEN 1 AND 255),
    target_session_id TEXT NOT NULL CHECK (length(target_session_id) BETWEEN 1 AND 255),
    provider TEXT NOT NULL CHECK (length(provider) BETWEEN 1 AND 128),
    fingerprint_domain TEXT NOT NULL CHECK (fingerprint_domain = 'agentwharf.attach-request.v1'),
    fingerprint_version INTEGER NOT NULL CHECK (fingerprint_version = 1),
    fingerprint_digest BLOB NOT NULL CHECK (length(fingerprint_digest) = 32),
    fingerprint_key_version INTEGER NOT NULL CHECK (fingerprint_key_version > 0),
    expires_at_ns INTEGER NOT NULL CHECK (expires_at_ns > created_at_ms * 1000000),
    admission_outcome TEXT NOT NULL CHECK (admission_outcome IN ('accepted', 'rejected')),
    issued_credential_generation INTEGER,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms > 0),
    CHECK (bootstrap_session_id <> target_session_id),
    CHECK ((admission_outcome = 'accepted' AND issued_credential_generation > 0) OR
        (admission_outcome = 'rejected' AND issued_credential_generation IS NULL))
);

CREATE INDEX IF NOT EXISTS session_attach_attempts_key_expiry_idx
ON session_attach_attempts (fingerprint_key_version, expires_at_ns);

CREATE INDEX IF NOT EXISTS session_attach_attempts_expiry_idx
ON session_attach_attempts (expires_at_ns, attempt_jti_hash);

CREATE TABLE IF NOT EXISTS session_attachments (
    attach_id TEXT PRIMARY KEY CHECK (length(attach_id) BETWEEN 1 AND 255),
    bootstrap_session_id TEXT NOT NULL CHECK (length(bootstrap_session_id) BETWEEN 1 AND 255),
    target_session_id TEXT NOT NULL UNIQUE CHECK (length(target_session_id) BETWEEN 1 AND 255),
    status TEXT NOT NULL CHECK (status IN ('join_pending', 'queued', 'start_received', 'reauthorization_required', 'canceled')),
    delivery_state TEXT NOT NULL CHECK (delivery_state IN ('pending', 'received', 'completed', 'outcome_unknown')),
    delivery_version INTEGER NOT NULL DEFAULT 0 CHECK (delivery_version >= 0),
    queue_reason TEXT CHECK (queue_reason IS NULL OR length(queue_reason) BETWEEN 1 AND 128),
    expires_at_ns INTEGER,
    canceled_at_ms INTEGER,
    blocking_session_id TEXT CHECK (blocking_session_id IS NULL OR length(blocking_session_id) BETWEEN 1 AND 255),
    target_credential_lineage_ref TEXT NOT NULL CHECK (length(target_credential_lineage_ref) BETWEEN 1 AND 255),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (bootstrap_session_id <> target_session_id),
    CHECK (blocking_session_id IS NULL OR blocking_session_id <> target_session_id),
    CHECK (expires_at_ns IS NULL OR expires_at_ns > created_at_ms * 1000000),
    CHECK (canceled_at_ms IS NULL OR canceled_at_ms >= created_at_ms)
);

CREATE INDEX IF NOT EXISTS session_attachments_status_expiry_idx
ON session_attachments (status, expires_at_ns);

CREATE TABLE IF NOT EXISTS session_workspace_leases (
    workspace_key BLOB PRIMARY KEY CHECK (length(workspace_key) = 32),
    worker_id TEXT NOT NULL CHECK (length(worker_id) BETWEEN 1 AND 255),
    session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
    connection_epoch INTEGER NOT NULL CHECK (connection_epoch > 0),
    credential_generation INTEGER NOT NULL CHECK (credential_generation > 0),
    lease_id TEXT NOT NULL CHECK (length(lease_id) BETWEEN 1 AND 255),
    child_parent_workspace_key BLOB CHECK (child_parent_workspace_key IS NULL OR length(child_parent_workspace_key) = 32),
    child_capability_digest BLOB CHECK (child_capability_digest IS NULL OR length(child_capability_digest) = 32),
    child_scope_expires_at_ns INTEGER,
    status TEXT NOT NULL CHECK (status IN ('reserved', 'start_received', 'quarantined', 'released')),
    version INTEGER NOT NULL CHECK (version > 0),
    expires_at_ns INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms > 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    CHECK (expires_at_ns > created_at_ms * 1000000),
    CHECK ((child_parent_workspace_key IS NULL AND child_capability_digest IS NULL AND child_scope_expires_at_ns IS NULL) OR (child_parent_workspace_key IS NOT NULL AND child_capability_digest IS NOT NULL AND child_scope_expires_at_ns IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS session_workspace_leases_owner_idx
ON session_workspace_leases (session_id, connection_epoch, credential_generation, status);
	`); err != nil {
		return fmt.Errorf("initialize sqlite event store schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO session_attention_migration (singleton, state) VALUES (1, CASE WHEN ? THEN 'pending' ELSE 'complete' END) ON CONFLICT (singleton) DO NOTHING`, legacyStore); err != nil {
		return fmt.Errorf("initialize sqlite attention migration marker: %w", err)
	}
	if fenceTables == 0 {
		_, err := s.db.ExecContext(ctx, `
CREATE TABLE session_adapter_fence_allocator (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), next_fence INTEGER NOT NULL CHECK (next_fence > 0));
CREATE TABLE fence_store.adapter_fence_allocator (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), next_fence INTEGER NOT NULL CHECK (typeof(next_fence) = 'integer' AND next_fence > 0));
CREATE TABLE session_adapter_fence_identity (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), store_id TEXT UNIQUE NOT NULL);
CREATE TABLE fence_store.adapter_fence_identity (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), store_id TEXT UNIQUE NOT NULL);
INSERT INTO session_adapter_fence_allocator SELECT 1, COALESCE(MAX(accepted_fence), 0) + 1 FROM session_adapter_connections;
INSERT INTO fence_store.adapter_fence_allocator SELECT singleton, next_fence FROM session_adapter_fence_allocator;
INSERT INTO fence_store.adapter_fence_identity VALUES (1, lower(hex(randomblob(16))));
INSERT INTO session_adapter_fence_identity SELECT singleton, store_id FROM fence_store.adapter_fence_identity;
CREATE TRIGGER fence_store.adapter_fence_allocator_advance BEFORE UPDATE OF next_fence ON adapter_fence_allocator WHEN NEW.next_fence <> OLD.next_fence + 1 BEGIN SELECT RAISE(ABORT, 'adapter fence allocator must advance by one'); END;
CREATE TRIGGER session_adapter_connections_advance_fence BEFORE UPDATE OF accepted_fence ON session_adapter_connections WHEN NEW.accepted_fence <> OLD.accepted_fence BEGIN SELECT CASE WHEN NEW.accepted_fence <= OLD.accepted_fence OR NEW.accepted_fence >= (SELECT next_fence FROM session_adapter_fence_allocator WHERE singleton = 1) THEN RAISE(ABORT, 'adapter accepted fence is not allocator-owned') END; END;`)
		if err != nil {
			return fmt.Errorf("initialize sqlite fence store schema: %w", err)
		}
	}
	var fenceOK bool
	if err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_adapter_fence_identity) = 1 AND (SELECT count(*) FROM fence_store.adapter_fence_identity) = 1 AND EXISTS (SELECT 1 FROM session_adapter_fence_identity AS main JOIN fence_store.adapter_fence_identity AS fence USING (singleton, store_id)) AND (SELECT count(*) FROM session_adapter_fence_allocator) = 1 AND (SELECT count(*) FROM session_adapter_fence_allocator WHERE singleton = 1 AND typeof(next_fence) = 'integer' AND next_fence > (SELECT COALESCE(MAX(accepted_fence), 0) FROM session_adapter_connections)) = 1 AND (SELECT count(*) FROM fence_store.adapter_fence_allocator) = 1 AND (SELECT count(*) FROM fence_store.adapter_fence_allocator WHERE singleton = 1 AND typeof(next_fence) = 'integer' AND next_fence >= (SELECT next_fence FROM session_adapter_fence_allocator WHERE singleton = 1)) = 1 AND EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = 'session_adapter_connections_advance_fence') AND EXISTS (SELECT 1 FROM fence_store.sqlite_master WHERE type = 'trigger' AND name = 'adapter_fence_allocator_advance')`).Scan(&fenceOK); err != nil || !fenceOK {
		return errors.New("sqlite fence store state is missing, mismatched or stale")
	}
	return nil
}
