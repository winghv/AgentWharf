package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/winghv/agentwharf/store"
	_ "modernc.org/sqlite"
)

const maxHistoryPageSize = 100

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

	existing, err := queryPendingCommand(ctx, tx, sessionID, request.CommandID)
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
	if err := tx.Commit(); err != nil {
		return store.PendingCommandCommit{}, fmt.Errorf("commit pending command: %w", err)
	}
	return store.PendingCommandCommit{Command: store.PendingCommand{
		SessionID: sessionID, CommandID: request.CommandID, Type: request.Type, EventSeq: seq,
		Status: store.PendingCommandPending, ExpiresAt: request.ExpiresAt,
	}}, nil
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
	command, err := queryPendingCommand(ctx, tx, sessionID, commandID)
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
	command, err := queryPendingCommand(ctx, tx, sessionID, commandID)
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
	if err := tx.Commit(); err != nil {
		return store.PendingCommand{}, fmt.Errorf("commit pending command resolution: %w", err)
	}
	command.Status = status
	return command, nil
}

func sqliteNowMillis(ctx context.Context, tx *sql.Tx) (int64, error) {
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

func queryPendingCommand(ctx context.Context, tx *sql.Tx, sessionID, commandID string) (store.PendingCommand, error) {
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
`, sessionID, commandID).Scan(
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
		createdAtMS < 1 || expiresAtNS <= createdAtMS*int64(time.Millisecond) ||
		expiresAtNS > (createdAtMS+30000)*int64(time.Millisecond) || eventType != "session.message" ||
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
		event.Type != "session.message" || json.Unmarshal(event.Payload, &payload) != nil || payload.Role != "user" ||
		!request.ExpiresAt.After(storeNow) || request.ExpiresAt.After(storeNow.Add(30*time.Second)) {
		return errors.New("invalid pending command")
	}
	return nil
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
    CHECK (revoked_at_ms IS NULL OR revoked_at_ms >= created_at_ms),
    CHECK (terminal_at_ms IS NULL OR terminal_at_ms >= created_at_ms)
);

CREATE INDEX IF NOT EXISTS session_adapter_connections_active_expiry_idx
ON session_adapter_connections (active_credential_expires_at_ms);

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
`); err != nil {
		return fmt.Errorf("initialize sqlite event store schema: %w", err)
	}
	return nil
}
