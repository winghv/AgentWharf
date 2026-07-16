package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/winghv/agentwharf/store"
	sqliteDriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	maxHistoryPageSize  = 100
	maxEventPayloadSize = 64 * 1024
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
		return attachment.DeliveryState == store.AttachmentDeliveryPending && attachment.QueueReason == nil && attachment.ExpiresAt != nil && attachment.CanceledAt == nil && attachment.BlockingSessionID == nil
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
	return value != nil && (*value == "start" || *value == "command")
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
`); err != nil {
		return fmt.Errorf("initialize sqlite event store schema: %w", err)
	}
	return nil
}
