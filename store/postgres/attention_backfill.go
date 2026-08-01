package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres/internal/db"
)

const (
	maxAttentionBackfillBatchSize  = 256
	attentionBackfillEventPageSize = 256
)

type AttentionBackfillCheckpoint struct{ AfterSessionID string }

func latestAttentionChangeSeq(seq int64, state any) pgtype.Int8 {
	if state == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: seq, Valid: true}
}

type AttentionBackfillResult struct {
	Checkpoint AttentionBackfillCheckpoint
	Processed  int
	Incomplete int
	Done       bool
}
type AttentionBackfillCheckpointStore interface {
	Load(context.Context) (AttentionBackfillCheckpoint, error)
	Save(context.Context, AttentionBackfillCheckpoint) error
}
type FileAttentionBackfillCheckpointStore struct{ Path string }

func validAttentionCheckpoint(c AttentionBackfillCheckpoint) bool {
	return len(c.AfterSessionID) <= 255 && !strings.ContainsRune(c.AfterSessionID, 0)
}

func (s *Store) RunAttentionBackfill(ctx context.Context, checkpoints AttentionBackfillCheckpointStore, batchSize int) (AttentionBackfillResult, error) {
	if checkpoints == nil {
		return AttentionBackfillResult{}, errors.New("attention backfill checkpoint store is nil")
	}
	checkpoint, err := checkpoints.Load(ctx)
	if err != nil {
		return AttentionBackfillResult{}, fmt.Errorf("load attention backfill checkpoint: %w", err)
	}
	for {
		result, batchErr := s.BackfillAttentionBatch(ctx, checkpoint, batchSize)
		if batchErr != nil {
			return result, batchErr
		}
		if err := checkpoints.Save(ctx, result.Checkpoint); err != nil {
			return result, fmt.Errorf("save attention backfill checkpoint: %w", err)
		}
		if result.Done {
			return result, nil
		}
		if result.Processed == 0 || result.Checkpoint.AfterSessionID == checkpoint.AfterSessionID {
			return result, errors.New("attention backfill made no progress")
		}
		checkpoint = result.Checkpoint
	}
}

func (s *Store) BackfillAttentionBatch(ctx context.Context, checkpoint AttentionBackfillCheckpoint, batchSize int) (AttentionBackfillResult, error) {
	if batchSize < 1 || batchSize > maxAttentionBackfillBatchSize {
		return AttentionBackfillResult{}, errors.New("attention backfill batch size is out of range")
	}
	if s.pool == nil {
		return AttentionBackfillResult{}, errors.New("postgres attention backfill pool is nil")
	}
	queries := db.New(s.pool)
	if checkpoint.AfterSessionID != "" {
		exists, err := queries.AttentionBackfillCheckpointExists(ctx, checkpoint.AfterSessionID)
		if err != nil || !exists.Valid || !exists.Bool {
			return AttentionBackfillResult{}, errors.New("attention backfill checkpoint is not a known session")
		}
	}
	rows, err := queries.BackfillAttentionSessions(ctx, db.BackfillAttentionSessionsParams{
		AfterSessionID: checkpoint.AfterSessionID,
		SessionLimit:   int32(batchSize + 1),
	})
	if err != nil {
		return AttentionBackfillResult{}, fmt.Errorf("select attention backfill sessions: %w", err)
	}
	result := AttentionBackfillResult{Checkpoint: checkpoint, Done: len(rows) <= batchSize}
	if len(rows) > batchSize {
		rows = rows[:batchSize]
	}
	for _, row := range rows {
		incomplete, rebuildErr := s.backfillAttentionSession(ctx, row)
		if rebuildErr != nil {
			return result, rebuildErr
		}
		result.Checkpoint.AfterSessionID = row
		result.Processed++
		if incomplete {
			result.Incomplete++
		}
	}
	return result, nil
}

func (s *Store) backfillAttentionSession(ctx context.Context, sessionID string) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin attention backfill: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return false, fmt.Errorf("lock attention backfill stream: %w", err)
	}
	ledger, err := queries.BackfillAttentionLedger(ctx, sessionID)
	if err != nil {
		return false, fmt.Errorf("select attention client activity: %w", err)
	}
	existingLatestSeq, err := queries.ResetAttentionForBackfill(ctx, sessionID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("reset attention projection: %w", err)
	}
	hadExistingSummary := err == nil
	incomplete := false
	eventCount := 0
	var afterSeq int64
	for {
		rows, queryErr := queries.BackfillAttentionEvents(ctx, db.BackfillAttentionEventsParams{
			SessionID: sessionID, AfterSeq: afterSeq, EventLimit: attentionBackfillEventPageSize,
		})
		if queryErr != nil {
			return false, fmt.Errorf("select attention backfill events: %w", queryErr)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			projection := attentionEventProjection(store.PendingEvent{Type: row.Type, Time: row.CreatedAt.Time, Payload: row.Payload})
			if eventCount == 0 && projection.state == nil {
				projection.projectionIncomplete = true
			}
			if projection.projectionIncomplete {
				incomplete = true
			}
			if err := queries.UpsertAttentionBackfillEvent(ctx, db.UpsertAttentionBackfillEventParams{
				SessionID: sessionID, LatestSeq: row.Seq, EventState: projection.state,
				PermissionID: projection.permissionID, PermissionDecisionID: projection.permissionDecisionID,
				PermissionChange: projection.permissionChange, TerminalOutcome: projection.terminalOutcome,
				LatestChangeSeq: latestAttentionChangeSeq(row.Seq, projection.state), EventTime: pgtype.Timestamptz{Time: row.CreatedAt.Time, Valid: true},
				ProjectionIncomplete: projection.projectionIncomplete,
			}); err != nil {
				return false, fmt.Errorf("project attention backfill event %d: %w", row.Seq, err)
			}
			afterSeq = row.Seq
			eventCount++
		}
	}
	attachment, attachmentErr := queries.BackfillAttentionAttachment(ctx, sessionID)
	hasAttachment := attachmentErr == nil
	if attachmentErr != nil && !errors.Is(attachmentErr, pgx.ErrNoRows) {
		return false, fmt.Errorf("select attention attachment: %w", attachmentErr)
	}
	ledgerVersion := ledger.CommandCount + ledger.OutcomeUnknownCount
	if hasAttachment {
		ledgerVersion += attachment.DeliveryVersion + 1
	}
	historyState, err := queries.SessionEventHistoryState(ctx, sessionID)
	if err != nil {
		return false, fmt.Errorf("select attention backfill history state: %w", err)
	}
	if eventCount == 0 {
		if err := queries.EnsureAttentionBackfillSummary(ctx, db.EnsureAttentionBackfillSummaryParams{
			SessionID: sessionID, LatestSeq: historyState.LatestSeq, SummaryVersion: ledgerVersion,
			LastClientCommandAt: ledger.LastClientCommandAt,
		}); err != nil {
			return false, fmt.Errorf("ensure incomplete attention summary: %w", err)
		}
		incomplete = incomplete || historyState.RetentionGap || hasAttachment
	}
	if err := queries.RestoreAttentionBackfillLedger(ctx, db.RestoreAttentionBackfillLedgerParams{
		SessionID: sessionID, SummaryVersion: ledgerVersion,
		LastClientCommandAt: ledger.LastClientCommandAt, HasOutcomeUnknown: ledger.OutcomeUnknownCount > 0,
	}); err != nil {
		return false, fmt.Errorf("restore attention ledger: %w", err)
	}
	if hasAttachment {
		blockerKind, blockerOperation := pgtype.Text{}, pgtype.Text{}
		blockerReason, blockerExpiresAt, blockingSessionID := pgtype.Text{}, pgtype.Timestamptz{}, pgtype.Text{}
		if ledger.OutcomeUnknownCount > 0 {
			blockerKind = pgtype.Text{String: "outcome_unknown", Valid: true}
			blockerOperation = pgtype.Text{String: "command", Valid: true}
		} else if attachment.DeliveryState == "outcome_unknown" {
			blockerKind = pgtype.Text{String: "outcome_unknown", Valid: true}
			blockerOperation = pgtype.Text{String: "attachment", Valid: true}
		} else {
			switch attachment.Status {
			case "join_pending":
				blockerKind = pgtype.Text{String: "queued", Valid: true}
				blockerReason, blockerExpiresAt = attachment.QueueReason, attachment.ExpiresAt
			case "queued":
				blockerKind = pgtype.Text{String: "queued", Valid: true}
				blockerReason, blockerExpiresAt, blockingSessionID = attachment.QueueReason, attachment.ExpiresAt, attachment.BlockingSessionID
			case "reauthorization_required":
				blockerKind = pgtype.Text{String: "reauthorization_required", Valid: true}
			case "canceled":
				blockerKind = pgtype.Text{String: "new_run_required", Valid: true}
			case "start_received":
			}
		}
		if err := queries.RestoreAttentionBackfillAttachment(ctx, db.RestoreAttentionBackfillAttachmentParams{
			SessionID: sessionID, BlockerKind: blockerKind, BlockerReason: blockerReason,
			BlockerExpiresAt: blockerExpiresAt, BlockingSessionID: blockingSessionID, BlockerOperation: blockerOperation,
		}); err != nil {
			return false, fmt.Errorf("restore attention attachment: %w", err)
		}
		incomplete = true
	}
	highWaterMismatch := hadExistingSummary && existingLatestSeq != historyState.LatestSeq
	sequenceMismatch := historyState.LatestSeq != afterSeq
	incomplete = incomplete || historyState.RetentionGap || highWaterMismatch || sequenceMismatch
	streamState, err := queries.BackfillAttentionStream(ctx, sessionID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("select attention stream: %w", err)
	}
	if err == nil {
		if err := queries.RestoreAttentionBackfillActivity(ctx, db.RestoreAttentionBackfillActivityParams{
			SessionID: sessionID, LastDurableEventAt: streamState.UpdatedAt,
		}); err != nil {
			return false, fmt.Errorf("restore attention Store-clock activity: %w", err)
		}
	}
	if err := queries.FinalizeAttentionBackfill(ctx, db.FinalizeAttentionBackfillParams{
		SessionID: sessionID, RetentionGap: historyState.RetentionGap || highWaterMismatch || sequenceMismatch,
		ProjectionIncomplete: incomplete,
	}); err != nil {
		return false, fmt.Errorf("finalize attention backfill: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit attention backfill: %w", err)
	}
	return incomplete, nil
}
