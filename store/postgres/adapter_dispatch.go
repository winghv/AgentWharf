package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres/internal/db"
)

func (s *Store) AppendAdapterEvents(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, events []store.PendingEvent) (int64, error) {
	if s == nil || s.pool == nil || len(events) == 0 {
		return 0, errors.New("invalid postgres adapter event commit")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin adapter event commit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	if err = tx.QueryRow(ctx, `SELECT 1 FROM session_adapter_connections WHERE session_id=$1 AND active_credential_generation=$2 AND connection_epoch=$3 AND accepted_fence=$4 AND connection_epoch>0 AND accepted_fence>0 AND $5::BIGINT>accepted_fence AND active_credential_expires_at>clock_timestamp() AND revoked_at IS NULL AND terminal_at IS NULL FOR UPDATE`, sessionID, admission.CredentialGeneration, admission.ConnectionEpoch, admission.AcceptedFence, admission.GrantFence).Scan(new(int)); err != nil {
		return 0, errors.New("adapter authority lost")
	}
	if err := queries.LockSessionEventStream(ctx, advisoryLockKey(sessionID)); err != nil {
		return 0, fmt.Errorf("lock session event stream: %w", err)
	}
	firstSeq, err := appendEventsLocked(ctx, queries, sessionID, events)
	if err != nil {
		return 0, err
	}
	if _, err = queries.ValidateAdapterAdmission(ctx, db.ValidateAdapterAdmissionParams{SessionID: sessionID, CredentialGeneration: admission.CredentialGeneration, ConnectionEpoch: admission.ConnectionEpoch, AcceptedFence: admission.AcceptedFence, GrantFence: admission.GrantFence}); err != nil {
		return 0, errors.New("adapter authority lost")
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit adapter events: %w", err)
	}
	return firstSeq, nil
}
