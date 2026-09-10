package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

// The caller holds the command row lock. Lock its immutable content reference
// through the status update too, so deletion or corruption cannot race admission.
func validateEncryptedCommandRow(ctx context.Context, tx pgx.Tx, command store.PendingCommand) error {
	if !store.ValidEncryptedCommandType(command.Type) || command.EventSeq < 1 {
		return errors.New("invalid encrypted command reference")
	}
	var eventType string
	var payload []byte
	err := tx.QueryRow(ctx, `SELECT type,payload FROM session_events WHERE session_id=$1 AND seq=$2 FOR SHARE`, command.SessionID, command.EventSeq).Scan(&eventType, &payload)
	if err != nil {
		return errors.New("encrypted command event unavailable")
	}
	if _, err = protocol.DecodeEncryptedPacketCarrier(payload, "command", command.Type, command.CommandID); err != nil || eventType != "session.command" {
		return errors.New("invalid encrypted command event")
	}
	return nil
}
