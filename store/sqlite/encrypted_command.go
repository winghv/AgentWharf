package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func validateEncryptedCommandRow(ctx context.Context, tx *sql.Tx, command store.PendingCommand) error {
	if !store.ValidEncryptedCommandType(command.Type) || command.EventSeq < 1 {
		return errors.New("invalid encrypted command reference")
	}
	var eventType string
	var payload []byte
	if err := tx.QueryRowContext(ctx, `SELECT type,payload FROM session_events WHERE session_id=? AND seq=?`, command.SessionID, command.EventSeq).Scan(&eventType, &payload); err != nil {
		return errors.New("encrypted command event unavailable")
	}
	if _, err := protocol.DecodeEncryptedPacketCarrier(payload, "command", command.Type, command.CommandID); err != nil || eventType != "session.command" {
		return errors.New("invalid encrypted command event")
	}
	return nil
}
