package e2ee

import (
	"context"
	"crypto/ed25519"
	"time"
)

// WithProcessStart holds the endpoint database writer reservation across local
// authorization and synchronous process creation, serializing with revocation
// even when a second executor uses the same database. It does not claim prompt
// delivery; ExecuteWire remains required for provider input.
func (e *CommandExecutor) WithProcessStart(ctx context.Context, session, id string, wire []byte, start func() error) error {
	if start == nil {
		return ErrInvalid
	}
	command, packet, err := DecodeCommandWire(session, id, "session.send", wire)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := e.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-e.lane }()
	key, err := e.vault.Load(ctx, session, command.KeyID)
	if err != nil {
		return err
	}
	defer clear(key)
	connection, err := e.journal.db.Conn(ctx)
	if err != nil {
		return ErrJournal
	}
	defer connection.Close()
	// SQL acquisition/validation uses the deadline, but automatic transaction
	// cancellation must not unlock authority while the synchronous start runs.
	tx, err := connection.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return ErrJournal
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE e2ee_local_sessions SET command_count=command_count WHERE session=? AND key_id=?`, session, command.KeyID)
	if err != nil {
		return ErrJournal
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return ErrUnauthorized
	}
	var public []byte
	if err := tx.QueryRowContext(ctx, `SELECT verify_key FROM e2ee_local_grants WHERE session=? AND device=? AND control=1`, session, command.Sender).Scan(&public); err != nil {
		return ErrUnauthorized
	}
	payload, err := OpenPacket(command, key, ed25519.PublicKey(public), packet)
	defer clear(payload)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// No durable mutation follows the side effect. Rolling back the no-op UPDATE
	// releases the writer reservation without falsely failing an existing child.
	return start()
}
