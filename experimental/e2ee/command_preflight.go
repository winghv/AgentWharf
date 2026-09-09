package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"time"
)

// VerifyWire checks local current membership and a signed packet without
// claiming delivery or releasing plaintext. Execution must still use ExecuteWire
// because this preflight is not an authority reservation across later effects.
func (e *CommandExecutor) VerifyWire(ctx context.Context, session, id, kind string, wire []byte) error {
	return e.InspectWire(ctx, session, id, kind, wire, func(json.RawMessage) error { return nil })
}

// InspectWire exposes authenticated content only for endpoint configuration
// validation. It does not authorize side effects; execution still needs durable
// admission. The supplied payload is cleared on return and must not be retained.
func (e *CommandExecutor) InspectWire(ctx context.Context, session, id, kind string, wire []byte, inspect func(json.RawMessage) error) error {
	if inspect == nil {
		return ErrInvalid
	}
	command, packet, err := DecodeCommandWire(session, id, kind, wire)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := e.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-e.lane }()
	var public []byte
	err = e.journal.db.QueryRowContext(ctx, `SELECT g.verify_key FROM e2ee_local_sessions s JOIN e2ee_local_grants g ON g.session=s.session WHERE s.session=? AND s.key_id=? AND g.device=? AND g.control=1`, session, command.KeyID, command.Sender).Scan(&public)
	if err != nil {
		return ErrUnauthorized
	}
	key, err := e.vault.Load(ctx, session, command.KeyID)
	if err != nil {
		return err
	}
	defer clear(key)
	payload, err := OpenPacket(command, key, ed25519.PublicKey(public), packet)
	defer clear(payload)
	if err != nil {
		return err
	}
	return inspect(payload)
}
