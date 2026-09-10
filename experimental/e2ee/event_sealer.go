package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"time"
)

// A per-endpoint bound leaves room for up to 64 writers (including the machine)
// under the proposed 2^20 aggregate limit. Client-side enforcement and a bound
// on distinct writers across enrollment replacements remain required.
const MaxEndpointSealsPerKey = 1 << 14

// SealEvent reserves a durable use before encryption. Failed seals consume their
// reservation; process restart must never reset it. This does not allocate Hub
// seq or promise delivery. Retries should retain the original resulting packet.
func (v *SessionKeyVault) SealEvent(ctx context.Context, event Context, payload json.RawMessage) (ContentPacket, error) {
	if event.Scope != "event" || event.Sender != v.identity.Device {
		return ContentPacket{}, ErrUnauthorized
	}
	if _, err := event.bytes(); err != nil {
		return ContentPacket{}, err
	}
	projection, err := ProjectPublicMetadata(event.Type, payload)
	if err != nil {
		return ContentPacket{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	key, err := v.Load(ctx, event.Session, event.KeyID)
	if err != nil {
		return ContentPacket{}, err
	}
	defer clear(key)
	tx, err := v.db.BeginTx(ctx, nil)
	if err != nil {
		return ContentPacket{}, ErrJournal
	}
	defer tx.Rollback()
	// Reading the active epoch occurs under the same writer reservation as
	// membership transitions. Historical keys are read-only through this path.
	result, err := tx.ExecContext(ctx, `UPDATE e2ee_local_sessions SET command_count=command_count WHERE session=? AND key_id=?`, event.Session, event.KeyID)
	if err != nil {
		return ContentPacket{}, ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ContentPacket{}, ErrUnauthorized
	}
	result, err = tx.ExecContext(ctx, `INSERT INTO e2ee_seal_budget(device,session,key_id,used) VALUES(?,?,?,1)
 ON CONFLICT(device,session,key_id) DO UPDATE SET used=used+1 WHERE used < ?`, v.identity.Device, event.Session, event.KeyID, MaxEndpointSealsPerKey)
	if err != nil {
		return ContentPacket{}, ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ContentPacket{}, ErrCapacity
	}
	if tx.Commit() != nil {
		return ContentPacket{}, ErrJournal
	}
	if ctx.Err() != nil {
		return ContentPacket{}, ErrJournal
	}
	signer := ed25519.NewKeyFromSeed(v.identity.SigningSeed)
	defer clear(signer)
	return SealPacket(event, key, signer, projection, payload)
}
