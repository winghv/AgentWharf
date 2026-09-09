package e2ee

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"time"
)

// SessionKeyVault persists only HPKE-wrapped session keys. Its local identity
// must come from protected endpoint storage, never the relay or a Provider.
type SessionKeyVault struct {
	db       *sql.DB
	identity LocalIdentity
	public   PairingIdentity
}

func NewSessionKeyVault(ctx context.Context, db *sql.DB, identity LocalIdentity) (*SessionKeyVault, error) {
	public, err := identity.Public()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS e2ee_session_keys (
 device TEXT NOT NULL, session TEXT NOT NULL, key_id TEXT NOT NULL,
 enc TEXT NOT NULL, ciphertext TEXT NOT NULL,
 PRIMARY KEY(device,session,key_id));
 CREATE TABLE IF NOT EXISTS e2ee_seal_budget (
 device TEXT NOT NULL, session TEXT NOT NULL, key_id TEXT NOT NULL,
 used INTEGER NOT NULL CHECK(used BETWEEN 0 AND 16384),
 PRIMARY KEY(device,session,key_id));`)
	if err != nil {
		return nil, ErrJournal
	}
	identity.SigningSeed = append([]byte(nil), identity.SigningSeed...)
	identity.WrappingPrivate = append([]byte(nil), identity.WrappingPrivate...)
	return &SessionKeyVault{db, identity, public}, nil
}

// Create is create-only and idempotent for the exact local session/key ID. It
// never replaces an existing wrapped key, including when that record is corrupt.
func (v *SessionKeyVault) Create(ctx context.Context, session, keyID string) ([]byte, error) {
	if !identifier.MatchString(session) || !identifier.MatchString(keyID) {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	key := make([]byte, 32)
	defer clear(key)
	if _, err := rand.Read(key); err != nil {
		return nil, ErrJournal
	}
	public, err := decode(v.public.WrappingKey, 65, 65)
	if err != nil {
		return nil, ErrInvalid
	}
	wrapCtx := WrapContext{session, keyID, v.identity.Device, v.identity.Device}
	wrapped, err := WrapKey(wrapCtx, v.identity.WrappingPrivate, public, key)
	if err != nil {
		return nil, err
	}
	// The database uniqueness constraint decides the winning key. All callers
	// read back the durable winner; no process can return its losing random key.
	_, err = v.db.ExecContext(ctx, `INSERT INTO e2ee_session_keys(device,session,key_id,enc,ciphertext) VALUES(?,?,?,?,?) ON CONFLICT(device,session,key_id) DO NOTHING`, v.identity.Device, session, keyID, wrapped.Enc, wrapped.Ciphertext)
	if err != nil {
		return nil, ErrJournal
	}
	return v.Load(ctx, session, keyID)
}

func (v *SessionKeyVault) Load(ctx context.Context, session, keyID string) ([]byte, error) {
	if !identifier.MatchString(session) || !identifier.MatchString(keyID) {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var wrapped WrappedKey
	err := v.db.QueryRowContext(ctx, `SELECT enc,ciphertext FROM e2ee_session_keys WHERE device=? AND session=? AND key_id=?`, v.identity.Device, session, keyID).Scan(&wrapped.Enc, &wrapped.Ciphertext)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUnauthorized
	}
	if err != nil {
		return nil, ErrJournal
	}
	public, err := decode(v.public.WrappingKey, 65, 65)
	if err != nil {
		return nil, ErrInvalid
	}
	return UnwrapKey(WrapContext{session, keyID, v.identity.Device, v.identity.Device}, v.identity.WrappingPrivate, public, wrapped)
}

// WrapForDevice requires both immutable device enrollment and a current Session
// grant. The registry and journal must use this same endpoint-owned database.
func (v *SessionKeyVault) WrapForDevice(ctx context.Context, session, keyID string, registry *DeviceRegistry, recipientID string) (WrappedKey, error) {
	if registry == nil || registry.db != v.db {
		return WrappedKey{}, ErrUnauthorized
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	recipient, err := registry.Device(ctx, recipientID)
	if err != nil {
		return WrappedKey{}, err
	}
	publicSigning, err := decode(recipient.SigningKey, 32, 32)
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	tx, err := v.db.BeginTx(ctx, nil)
	if err != nil {
		return WrappedKey{}, ErrJournal
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE e2ee_local_sessions SET command_count=command_count WHERE session=?`, session); err != nil {
		return WrappedKey{}, ErrJournal
	}
	var allowed int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM e2ee_local_grants g JOIN e2ee_local_sessions s ON s.session=g.session WHERE s.session=? AND s.key_id=? AND g.device=? AND g.verify_key=?`, session, keyID, recipient.Device, publicSigning).Scan(&allowed)
	if errors.Is(err, sql.ErrNoRows) {
		return WrappedKey{}, ErrUnauthorized
	}
	if err != nil {
		return WrappedKey{}, ErrJournal
	}
	var stored WrappedKey
	if err := tx.QueryRowContext(ctx, `SELECT enc,ciphertext FROM e2ee_session_keys WHERE device=? AND session=? AND key_id=?`, v.identity.Device, session, keyID).Scan(&stored.Enc, &stored.Ciphertext); err != nil {
		return WrappedKey{}, ErrJournal
	}
	ownPublic, err := decode(v.public.WrappingKey, 65, 65)
	if err != nil {
		return WrappedKey{}, err
	}
	key, err := UnwrapKey(WrapContext{session, keyID, v.identity.Device, v.identity.Device}, v.identity.WrappingPrivate, ownPublic, stored)
	if err != nil {
		return WrappedKey{}, err
	}
	defer clear(key)
	public, err := decode(recipient.WrappingKey, 65, 65)
	if err != nil {
		return WrappedKey{}, err
	}
	wrapped, err := WrapKey(WrapContext{session, keyID, v.identity.Device, recipient.Device}, v.identity.WrappingPrivate, public, key)
	if err != nil {
		return WrappedKey{}, err
	}
	if tx.Commit() != nil {
		return WrappedKey{}, ErrJournal
	}
	return wrapped, nil
}
