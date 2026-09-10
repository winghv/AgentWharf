package e2ee

import (
	"context"
	"crypto/rand"
	"time"
)

// TransitionSession atomically creates a new epoch's wrapped content key and
// replaces its grants. Caller authentication is a local endpoint responsibility;
// this method must never be exposed using platform bearer authorization alone.
func (v *SessionKeyVault) TransitionSession(ctx context.Context, journal *CommandJournal, session, keyID string, expectedEpoch int64, grants []DeviceGrant) error {
	return v.transitionSession(ctx, journal, session, keyID, expectedEpoch, grants, nil)
}

func (v *SessionKeyVault) transitionSession(ctx context.Context, journal *CommandJournal, session, keyID string, expectedEpoch int64, grants []DeviceGrant, membership *membershipAuthorization) error {
	if journal == nil || journal.db != v.db {
		return ErrInvalid
	}
	if err := validateGrants(session, keyID, expectedEpoch, grants); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	key := make([]byte, 32)
	defer clear(key)
	if _, err := rand.Read(key); err != nil {
		return ErrJournal
	}
	public, err := decode(v.public.WrappingKey, 65, 65)
	if err != nil {
		return err
	}
	wrapped, err := WrapKey(WrapContext{session, keyID, v.identity.Device, v.identity.Device}, v.identity.WrappingPrivate, public, key)
	if err != nil {
		return err
	}
	tx, err := v.db.BeginTx(ctx, nil)
	if err != nil {
		return ErrJournal
	}
	defer tx.Rollback()
	if membership != nil {
		duplicate, err := membership.authorize(ctx, tx, session, keyID, expectedEpoch)
		if err != nil {
			return err
		}
		if duplicate {
			return tx.Commit()
		}
	}
	if err := replaceGrantsTx(ctx, tx, session, keyID, expectedEpoch, grants); err != nil {
		return err
	}
	// A preexisting standalone key cannot be silently adopted or replaced.
	result, err := tx.ExecContext(ctx, `INSERT INTO e2ee_session_keys(device,session,key_id,enc,ciphertext) VALUES(?,?,?,?,?) ON CONFLICT(device,session,key_id) DO NOTHING`, v.identity.Device, session, keyID, wrapped.Enc, wrapped.Ciphertext)
	if err != nil {
		return ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ErrConflict
	}
	if membership != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO e2ee_membership_receipts(session,epoch,key_id,request_hash) VALUES(?,?,?,?) ON CONFLICT(session) DO UPDATE SET epoch=excluded.epoch,key_id=excluded.key_id,request_hash=excluded.request_hash`, session, expectedEpoch+1, keyID, membership.hash); err != nil {
			return ErrJournal
		}
	}
	if tx.Commit() != nil {
		return ErrJournal
	}
	return nil
}
