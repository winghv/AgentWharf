package e2ee

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// DeviceRegistry belongs to one locally configured machine/account binding.
// Never construct it with account or machine values supplied by an untrusted relay.
type DeviceRegistry struct {
	db               *sql.DB
	machine, account string
}

func NewDeviceRegistry(ctx context.Context, db *sql.DB, machine, account string) (*DeviceRegistry, error) {
	if !identifier.MatchString(machine) || !identifier.MatchString(account) {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
 CREATE TABLE IF NOT EXISTS e2ee_devices (
 machine TEXT NOT NULL, account TEXT NOT NULL, device TEXT NOT NULL,
 signing_key TEXT NOT NULL, wrapping_key TEXT NOT NULL,
 PRIMARY KEY(machine,account,device));
 CREATE TABLE IF NOT EXISTS e2ee_pairing_receipts (
 machine TEXT NOT NULL, account TEXT NOT NULL, invitation TEXT NOT NULL,
 device TEXT NOT NULL, fingerprint BLOB NOT NULL, confirmation TEXT NOT NULL,
 expires_at INTEGER NOT NULL, PRIMARY KEY(machine,account,invitation));`)
	if err != nil {
		return nil, ErrJournal
	}
	return &DeviceRegistry{db, machine, account}, nil
}

func pairingFingerprint(request WrappedKey) []byte {
	data, _ := json.Marshal(request)
	hash := sha256.Sum256(data)
	return hash[:]
}

// Enroll atomically stores authenticated device keys and a recoverable receipt.
// Session control grants remain separate; enrolling never grants all-session access.
func (r *DeviceRegistry) Enroll(ctx context.Context, invitation *PairingInvitation, request WrappedKey, now time.Time) (PairingReceipt, error) {
	if invitation == nil {
		return PairingReceipt{}, ErrInvalid
	}
	invitation.mu.Lock()
	machine, id, expires := invitation.offer.Machine, invitation.offer.ID, invitation.offer.ExpiresAt
	invitation.mu.Unlock()
	if machine != r.machine {
		return PairingReceipt{}, ErrUnauthorized
	}
	_, err := invitation.acceptEnrollment(request, now, func(identity PairingIdentity, receipt PairingReceipt) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return ErrJournal
		}
		defer tx.Rollback()
		// Cleanup is bounded; invitation secrets and private keys are never stored here.
		if _, err := tx.ExecContext(ctx, `DELETE FROM e2ee_pairing_receipts WHERE rowid IN (SELECT rowid FROM e2ee_pairing_receipts WHERE machine=? AND account=? AND expires_at<=? LIMIT 128)`, r.machine, r.account, now.UnixMilli()); err != nil {
			return ErrJournal
		}
		var pending int
		if tx.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_pairing_receipts WHERE machine=? AND account=?`, r.machine, r.account).Scan(&pending) != nil {
			return ErrJournal
		}
		if pending >= 128 {
			return ErrCapacity
		}
		// The cleanup write serializes enrollment before identity comparison.
		_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_devices(machine,account,device,signing_key,wrapping_key) VALUES(?,?,?,?,?) ON CONFLICT(machine,account,device) DO NOTHING`, r.machine, r.account, identity.Device, identity.SigningKey, identity.WrappingKey)
		if err != nil {
			return ErrJournal
		}
		var count int
		if tx.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_devices WHERE machine=? AND account=?`, r.machine, r.account).Scan(&count) != nil {
			return ErrJournal
		}
		if count > 32 {
			return ErrCapacity
		}
		var signing, wrapping string
		if tx.QueryRowContext(ctx, `SELECT signing_key,wrapping_key FROM e2ee_devices WHERE machine=? AND account=? AND device=?`, r.machine, r.account, identity.Device).Scan(&signing, &wrapping) != nil {
			return ErrJournal
		}
		if signing != identity.SigningKey || wrapping != identity.WrappingKey {
			return ErrConflict
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_pairing_receipts(machine,account,invitation,device,fingerprint,confirmation,expires_at) VALUES(?,?,?,?,?,?,?)`, r.machine, r.account, id, identity.Device, pairingFingerprint(request), receipt.Confirmation, expires)
		if err != nil {
			return ErrJournal
		}
		if tx.Commit() != nil {
			return ErrJournal
		}
		return nil
	})
	if err != nil {
		return PairingReceipt{}, err
	}
	return r.RecoverReceipt(ctx, id, request, now)
}

// RecoverReceipt handles exact response-loss retries after daemon restart. It
// requires the original encrypted request, not just a server-visible invitation ID.
func (r *DeviceRegistry) RecoverReceipt(ctx context.Context, id string, request WrappedKey, now time.Time) (PairingReceipt, error) {
	if _, err := decode(id, 16, 16); err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	if _, err := decode(request.Enc, 65, 65); err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	if _, err := decode(request.Ciphertext, 16, 1024); err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var confirmation string
	err := r.db.QueryRowContext(ctx, `SELECT confirmation FROM e2ee_pairing_receipts WHERE machine=? AND account=? AND invitation=? AND fingerprint=? AND expires_at>?`, r.machine, r.account, id, pairingFingerprint(request), now.UnixMilli()).Scan(&confirmation)
	if errors.Is(err, sql.ErrNoRows) {
		return PairingReceipt{}, ErrUnauthorized
	}
	if err != nil {
		return PairingReceipt{}, ErrJournal
	}
	return PairingReceipt{confirmation}, nil
}

func (r *DeviceRegistry) Device(ctx context.Context, id string) (PairingIdentity, error) {
	if !identifier.MatchString(id) {
		return PairingIdentity{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	identity := PairingIdentity{Device: id}
	err := r.db.QueryRowContext(ctx, `SELECT signing_key,wrapping_key FROM e2ee_devices WHERE machine=? AND account=? AND device=?`, r.machine, r.account, id).Scan(&identity.SigningKey, &identity.WrappingKey)
	if errors.Is(err, sql.ErrNoRows) {
		return PairingIdentity{}, ErrUnauthorized
	}
	if err != nil {
		return PairingIdentity{}, ErrJournal
	}
	if validatePairingIdentity(identity) != nil {
		return PairingIdentity{}, ErrJournal
	}
	return identity, nil
}
