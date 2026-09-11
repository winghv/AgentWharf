package e2ee

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"time"
)

// List returns the enrolled device identities for this machine/account binding.
// It seeds session membership for account-terminal trust.
func (r *DeviceRegistry) List(ctx context.Context) ([]PairingIdentity, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := r.db.QueryContext(ctx, "SELECT device,signing_key,wrapping_key FROM e2ee_devices WHERE machine=? AND account=? ORDER BY device", r.machine, r.account)
	if err != nil {
		return nil, ErrJournal
	}
	defer rows.Close()
	devices := make([]PairingIdentity, 0, 32)
	for rows.Next() {
		var identity PairingIdentity
		if err := rows.Scan(&identity.Device, &identity.SigningKey, &identity.WrappingKey); err != nil {
			return nil, ErrJournal
		}
		if validatePairingIdentity(identity) != nil {
			continue
		}
		devices = append(devices, identity)
	}
	if rows.Err() != nil {
		return nil, ErrJournal
	}
	return devices, nil
}

// EnrollTrusted stores a device identity authorized by the machine's
// account-terminal trust setting. Unlike Enroll it consumes no local invitation
// secret; the caller must have authenticated the platform account scope first.
// It never grants session control; content grants are added separately.
func (r *DeviceRegistry) EnrollTrusted(ctx context.Context, identity PairingIdentity) error {
	if validatePairingIdentity(identity) != nil {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ErrJournal
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO e2ee_devices(machine,account,device,signing_key,wrapping_key,trusted) VALUES(?,?,?,?,?,1) ON CONFLICT(machine,account,device) DO NOTHING", r.machine, r.account, identity.Device, identity.SigningKey, identity.WrappingKey); err != nil {
		return ErrJournal
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM e2ee_devices WHERE machine=? AND account=?", r.machine, r.account).Scan(&count); err != nil {
		return ErrJournal
	}
	if count > 32 {
		return ErrCapacity
	}
	var signing, wrapping string
	if err := tx.QueryRowContext(ctx, "SELECT signing_key,wrapping_key FROM e2ee_devices WHERE machine=? AND account=? AND device=?", r.machine, r.account, identity.Device).Scan(&signing, &wrapping); err != nil {
		return ErrJournal
	}
	if signing != identity.SigningKey || wrapping != identity.WrappingKey {
		return ErrConflict
	}
	return tx.Commit()
}

// GrantEnrolledDevice adds or upgrades a device grant for every session that
// already has a current key. It never removes or downgrades an existing control
// grant, and returns the number of sessions it inserted or upgraded.
func (j *CommandJournal) GrantEnrolledDevice(ctx context.Context, device string, verifyKey ed25519.PublicKey, control bool) (int, error) {
	if !identifier.MatchString(device) || len(verifyKey) != ed25519.PublicKeySize {
		return 0, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	insert := "INSERT INTO e2ee_local_grants(session, device, verify_key, control) SELECT session, ?, ?, ? FROM e2ee_local_sessions WHERE 1 ON CONFLICT(session, device) DO NOTHING"
	if control {
		insert = "INSERT INTO e2ee_local_grants(session, device, verify_key, control) SELECT session, ?, ?, ? FROM e2ee_local_sessions WHERE 1 ON CONFLICT(session, device) DO UPDATE SET control=1 WHERE e2ee_local_grants.control=0"
	}
	result, err := j.db.ExecContext(ctx, insert, device, []byte(verifyKey), control)
	if err != nil {
		return 0, ErrJournal
	}
	added, err := result.RowsAffected()
	if err != nil {
		return 0, ErrJournal
	}
	return int(added), nil
}

// sessionSeedGrants returns the creator's control grant plus view grants for
// every other enrolled device. Callers must already have authenticated the creator.
func (r *DeviceRegistry) sessionSeedGrants(ctx context.Context, creator string, creatorKey ed25519.PublicKey) ([]DeviceGrant, error) {
	devices, err := r.List(ctx)
	if err != nil {
		return nil, err
	}
	grants := []DeviceGrant{{DeviceID: creator, VerifyKey: creatorKey, Control: true}}
	for _, device := range devices {
		if device.Device == creator {
			continue
		}
		key, err := decode(device.SigningKey, 32, 32)
		if err != nil {
			continue
		}
		grants = append(grants, DeviceGrant{DeviceID: device.Device, VerifyKey: ed25519.PublicKey(key), Control: false})
	}
	if len(grants) > 32 {
		return nil, ErrCapacity
	}
	return grants, nil
}

// SessionExists reports whether the endpoint holds a key state for the session.
func (j *CommandJournal) SessionExists(ctx context.Context, session string) (bool, error) {
	if !identifier.MatchString(session) {
		return false, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var count int
	if err := j.db.QueryRowContext(ctx, "SELECT count(*) FROM e2ee_local_sessions WHERE session=?", session).Scan(&count); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, ErrJournal
	}
	return count > 0, nil
}
