package e2ee

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

// DeviceRecord is an enrolled terminal with the trust provenance needed to
// revoke only the devices account-terminal trust added.
type DeviceRecord struct {
	Identity PairingIdentity
	Trusted  bool
}

// ensureDeviceTrustedColumn upgrades an endpoint database created before the
// trusted flag existed. Existing rows default to 0, i.e. offer-enrolled, so a
// later trust-off pass never revokes a device it did not add.
func ensureDeviceTrustedColumn(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var name string
	err := db.QueryRowContext(ctx, "SELECT name FROM pragma_table_info('e2ee_devices') WHERE name='trusted'").Scan(&name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ErrJournal
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE e2ee_devices ADD COLUMN trusted INTEGER NOT NULL DEFAULT 0 CHECK(trusted IN (0, 1))"); err != nil {
		return ErrJournal
	}
	return nil
}

// ListDetailed returns enrolled devices with their trust provenance.
func (r *DeviceRegistry) ListDetailed(ctx context.Context) ([]DeviceRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := r.db.QueryContext(ctx, "SELECT device,signing_key,wrapping_key,trusted FROM e2ee_devices WHERE machine=? AND account=? ORDER BY device", r.machine, r.account)
	if err != nil {
		return nil, ErrJournal
	}
	defer rows.Close()
	records := make([]DeviceRecord, 0, 32)
	for rows.Next() {
		var record DeviceRecord
		var trusted int
		if err := rows.Scan(&record.Identity.Device, &record.Identity.SigningKey, &record.Identity.WrappingKey, &trusted); err != nil {
			return nil, ErrJournal
		}
		if validatePairingIdentity(record.Identity) != nil {
			continue
		}
		record.Trusted = trusted == 1
		records = append(records, record)
	}
	if rows.Err() != nil {
		return nil, ErrJournal
	}
	return records, nil
}

// RevokeDevice removes one device's identity, session grants, wrapped keys and
// seal budget. It does not rotate session keys; a caller that must cut the
// device off from future content rotates the affected sessions in the same pass.
func (r *DeviceRegistry) RevokeDevice(ctx context.Context, device string) error {
	if !identifier.MatchString(device) {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ErrJournal
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM e2ee_devices WHERE machine=? AND account=? AND device=?", r.machine, r.account, device); err != nil {
		return ErrJournal
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM e2ee_local_grants WHERE device=?", device); err != nil {
		return ErrJournal
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM e2ee_session_keys WHERE device=?", device); err != nil {
		return ErrJournal
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM e2ee_seal_budget WHERE device=?", device); err != nil {
		return ErrJournal
	}
	return tx.Commit()
}

// RevokeTrusted removes every device that account-terminal trust enrolled and
// returns their IDs. Offer-enrolled devices are untouched.
func (r *DeviceRegistry) RevokeTrusted(ctx context.Context) ([]string, error) {
	records, err := r.ListDetailed(ctx)
	if err != nil {
		return nil, err
	}
	revoked := make([]string, 0, len(records))
	for _, record := range records {
		if !record.Trusted {
			continue
		}
		if err := r.RevokeDevice(ctx, record.Identity.Device); err != nil {
			return revoked, err
		}
		revoked = append(revoked, record.Identity.Device)
	}
	return revoked, nil
}

// SessionState returns a session's current epoch and key id.
func (j *CommandJournal) SessionState(ctx context.Context, session string) (int64, string, error) {
	if !identifier.MatchString(session) {
		return 0, "", ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var epoch int64
	var keyID string
	err := j.db.QueryRowContext(ctx, "SELECT epoch, key_id FROM e2ee_local_sessions WHERE session=?", session).Scan(&epoch, &keyID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", ErrUnauthorized
	}
	if err != nil {
		return 0, "", ErrJournal
	}
	return epoch, keyID, nil
}

// ListSessions returns sessions that hold a current key state.
func (j *CommandJournal) ListSessions(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := j.db.QueryContext(ctx, "SELECT session FROM e2ee_local_sessions ORDER BY session")
	if err != nil {
		return nil, ErrJournal
	}
	defer rows.Close()
	sessions := make([]string, 0, 16)
	for rows.Next() {
		var session string
		if err := rows.Scan(&session); err != nil {
			return nil, ErrJournal
		}
		sessions = append(sessions, session)
	}
	if rows.Err() != nil {
		return nil, ErrJournal
	}
	return sessions, nil
}

func (j *CommandJournal) sessionGrants(ctx context.Context, session string) ([]DeviceGrant, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := j.db.QueryContext(ctx, "SELECT device, verify_key, control FROM e2ee_local_grants WHERE session=? ORDER BY device", session)
	if err != nil {
		return nil, ErrJournal
	}
	defer rows.Close()
	grants := make([]DeviceGrant, 0, 32)
	for rows.Next() {
		var device string
		var verify []byte
		var control int
		if err := rows.Scan(&device, &verify, &control); err != nil {
			return nil, ErrJournal
		}
		if len(verify) != ed25519.PublicKeySize {
			continue
		}
		grants = append(grants, DeviceGrant{DeviceID: device, VerifyKey: ed25519.PublicKey(append([]byte(nil), verify...)), Control: control == 1})
	}
	if rows.Err() != nil {
		return nil, ErrJournal
	}
	return grants, nil
}

// withMachineControlGrant keeps the machine's own identity as a control member
// so a session stays locally readable when no terminal remains authorized.
func (v *SessionKeyVault) withMachineControlGrant(grants []DeviceGrant) ([]DeviceGrant, error) {
	key, err := decode(v.public.SigningKey, 32, 32)
	if err != nil {
		return nil, err
	}
	for i := range grants {
		if grants[i].DeviceID == v.identity.Device {
			grants[i].Control = true
			return grants, nil
		}
	}
	if len(grants) >= 32 {
		return nil, ErrCapacity
	}
	return append(grants, DeviceGrant{DeviceID: v.identity.Device, VerifyKey: ed25519.PublicKey(key), Control: true}), nil
}

// RotateSessionAfterRevoke advances a session to a fresh key whose grants are
// its current members plus the machine's own control grant. Callers must have
// already removed the revoked devices' grants; the previous key is never reused.
func (v *SessionKeyVault) RotateSessionAfterRevoke(ctx context.Context, journal *CommandJournal, session string) (string, error) {
	epoch, _, err := journal.SessionState(ctx, session)
	if err != nil {
		return "", err
	}
	grants, err := journal.sessionGrants(ctx, session)
	if err != nil {
		return "", err
	}
	grants, err = v.withMachineControlGrant(grants)
	if err != nil {
		return "", err
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", ErrJournal
	}
	newKeyID := hex.EncodeToString(raw)
	if err := v.TransitionSession(ctx, journal, session, newKeyID, epoch, grants); err != nil {
		return "", err
	}
	return newKeyID, nil
}

// RevokeTrustedTerminals removes the devices account-terminal trust enrolled and
// rotates every active session so they cannot read future content. It returns
// the revoked device IDs.
func (v *SessionKeyVault) RevokeTrustedTerminals(ctx context.Context, journal *CommandJournal, registry *DeviceRegistry) ([]string, error) {
	revoked, err := registry.RevokeTrusted(ctx)
	if err != nil {
		return revoked, err
	}
	if len(revoked) == 0 {
		return revoked, nil
	}
	sessions, err := journal.ListSessions(ctx)
	if err != nil {
		return revoked, err
	}
	for _, session := range sessions {
		if _, err := v.RotateSessionAfterRevoke(ctx, journal, session); err != nil {
			return revoked, err
		}
	}
	return revoked, nil
}
