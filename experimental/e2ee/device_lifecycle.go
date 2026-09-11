package e2ee

import (
	"context"
	"database/sql"
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
