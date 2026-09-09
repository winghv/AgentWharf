package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const maxCommandsPerSession = 4096

var (
	ErrUnauthorized = errors.New("endpoint command unauthorized")
	ErrConflict     = errors.New("endpoint command conflict")
	ErrCapacity     = errors.New("endpoint command capacity exhausted")
	ErrJournal      = errors.New("endpoint journal unavailable")
)

type DeviceGrant struct {
	DeviceID  string
	VerifyKey ed25519.PublicKey
	Control   bool
}

// CommandJournal uses an endpoint-owned database, never the platform EventStore.
// The caller owns the database, protected storage, and authenticated enrollment.
type CommandJournal struct{ db *sql.DB }

type CommandAdmission struct {
	Execute bool
	State   string
}

// NewCommandJournal initializes an isolated experimental schema. Transactions
// containing admission writes must be durably committed before any Provider effect.
func NewCommandJournal(ctx context.Context, db *sql.DB) (*CommandJournal, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, ErrJournal
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
 CREATE TABLE IF NOT EXISTS e2ee_local_sessions (
 session TEXT PRIMARY KEY,
 epoch INTEGER NOT NULL CHECK(epoch > 0),
 key_id TEXT NOT NULL,
 command_count INTEGER NOT NULL DEFAULT 0 CHECK(command_count BETWEEN 0 AND 4096)
 );
 CREATE TABLE IF NOT EXISTS e2ee_local_key_epochs (
 session TEXT NOT NULL,
 key_id TEXT NOT NULL,
 PRIMARY KEY(session, key_id)
 );
 CREATE TABLE IF NOT EXISTS e2ee_local_grants (
 session TEXT NOT NULL,
 device TEXT NOT NULL,
 verify_key BLOB NOT NULL CHECK(length(verify_key) = 32),
 control INTEGER NOT NULL CHECK(control IN (0, 1)),
 PRIMARY KEY(session, device)
 );
 CREATE TABLE IF NOT EXISTS e2ee_membership_receipts (
 session TEXT PRIMARY KEY,
 epoch INTEGER NOT NULL CHECK(epoch > 1),
 key_id TEXT NOT NULL,
 request_hash BLOB NOT NULL CHECK(length(request_hash)=32)
 );
 CREATE TABLE IF NOT EXISTS e2ee_local_commands (
 session TEXT NOT NULL,
 message TEXT NOT NULL,
 fingerprint BLOB NOT NULL CHECK(length(fingerprint) = 32),
 state TEXT NOT NULL CHECK(state IN ('claimed', 'completed', 'outcome_unknown')),
 PRIMARY KEY(session, message)
 );`)
	if err != nil {
		return nil, ErrJournal
	}
	if tx.Commit() != nil {
		return nil, ErrJournal
	}
	return &CommandJournal{db: db}, nil
}

// ReplaceGrants accepts only a locally authenticated membership transition.
// expectedEpoch=0 creates a new session; all later changes use exact CAS.
// Never call this with a membership list authenticated only by platform bearer.
func (j *CommandJournal) ReplaceGrants(ctx context.Context, session, keyID string, expectedEpoch int64, grants []DeviceGrant) error {
	if err := validateGrants(session, keyID, expectedEpoch, grants); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return ErrJournal
	}
	defer tx.Rollback()
	if err := replaceGrantsTx(ctx, tx, session, keyID, expectedEpoch, grants); err != nil {
		return err
	}
	if tx.Commit() != nil {
		return ErrJournal
	}
	return nil
}

func validateGrants(session, keyID string, expectedEpoch int64, grants []DeviceGrant) error {
	if !identifier.MatchString(session) || !identifier.MatchString(keyID) || expectedEpoch < 0 || expectedEpoch >= 1<<53 || len(grants) == 0 || len(grants) > 32 {
		return ErrInvalid
	}
	seen := make(map[string]bool, len(grants))
	for _, grant := range grants {
		if !identifier.MatchString(grant.DeviceID) || len(grant.VerifyKey) != ed25519.PublicKeySize || seen[grant.DeviceID] {
			return ErrInvalid
		}
		seen[grant.DeviceID] = true
	}
	return nil
}

func replaceGrantsTx(ctx context.Context, tx *sql.Tx, session, keyID string, expectedEpoch int64, grants []DeviceGrant) error {
	var result sql.Result
	var err error
	if expectedEpoch == 0 {
		result, err = tx.ExecContext(ctx, `INSERT INTO e2ee_local_sessions(session, epoch, key_id) VALUES (?, 1, ?) ON CONFLICT(session) DO NOTHING`, session, keyID)
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE e2ee_local_sessions SET epoch = epoch + 1, key_id = ? WHERE session = ? AND epoch = ? AND key_id <> ?`, keyID, session, expectedEpoch, keyID)
	}
	if err != nil {
		return ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ErrConflict
	}
	result, err = tx.ExecContext(ctx, `INSERT INTO e2ee_local_key_epochs(session, key_id) VALUES (?, ?) ON CONFLICT(session, key_id) DO NOTHING`, session, keyID)
	if err != nil {
		return ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ErrConflict
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM e2ee_local_grants WHERE session = ?`, session); err != nil {
		return ErrJournal
	}
	for _, grant := range grants {
		if _, err = tx.ExecContext(ctx, `INSERT INTO e2ee_local_grants(session, device, verify_key, control) VALUES (?, ?, ?, ?)`, session, grant.DeviceID, []byte(grant.VerifyKey), grant.Control); err != nil {
			return ErrJournal
		}
	}
	return nil
}

// Admit decrypts and authenticates before reserving a command. A claimed command
// is never automatically retried, including after restart. This prevents duplicate
// effects but deliberately exposes uncertainty after a crash before completion.
func (j *CommandJournal) Admit(ctx context.Context, command Context, key []byte, envelope Envelope) (CommandAdmission, []byte, error) {
	return j.admitValidated(ctx, command, key, envelope, nil)
}

// validate executes after authentication but before the durable reservation.
// It must be pure and bounded; it must not perform Provider or database effects.
func (j *CommandJournal) admitValidated(ctx context.Context, command Context, key []byte, envelope Envelope, validate func([]byte) error) (CommandAdmission, []byte, error) {
	if command.Scope != "command" && command.Scope != "launch" {
		return CommandAdmission{}, nil, ErrInvalid
	}
	aad, err := command.bytes()
	if err != nil {
		return CommandAdmission{}, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return CommandAdmission{}, nil, ErrJournal
	}
	defer tx.Rollback()
	// Acquire SQLite's writer reservation before reading membership so admission
	// and revocation serialize, including across processes/connections.
	result, err := tx.ExecContext(ctx, `UPDATE e2ee_local_sessions SET command_count = command_count WHERE session = ? AND key_id = ?`, command.Session, command.KeyID)
	if err != nil {
		return CommandAdmission{}, nil, ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return CommandAdmission{}, nil, ErrUnauthorized
	}
	var publicKey []byte
	var control bool
	err = tx.QueryRowContext(ctx, `SELECT verify_key, control FROM e2ee_local_grants WHERE session = ? AND device = ?`, command.Session, command.Sender).Scan(&publicKey, &control)
	if errors.Is(err, sql.ErrNoRows) {
		return CommandAdmission{}, nil, ErrUnauthorized
	}
	if err != nil {
		return CommandAdmission{}, nil, ErrJournal
	}
	if !control {
		return CommandAdmission{}, nil, ErrUnauthorized
	}
	plaintext, err := Open(command, key, ed25519.PublicKey(publicKey), envelope)
	if err != nil {
		return CommandAdmission{}, nil, err
	}
	defer clear(plaintext)
	if validate != nil {
		if err := validate(plaintext); err != nil {
			return CommandAdmission{}, nil, ErrInvalid
		}
	}
	wire, err := json.Marshal(envelope)
	if err != nil {
		return CommandAdmission{}, nil, ErrInvalid
	}
	fingerprint := sha256.Sum256(append(aad, wire...))
	var stored []byte
	var state string
	err = tx.QueryRowContext(ctx, `SELECT fingerprint, state FROM e2ee_local_commands WHERE session = ? AND message = ?`, command.Session, command.MessageID).Scan(&stored, &state)
	if err == nil {
		if !bytes.Equal(stored, fingerprint[:]) {
			return CommandAdmission{}, nil, ErrConflict
		}
		if state == "claimed" {
			state = "outcome_unknown"
		}
		return CommandAdmission{State: state}, nil, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return CommandAdmission{}, nil, ErrJournal
	}
	result, err = tx.ExecContext(ctx, `UPDATE e2ee_local_sessions SET command_count = command_count + 1 WHERE session = ? AND command_count < ?`, command.Session, maxCommandsPerSession)
	if err != nil {
		return CommandAdmission{}, nil, ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return CommandAdmission{}, nil, ErrCapacity
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_local_commands(session, message, fingerprint, state) VALUES (?, ?, ?, 'claimed')`, command.Session, command.MessageID, fingerprint[:])
	if err != nil {
		return CommandAdmission{}, nil, ErrJournal
	}
	if tx.Commit() != nil {
		return CommandAdmission{}, nil, ErrJournal
	}
	return CommandAdmission{Execute: true, State: "claimed"}, append([]byte(nil), plaintext...), nil
}

// Finish is called only by the local executor, never from a Hub receipt.
func (j *CommandJournal) Finish(ctx context.Context, session, message, state string) error {
	if !identifier.MatchString(session) || !identifier.MatchString(message) || (state != "completed" && state != "outcome_unknown") {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := j.db.ExecContext(ctx, `UPDATE e2ee_local_commands SET state = ? WHERE session = ? AND message = ? AND state IN ('claimed', ?)`, state, session, message, state)
	if err != nil {
		return ErrJournal
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return ErrConflict
	}
	return nil
}
