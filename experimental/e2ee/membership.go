package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

type SessionMember struct {
	Device  string `json:"device"`
	Control bool   `json:"control"`
}
type SessionMembershipChange struct {
	Machine       string          `json:"machine"`
	Account       string          `json:"account"`
	Session       string          `json:"session"`
	Device        string          `json:"device"`
	KeyID         string          `json:"key_id"`
	ExpectedEpoch int64           `json:"expected_epoch"`
	Members       []SessionMember `json:"members"`
	Signature     string          `json:"signature"`
}

func (r SessionMembershipChange) signingBytes() ([]byte, error) {
	for _, value := range []string{r.Machine, r.Account, r.Session, r.Device, r.KeyID} {
		if !identifier.MatchString(value) {
			return nil, ErrInvalid
		}
	}
	if r.ExpectedEpoch < 1 || r.ExpectedEpoch >= 9007199254740991 || len(r.Members) < 1 || len(r.Members) > 32 {
		return nil, ErrInvalid
	}
	members := make([][]any, len(r.Members))
	for i, member := range r.Members {
		if !identifier.MatchString(member.Device) || (i > 0 && r.Members[i-1].Device >= member.Device) {
			return nil, ErrInvalid
		}
		members[i] = []any{member.Device, member.Control}
	}
	return json.Marshal([]any{"agentwharf.e2ee.membership.v1", r.Machine, r.Account, r.Session, r.Device, r.KeyID, r.ExpectedEpoch, members})
}
func SignSessionMembershipChange(r SessionMembershipChange, key ed25519.PrivateKey) (SessionMembershipChange, error) {
	data, err := r.signingBytes()
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return SessionMembershipChange{}, ErrInvalid
	}
	r.Members = append([]SessionMember(nil), r.Members...)
	r.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, data))
	return r, nil
}
func DecodeSessionMembershipChange(data []byte) (SessionMembershipChange, error) {
	var r SessionMembershipChange
	if len(data) > 8192 {
		return r, ErrInvalid
	}
	fields, err := packetFields(data)
	if err != nil || len(fields) != 8 {
		return r, ErrInvalid
	}
	for _, name := range []string{"machine", "account", "session", "device", "key_id", "expected_epoch", "members", "signature"} {
		if fields[name] == nil {
			return r, ErrInvalid
		}
	}
	if json.Unmarshal(data, &r) != nil {
		return r, ErrInvalid
	}
	var members []json.RawMessage
	if json.Unmarshal(fields["members"], &members) != nil {
		return r, ErrInvalid
	}
	for _, raw := range members {
		f, err := packetFields(raw)
		if err != nil || len(f) != 2 || f["device"] == nil || f["control"] == nil || (string(f["control"]) != "true" && string(f["control"]) != "false") {
			return r, ErrInvalid
		}
	}
	if _, err := r.signingBytes(); err != nil {
		return r, err
	}
	if _, err := decode(r.Signature, 64, 64); err != nil {
		return r, err
	}
	return r, nil
}

// ApplyMembership uses the immutable paired registry for identities, and the
// current session controller grant for authority. Bearer/relay state is irrelevant.
func (e *CommandExecutor) ApplyMembership(ctx context.Context, registry *DeviceRegistry, r SessionMembershipChange) error {
	r.Members = append([]SessionMember(nil), r.Members...)
	if registry == nil || registry.db != e.vault.db || registry.machine != r.Machine || registry.account != r.Account {
		return ErrUnauthorized
	}
	data, err := r.signingBytes()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	device, err := registry.Device(ctx, r.Device)
	if err != nil {
		return ErrUnauthorized
	}
	public, err := decode(device.SigningKey, 32, 32)
	if err != nil {
		return err
	}
	signature, err := decode(r.Signature, 64, 64)
	if err != nil || !ed25519.Verify(public, data, signature) {
		return ErrUnauthorized
	}
	grants := make([]DeviceGrant, len(r.Members))
	for i, member := range r.Members {
		device, err := registry.Device(ctx, member.Device)
		if err != nil {
			return ErrUnauthorized
		}
		key, err := decode(device.SigningKey, 32, 32)
		if err != nil {
			return err
		}
		grants[i] = DeviceGrant{DeviceID: member.Device, VerifyKey: key, Control: member.Control}
	}
	if err := e.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-e.lane }()
	hash := sha256.Sum256(data)
	return e.vault.transitionSession(ctx, e.journal, r.Session, r.KeyID, r.ExpectedEpoch, grants, &membershipAuthorization{device: r.Device, public: public, hash: hash[:]})
}

type membershipAuthorization struct {
	device       string
	public, hash []byte
}

func (a *membershipAuthorization) authorize(ctx context.Context, tx *sql.Tx, session, keyID string, epoch int64) (bool, error) {
	if _, err := tx.ExecContext(ctx, `UPDATE e2ee_local_sessions SET command_count=command_count WHERE session=?`, session); err != nil {
		return false, ErrJournal
	}
	var stored []byte
	err := tx.QueryRowContext(ctx, `SELECT r.request_hash FROM e2ee_membership_receipts r JOIN e2ee_local_sessions s ON s.session=r.session AND s.epoch=r.epoch AND s.key_id=r.key_id WHERE r.session=? AND r.epoch=? AND r.key_id=?`, session, epoch+1, keyID).Scan(&stored)
	if err == nil {
		if bytes.Equal(stored, a.hash) {
			return true, nil
		}
		return false, ErrConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, ErrJournal
	}
	var authorized int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_local_sessions s JOIN e2ee_local_grants g ON g.session=s.session WHERE s.session=? AND s.epoch=? AND g.device=? AND g.verify_key=? AND g.control=1`, session, epoch, a.device, a.public).Scan(&authorized); err != nil {
		return false, ErrJournal
	}
	if authorized != 1 {
		return false, ErrUnauthorized
	}
	return false, nil
}
