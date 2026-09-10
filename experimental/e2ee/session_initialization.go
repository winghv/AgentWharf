package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"time"
)

// SessionInitialization authorizes only epoch one for its signing device.
// It is not a membership update or permission to distribute historical keys.
type SessionInitialization struct {
	Machine   string `json:"machine"`
	Account   string `json:"account"`
	Session   string `json:"session"`
	KeyID     string `json:"key_id"`
	Device    string `json:"device"`
	Signature string `json:"signature"`
}

// DecodeSessionInitialization rejects ambiguous JSON before endpoint validation.
func DecodeSessionInitialization(data []byte) (SessionInitialization, error) {
	if len(data) > 2048 {
		return SessionInitialization{}, ErrInvalid
	}
	fields, err := packetFields(data)
	if err != nil || len(fields) != 6 {
		return SessionInitialization{}, ErrInvalid
	}
	var request SessionInitialization
	targets := map[string]*string{"machine": &request.Machine, "account": &request.Account, "session": &request.Session, "key_id": &request.KeyID, "device": &request.Device, "signature": &request.Signature}
	for name, target := range targets {
		if fields[name] == nil || json.Unmarshal(fields[name], target) != nil {
			return SessionInitialization{}, ErrInvalid
		}
	}
	if _, err := request.signingBytes(); err != nil {
		return SessionInitialization{}, err
	}
	if _, err := decode(request.Signature, 64, 64); err != nil {
		return SessionInitialization{}, err
	}
	return request, nil
}

func (r SessionInitialization) signingBytes() ([]byte, error) {
	for _, id := range []string{r.Machine, r.Account, r.Session, r.KeyID, r.Device} {
		if !identifier.MatchString(id) {
			return nil, ErrInvalid
		}
	}
	return json.Marshal([]string{"agentwharf.e2ee.session-init.v1", r.Machine, r.Account, r.Session, r.KeyID, r.Device})
}

func SignSessionInitialization(request SessionInitialization, signer ed25519.PrivateKey) (SessionInitialization, error) {
	data, err := request.signingBytes()
	if err != nil || len(signer) != ed25519.PrivateKeySize {
		return SessionInitialization{}, ErrInvalid
	}
	request.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(signer, data))
	return request, nil
}

// VerifySessionInitialization checks paired-device authority without creating
// keys or mutating membership. Relay completion flags are not authentication.
func (r *DeviceRegistry) VerifySessionInitialization(ctx context.Context, request SessionInitialization) (ed25519.PublicKey, error) {
	if r == nil || request.Machine != r.machine || request.Account != r.account {
		return nil, ErrUnauthorized
	}
	data, err := request.signingBytes()
	if err != nil {
		return nil, err
	}
	device, err := r.Device(ctx, request.Device)
	if err != nil {
		return nil, err
	}
	public, err := decode(device.SigningKey, 32, 32)
	if err != nil {
		return nil, err
	}
	signature, err := decode(request.Signature, 64, 64)
	if err != nil || !ed25519.Verify(public, data, signature) {
		return nil, ErrUnauthorized
	}
	return public, nil
}

// InitializeSession authenticates against the endpoint registry, never relay
// metadata. Exact retries can recover the wrapped key; epoch CAS prevents a
// request from replacing any preexisting session or changing its grants.
func (e *CommandExecutor) InitializeSession(ctx context.Context, registry *DeviceRegistry, request SessionInitialization) (WrappedKey, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if registry == nil || registry.db != e.vault.db || request.Machine != registry.machine || request.Account != registry.account {
		return WrappedKey{}, ErrUnauthorized
	}
	public, err := registry.VerifySessionInitialization(ctx, request)
	if err != nil {
		return WrappedKey{}, err
	}
	if err := e.acquire(ctx); err != nil {
		return WrappedKey{}, err
	}
	defer func() { <-e.lane }()
	grants, err := registry.sessionSeedGrants(ctx, request.Device, public)
	if err != nil {
		return WrappedKey{}, err
	}
	err = e.vault.TransitionSession(ctx, e.journal, request.Session, request.KeyID, 0, grants)
	if err != nil && err != ErrConflict {
		return WrappedKey{}, err
	}
	if err == ErrConflict {
		if err := e.verifyInitializedSession(ctx, request, public); err != nil {
			return WrappedKey{}, err
		}
	}
	return e.vault.WrapForDevice(ctx, request.Session, request.KeyID, registry, request.Device)
}

// VerifyInitializedSession checks completed relay recovery without creating
// authority, distributing keys, or accepting a request superseded by rotation.
func (e *CommandExecutor) VerifyInitializedSession(ctx context.Context, registry *DeviceRegistry, request SessionInitialization) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if registry == nil || registry.db != e.vault.db || request.Machine != registry.machine || request.Account != registry.account {
		return ErrUnauthorized
	}
	public, err := registry.VerifySessionInitialization(ctx, request)
	if err != nil {
		return err
	}
	if err := e.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-e.lane }()
	return e.verifyInitializedSession(ctx, request, public)
}

func (e *CommandExecutor) verifyInitializedSession(ctx context.Context, request SessionInitialization, public []byte) error {
	var count int
	err := e.journal.db.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_local_sessions s JOIN e2ee_local_grants g ON g.session=s.session WHERE s.session=? AND s.epoch=1 AND s.key_id=? AND g.device=? AND g.verify_key=? AND g.control=1`, request.Session, request.KeyID, request.Device, public).Scan(&count)
	if err != nil {
		return ErrJournal
	}
	if count != 1 {
		return ErrConflict
	}
	return nil
}
