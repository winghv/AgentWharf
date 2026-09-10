package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
)

// TrustedSessionKeyRequest is the account-terminal-trust variant of
// SessionKeyRequest. It carries the recipient's public keys so a machine under
// "trust account terminals" can enroll the device without a local offer. The
// self-signature only proves key possession; delivery must still arrive over an
// account-scoped, machine-authenticated relay.
type TrustedSessionKeyRequest struct {
	Machine     string `json:"machine"`
	Account     string `json:"account"`
	Session     string `json:"session"`
	KeyID       string `json:"key_id"`
	Device      string `json:"device"`
	SigningKey  string `json:"signing_key"`
	WrappingKey string `json:"wrapping_key"`
	Signature   string `json:"signature"`
}

func (r TrustedSessionKeyRequest) signingBytes() ([]byte, error) {
	for _, value := range []string{r.Machine, r.Account, r.Session, r.KeyID, r.Device, r.SigningKey, r.WrappingKey} {
		if !identifier.MatchString(value) {
			return nil, ErrInvalid
		}
	}
	return json.Marshal([]string{"agentwharf.e2ee.key-request.v2", r.Machine, r.Account, r.Session, r.KeyID, r.Device, r.SigningKey, r.WrappingKey})
}

// SignTrustedSessionKeyRequest signs a v2 request with the recipient identity.
func SignTrustedSessionKeyRequest(r TrustedSessionKeyRequest, key ed25519.PrivateKey) (TrustedSessionKeyRequest, error) {
	data, err := r.signingBytes()
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return TrustedSessionKeyRequest{}, ErrInvalid
	}
	r.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, data))
	return r, nil
}

// DecodeTrustedSessionKeyRequest rejects ambiguous JSON before endpoint use.
func DecodeTrustedSessionKeyRequest(data []byte) (TrustedSessionKeyRequest, error) {
	if len(data) > 4096 {
		return TrustedSessionKeyRequest{}, ErrInvalid
	}
	fields, err := packetFields(data)
	if err != nil || len(fields) != 8 {
		return TrustedSessionKeyRequest{}, ErrInvalid
	}
	var request TrustedSessionKeyRequest
	targets := map[string]*string{"machine": &request.Machine, "account": &request.Account, "session": &request.Session, "key_id": &request.KeyID, "device": &request.Device, "signing_key": &request.SigningKey, "wrapping_key": &request.WrappingKey, "signature": &request.Signature}
	for name, target := range targets {
		if fields[name] == nil || json.Unmarshal(fields[name], target) != nil {
			return TrustedSessionKeyRequest{}, ErrInvalid
		}
	}
	if _, err := request.signingBytes(); err != nil {
		return TrustedSessionKeyRequest{}, err
	}
	if _, err := decode(request.SigningKey, 32, 32); err != nil {
		return TrustedSessionKeyRequest{}, err
	}
	if _, err := decode(request.WrappingKey, 65, 65); err != nil {
		return TrustedSessionKeyRequest{}, err
	}
	if _, err := decode(request.Signature, 64, 64); err != nil {
		return TrustedSessionKeyRequest{}, err
	}
	return request, nil
}

// Identity returns the recipient public identity carried by the request.
func (r TrustedSessionKeyRequest) Identity() PairingIdentity {
	return PairingIdentity{Device: r.Device, SigningKey: r.SigningKey, WrappingKey: r.WrappingKey}
}

// RecoverSessionKeyTrusted verifies a v2 request against its embedded signing
// key, optionally enrolls the device when account-terminal trust is enabled,
// seeds a view grant for every existing session, and returns the wrapped key.
func (e *CommandExecutor) RecoverSessionKeyTrusted(ctx context.Context, registry *DeviceRegistry, request TrustedSessionKeyRequest, trustEnabled bool) (WrappedKey, error) {
	// The account label is local to the machine binding; a new terminal that
	// never consumed an offer cannot know it. The relay is already scoped to the
	// machine owner, so only the machine identity has to match here.
	if registry == nil || registry.db != e.vault.db || request.Machine != registry.machine {
		return WrappedKey{}, ErrUnauthorized
	}
	data, err := request.signingBytes()
	if err != nil {
		return WrappedKey{}, err
	}
	public, err := decode(request.SigningKey, 32, 32)
	if err != nil {
		return WrappedKey{}, err
	}
	signature, err := decode(request.Signature, 64, 64)
	if err != nil || !ed25519.Verify(public, data, signature) {
		return WrappedKey{}, ErrUnauthorized
	}
	identity := request.Identity()
	if validatePairingIdentity(identity) != nil {
		return WrappedKey{}, ErrInvalid
	}
	if err := e.acquire(ctx); err != nil {
		return WrappedKey{}, err
	}
	defer func() { <-e.lane }()
	registered, err := registry.Device(ctx, request.Device)
	if err != nil {
		if !trustEnabled {
			return WrappedKey{}, ErrUnauthorized
		}
		if err := registry.EnrollTrusted(ctx, identity); err != nil {
			return WrappedKey{}, err
		}
	} else if registered.SigningKey != identity.SigningKey || registered.WrappingKey != identity.WrappingKey {
		return WrappedKey{}, ErrConflict
	}
	if _, err := e.journal.GrantEnrolledDevice(ctx, request.Device, ed25519.PublicKey(public), false); err != nil {
		return WrappedKey{}, err
	}
	return e.vault.WrapForDevice(ctx, request.Session, request.KeyID, registry, request.Device)
}
