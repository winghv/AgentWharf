package e2ee

import (
	"context"
	"crypto/ed25519"
	"time"
)

// InitializeSessionTrusted is the new-session counterpart of
// RecoverSessionKeyTrusted. A terminal that never consumed a local machine
// offer authenticates a v2 request against its embedded signing key, is
// optionally enrolled under account-terminal trust, becomes the session's
// creator grant, and receives the wrapped session key for its requested key id.
func (e *CommandExecutor) InitializeSessionTrusted(ctx context.Context, registry *DeviceRegistry, request TrustedSessionKeyRequest, trustEnabled, control bool) (WrappedKey, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if registry == nil || registry.db != e.vault.db || request.Machine != registry.machine {
		return WrappedKey{}, ErrUnauthorized
	}
	data, err := request.signingBytes()
	if err != nil {
		return WrappedKey{}, err
	}
	public, err := decode(request.SigningKey, ed25519.PublicKeySize, ed25519.PublicKeySize)
	if err != nil {
		return WrappedKey{}, err
	}
	signature, err := decode(request.Signature, ed25519.SignatureSize, ed25519.SignatureSize)
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
	grants, err := registry.sessionSeedGrants(ctx, request.Device, ed25519.PublicKey(public))
	if err != nil {
		return WrappedKey{}, err
	}
	for index := range grants {
		if grants[index].DeviceID == request.Device {
			grants[index].Control = control
		}
	}
	err = e.vault.TransitionSession(ctx, e.journal, request.Session, request.KeyID, 0, grants)
	if err != nil && err != ErrConflict {
		return WrappedKey{}, err
	}
	if err == ErrConflict {
		// The endpoint can create the session key before this initialization
		// arrives. Grant this device on the existing session and wrap its key.
		if _, err := e.journal.GrantEnrolledDevice(ctx, request.Device, ed25519.PublicKey(public), control); err != nil {
			return WrappedKey{}, err
		}
	}
	return e.vault.WrapForDevice(ctx, request.Session, request.KeyID, registry, request.Device)
}

// VerifyInitializedSessionTrusted checks a completed v2 initialization without
// creating keys, enrolling the device, or accepting a rotated session.
func (e *CommandExecutor) VerifyInitializedSessionTrusted(ctx context.Context, registry *DeviceRegistry, request TrustedSessionKeyRequest) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if registry == nil || registry.db != e.vault.db || request.Machine != registry.machine {
		return ErrUnauthorized
	}
	data, err := request.signingBytes()
	if err != nil {
		return err
	}
	public, err := decode(request.SigningKey, ed25519.PublicKeySize, ed25519.PublicKeySize)
	if err != nil {
		return err
	}
	signature, err := decode(request.Signature, ed25519.SignatureSize, ed25519.SignatureSize)
	if err != nil || !ed25519.Verify(public, data, signature) {
		return ErrUnauthorized
	}
	var count int
	err = e.journal.db.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_local_sessions s JOIN e2ee_local_grants g ON g.session=s.session WHERE s.session=? AND s.epoch=1 AND s.key_id=? AND g.device=? AND g.verify_key=?`, request.Session, request.KeyID, request.Device, []byte(public)).Scan(&count)
	if err != nil {
		return ErrJournal
	}
	if count != 1 {
		return ErrConflict
	}
	return nil
}
