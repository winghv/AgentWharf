package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"time"
)

// SessionKeyRequest proves recipient possession independently of relay auth.
// Unlike initialization it cannot create a session or grant any membership.
type SessionKeyRequest struct {
	Machine   string `json:"machine"`
	Account   string `json:"account"`
	Session   string `json:"session"`
	KeyID     string `json:"key_id"`
	Device    string `json:"device"`
	Signature string `json:"signature"`
}

func (r SessionKeyRequest) signingBytes() ([]byte, error) {
	for _, value := range []string{r.Machine, r.Account, r.Session, r.KeyID, r.Device} {
		if !identifier.MatchString(value) {
			return nil, ErrInvalid
		}
	}
	return json.Marshal([]string{"agentwharf.e2ee.key-request.v1", r.Machine, r.Account, r.Session, r.KeyID, r.Device})
}
func SignSessionKeyRequest(r SessionKeyRequest, key ed25519.PrivateKey) (SessionKeyRequest, error) {
	data, err := r.signingBytes()
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return SessionKeyRequest{}, ErrInvalid
	}
	r.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, data))
	return r, nil
}
func DecodeSessionKeyRequest(data []byte) (SessionKeyRequest, error) {
	if len(data) > 2048 {
		return SessionKeyRequest{}, ErrInvalid
	}
	fields, err := packetFields(data)
	if err != nil || len(fields) != 6 {
		return SessionKeyRequest{}, ErrInvalid
	}
	var request SessionKeyRequest
	for name, target := range map[string]*string{"machine": &request.Machine, "account": &request.Account, "session": &request.Session, "key_id": &request.KeyID, "device": &request.Device, "signature": &request.Signature} {
		if fields[name] == nil || json.Unmarshal(fields[name], target) != nil {
			return SessionKeyRequest{}, ErrInvalid
		}
	}
	if _, err := request.signingBytes(); err != nil {
		return SessionKeyRequest{}, err
	}
	if _, err := decode(request.Signature, 64, 64); err != nil {
		return SessionKeyRequest{}, err
	}
	return request, nil
}
func (e *CommandExecutor) RecoverSessionKey(ctx context.Context, registry *DeviceRegistry, request SessionKeyRequest) (WrappedKey, error) {
	if registry == nil || registry.db != e.vault.db || request.Machine != registry.machine || request.Account != registry.account {
		return WrappedKey{}, ErrUnauthorized
	}
	data, err := request.signingBytes()
	if err != nil {
		return WrappedKey{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	device, err := registry.Device(ctx, request.Device)
	if err != nil {
		return WrappedKey{}, ErrUnauthorized
	}
	public, err := decode(device.SigningKey, 32, 32)
	if err != nil {
		return WrappedKey{}, err
	}
	signature, err := decode(request.Signature, 64, 64)
	if err != nil || !ed25519.Verify(public, data, signature) {
		return WrappedKey{}, ErrUnauthorized
	}
	if err := e.acquire(ctx); err != nil {
		return WrappedKey{}, err
	}
	defer func() { <-e.lane }()
	return e.vault.WrapForDevice(ctx, request.Session, request.KeyID, registry, request.Device)
}
