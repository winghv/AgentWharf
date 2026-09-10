package protocol

import "encoding/json"

// TrustedSessionKeyRequest is the account-terminal-trust v2 key request carrier.
// It carries the recipient public keys; only the endpoint verifies the
// self-signature and decides whether account-terminal trust permits enrollment.
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

// DecodeTrustedSessionKeyRequest validates routing, key encodings and the exact
// field set. It does not verify the signature or authorize enrollment.
func DecodeTrustedSessionKeyRequest(data []byte, machine, session, device, keyID string) (TrustedSessionKeyRequest, error) {
	var request TrustedSessionKeyRequest
	if len(data) > 4096 || !validProtocolIdentifier(machine) || !validProtocolIdentifier(session) || !validProtocolIdentifier(device) || !validProtocolIdentifier(keyID) {
		return request, ErrSessionKeyRequest
	}
	if _, err := encryptedObject(data, "machine", "account", "session", "key_id", "device", "signing_key", "wrapping_key", "signature"); err != nil {
		return request, ErrSessionKeyRequest
	}
	if json.Unmarshal(data, &request) != nil || request.Machine != machine || request.Session != session || request.Device != device || request.KeyID != keyID || !validProtocolIdentifier(request.Account) || !validBase64URL(request.SigningKey, 32, 32) || !validBase64URL(request.WrappingKey, 65, 65) || !validBase64URL(request.Signature, 64, 64) {
		return TrustedSessionKeyRequest{}, ErrSessionKeyRequest
	}
	return request, nil
}
