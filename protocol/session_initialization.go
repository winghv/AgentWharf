package protocol

import (
	"encoding/json"
	"errors"
)

var ErrSessionInitialization = errors.New("invalid signed session initialization")
var ErrSessionKeyRequest = errors.New("invalid signed session key request")

// SessionInitialization is a relay carrier, not proof of platform authorization.
// Only an endpoint's paired-device registry can authenticate its signature.
type SessionInitialization struct {
	Machine   string `json:"machine"`
	Account   string `json:"account"`
	Session   string `json:"session"`
	KeyID     string `json:"key_id"`
	Device    string `json:"device"`
	Signature string `json:"signature"`
}

// DecodeSessionKeyResponse validates only the HPKE carrier, not sender trust.
func DecodeSessionKeyRequest(data []byte, machine, session, device, keyID string) (SessionInitialization, error) {
	var request SessionInitialization
	if len(data) > 2048 || !validProtocolIdentifier(machine) || !validProtocolIdentifier(session) || !validProtocolIdentifier(device) || !validProtocolIdentifier(keyID) {
		return request, ErrSessionKeyRequest
	}
	if _, err := encryptedObject(data, "machine", "account", "session", "key_id", "device", "signature"); err != nil {
		return request, ErrSessionKeyRequest
	}
	if json.Unmarshal(data, &request) != nil || request.Machine != machine || request.Session != session || request.Device != device || request.KeyID != keyID || !validProtocolIdentifier(request.Account) || !validBase64URL(request.Signature, 64, 64) {
		return SessionInitialization{}, ErrSessionKeyRequest
	}
	raw, err := json.Marshal([]string{"agentwharf.e2ee.key-request.v1", request.Machine, request.Account, request.Session, request.KeyID, request.Device})
	if err != nil {
		return SessionInitialization{}, ErrSessionKeyRequest
	}
	_ = raw
	return request, nil
}

func DecodeSessionKeyResponse(data []byte) (EncryptedPairingRequest, error) {
	if len(data) > 1024 {
		return EncryptedPairingRequest{}, ErrSessionInitialization
	}
	response, err := DecodeEncryptedPairingRequest(data)
	if err != nil || !validBase64URL(response.Ciphertext, 48, 48) {
		return EncryptedPairingRequest{}, ErrSessionInitialization
	}
	return response, nil
}

func DecodeSessionInitialization(data []byte, machine, session string) (SessionInitialization, error) {
	var request SessionInitialization
	if len(data) > 2048 || !validProtocolIdentifier(machine) || !validProtocolIdentifier(session) {
		return request, ErrSessionInitialization
	}
	if _, err := encryptedObject(data, "machine", "account", "session", "key_id", "device", "signature"); err != nil {
		return request, ErrSessionInitialization
	}
	if json.Unmarshal(data, &request) != nil {
		return SessionInitialization{}, ErrSessionInitialization
	}
	if request.Machine != machine || request.Session != session || !validProtocolIdentifier(request.Account) || !validProtocolIdentifier(request.KeyID) || !validProtocolIdentifier(request.Device) || !validBase64URL(request.Signature, 64, 64) {
		return SessionInitialization{}, ErrSessionInitialization
	}
	return request, nil
}
