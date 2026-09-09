package protocol

import (
	"encoding/json"
	"errors"
)

var ErrEncryptedPairing = errors.New("invalid encrypted pairing relay payload")

// EncryptedPairingRequest contains no local invitation secret or device private
// key. Validation is structural only; enrollment remains an endpoint operation.
type EncryptedPairingRequest struct {
	Enc        string `json:"enc"`
	Ciphertext string `json:"ciphertext"`
}

func DecodeEncryptedPairingRequest(data []byte) (EncryptedPairingRequest, error) {
	if len(data) > 2048 {
		return EncryptedPairingRequest{}, ErrEncryptedPairing
	}
	if _, err := encryptedObject(data, "enc", "ciphertext"); err != nil {
		return EncryptedPairingRequest{}, ErrEncryptedPairing
	}
	var request EncryptedPairingRequest
	if json.Unmarshal(data, &request) != nil || !validBase64URL(request.Enc, 65, 65) || !validBase64URL(request.Ciphertext, 16, 1024) {
		return EncryptedPairingRequest{}, ErrEncryptedPairing
	}
	return request, nil
}
