package e2ee

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
)

// LocalIdentity is private endpoint material. Never serialize into protocol,
// platform requests, Provider environment, logs or diagnostics.
type LocalIdentity struct {
	Version         int    `json:"version"`
	Device          string `json:"device"`
	SigningSeed     []byte `json:"signing_seed"`
	WrappingPrivate []byte `json:"wrapping_private"`
}

func NewLocalIdentity() (LocalIdentity, error) {
	id := make([]byte, 16)
	seed := make([]byte, 32)
	if _, err := rand.Read(id); err != nil {
		return LocalIdentity{}, ErrInvalid
	}
	if _, err := rand.Read(seed); err != nil {
		return LocalIdentity{}, ErrInvalid
	}
	private, _, err := GenerateWrappingKey()
	if err != nil {
		return LocalIdentity{}, err
	}
	return LocalIdentity{1, "device_" + base64.RawURLEncoding.EncodeToString(id), seed, private}, nil
}

func (identity LocalIdentity) Public() (PairingIdentity, error) {
	if identity.Version != 1 || !identifier.MatchString(identity.Device) || len(identity.SigningSeed) != 32 || len(identity.WrappingPrivate) != 32 {
		return PairingIdentity{}, ErrInvalid
	}
	wrapping, err := wrappingPrivatePublic(identity.WrappingPrivate)
	if err != nil {
		return PairingIdentity{}, err
	}
	signing := ed25519.NewKeyFromSeed(identity.SigningSeed)
	defer clear(signing)
	return PairingIdentity{identity.Device, base64.RawURLEncoding.EncodeToString(signing.Public().(ed25519.PublicKey)), base64.RawURLEncoding.EncodeToString(wrapping)}, nil
}

func decodeLocalIdentity(data []byte) (LocalIdentity, error) {
	var identity LocalIdentity
	if len(data) > 2048 {
		return identity, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&identity) != nil {
		return LocalIdentity{}, ErrInvalid
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return LocalIdentity{}, ErrInvalid
	}
	canonical, err := json.Marshal(identity)
	if err != nil || !bytes.Equal(canonical, data) {
		return LocalIdentity{}, ErrInvalid
	}
	if _, err := identity.Public(); err != nil {
		return LocalIdentity{}, err
	}
	return identity, nil
}
