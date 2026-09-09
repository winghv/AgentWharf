package e2ee

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"github.com/cloudflare/circl/hpke"
)

var wrappingSuite = hpke.NewSuite(hpke.KEM_P256_HKDF_SHA256, hpke.KDF_HKDF_SHA256, hpke.AEAD_AES256GCM)

// WrapContext is constructed from authenticated local membership, not relay data.
type WrapContext struct {
	Session   string
	KeyID     string
	Sender    string
	Recipient string
}

type WrappedKey struct {
	Enc        string `json:"enc"`
	Ciphertext string `json:"ciphertext"`
}

func (c WrapContext) bytes() ([]byte, error) {
	values := []string{"agentwharf.e2ee.keywrap.v1", c.Session, c.KeyID, c.Sender, c.Recipient}
	for _, value := range values[1:] {
		if !identifier.MatchString(value) {
			return nil, ErrInvalid
		}
	}
	return json.Marshal(values)
}

// GenerateWrappingKey returns the RFC 9180 P-256 serialized private/public keys.
func GenerateWrappingKey() (private, public []byte, err error) {
	pk, sk, err := hpke.KEM_P256_HKDF_SHA256.Scheme().GenerateKeyPair()
	if err != nil {
		return nil, nil, ErrInvalid
	}
	private, err = sk.MarshalBinary()
	if err != nil {
		return nil, nil, ErrInvalid
	}
	public, err = pk.MarshalBinary()
	if err != nil {
		return nil, nil, ErrInvalid
	}
	return private, public, nil
}

func wrappingPrivatePublic(private []byte) ([]byte, error) {
	sk, err := hpke.KEM_P256_HKDF_SHA256.Scheme().UnmarshalBinaryPrivateKey(private)
	if err != nil {
		return nil, ErrInvalid
	}
	public, err := sk.Public().MarshalBinary()
	if err != nil {
		return nil, ErrInvalid
	}
	return public, nil
}

// WrapKey uses HPKE Auth mode: recipient encryption and sender authentication.
// It wraps exactly one 32-byte content key, never arbitrary user payloads.
func WrapKey(ctx WrapContext, senderPrivate, recipientPublic, key []byte) (WrappedKey, error) {
	if len(key) != 32 || len(senderPrivate) != 32 || len(recipientPublic) != 65 {
		return WrappedKey{}, ErrInvalid
	}
	aad, err := ctx.bytes()
	if err != nil {
		return WrappedKey{}, err
	}
	scheme := hpke.KEM_P256_HKDF_SHA256.Scheme()
	sk, err := scheme.UnmarshalBinaryPrivateKey(senderPrivate)
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	pk, err := scheme.UnmarshalBinaryPublicKey(recipientPublic)
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	info := sha256.Sum256(aad)
	sender, err := wrappingSuite.NewSender(pk, info[:])
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	enc, sealer, err := sender.SetupAuth(rand.Reader, sk)
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	ciphertext, err := sealer.Seal(key, aad)
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	return WrappedKey{base64.RawURLEncoding.EncodeToString(enc), base64.RawURLEncoding.EncodeToString(ciphertext)}, nil
}

func UnwrapKey(ctx WrapContext, recipientPrivate, senderPublic []byte, wrapped WrappedKey) ([]byte, error) {
	if len(recipientPrivate) != 32 || len(senderPublic) != 65 {
		return nil, ErrInvalid
	}
	aad, err := ctx.bytes()
	if err != nil {
		return nil, err
	}
	enc, err := decode(wrapped.Enc, 65, 65)
	if err != nil {
		return nil, err
	}
	ciphertext, err := decode(wrapped.Ciphertext, 48, 48)
	if err != nil {
		return nil, err
	}
	scheme := hpke.KEM_P256_HKDF_SHA256.Scheme()
	sk, err := scheme.UnmarshalBinaryPrivateKey(recipientPrivate)
	if err != nil {
		return nil, ErrInvalid
	}
	pk, err := scheme.UnmarshalBinaryPublicKey(senderPublic)
	if err != nil {
		return nil, ErrInvalid
	}
	info := sha256.Sum256(aad)
	receiver, err := wrappingSuite.NewReceiver(sk, info[:])
	if err != nil {
		return nil, ErrInvalid
	}
	opener, err := receiver.SetupAuth(enc, pk)
	if err != nil {
		return nil, ErrInvalid
	}
	key, err := opener.Open(ciphertext, aad)
	if err != nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	return key, nil
}
