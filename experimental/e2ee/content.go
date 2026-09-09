// Package e2ee is an isolated content-cryptography prototype, not a Hub capability.
package e2ee

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"regexp"
)

const MaxContentBytes = 32768

var ErrInvalid = errors.New("invalid encrypted content")
var identifier = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,128}$`)

type Context struct {
	Scope     string
	Session   string
	Sender    string
	KeyID     string
	MessageID string
	Type      string
}

type Envelope struct {
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
	Signature  string `json:"signature"`
}

// DecodeEnvelope rejects duplicate and unknown members before cryptographic use.
func DecodeEnvelope(data []byte) (Envelope, error) {
	if len(data) > 48*1024 {
		return Envelope{}, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return Envelope{}, ErrInvalid
	}
	fields := make(map[string]string, 3)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return Envelope{}, ErrInvalid
		}
		name, ok := token.(string)
		if !ok || (name != "nonce" && name != "ciphertext" && name != "signature") {
			return Envelope{}, ErrInvalid
		}
		if _, exists := fields[name]; exists {
			return Envelope{}, ErrInvalid
		}
		var value string
		if decoder.Decode(&value) != nil {
			return Envelope{}, ErrInvalid
		}
		fields[name] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || len(fields) != 3 {
		return Envelope{}, ErrInvalid
	}
	if _, err = decoder.Token(); err != io.EOF {
		return Envelope{}, ErrInvalid
	}
	envelope := Envelope{fields["nonce"], fields["ciphertext"], fields["signature"]}
	if _, err = decode(envelope.Nonce, 12, 12); err != nil {
		return Envelope{}, err
	}
	if _, err = decode(envelope.Ciphertext, 16, MaxContentBytes+16); err != nil {
		return Envelope{}, err
	}
	if _, err = decode(envelope.Signature, 64, 64); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func (c Context) bytes() ([]byte, error) {
	if c.Scope != "command" && c.Scope != "event" && c.Scope != "launch" {
		return nil, ErrInvalid
	}
	values := []string{"agentwharf.e2ee.prototype.v1", c.Scope, c.Session, c.Sender, c.KeyID, c.MessageID, c.Type}
	for _, value := range values[1:] {
		if !identifier.MatchString(value) {
			return nil, ErrInvalid
		}
	}
	return json.Marshal(values)
}

func gcm(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalid
	}
	return cipher.NewGCM(block)
}

func decode(value string, min, max int) ([]byte, error) {
	if len(value) > base64.RawURLEncoding.EncodedLen(max) {
		return nil, ErrInvalid
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(data) < min || len(data) > max || base64.RawURLEncoding.EncodeToString(data) != value {
		return nil, ErrInvalid
	}
	return data, nil
}

func Seal(ctx Context, key []byte, signingKey ed25519.PrivateKey, plaintext []byte) (Envelope, error) {
	if len(plaintext) > MaxContentBytes || len(signingKey) != ed25519.PrivateKeySize {
		return Envelope{}, ErrInvalid
	}
	aad, err := ctx.bytes()
	if err != nil {
		return Envelope{}, err
	}
	aead, err := gcm(key)
	if err != nil {
		return Envelope{}, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return Envelope{}, err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, aad)
	signed := append(append(aad, nonce...), ciphertext...)
	return Envelope{base64.RawURLEncoding.EncodeToString(nonce), base64.RawURLEncoding.EncodeToString(ciphertext), base64.RawURLEncoding.EncodeToString(ed25519.Sign(signingKey, signed))}, nil
}

func Open(ctx Context, key []byte, verifyKey ed25519.PublicKey, envelope Envelope) ([]byte, error) {
	if len(verifyKey) != ed25519.PublicKeySize {
		return nil, ErrInvalid
	}
	aad, err := ctx.bytes()
	if err != nil {
		return nil, err
	}
	nonce, err := decode(envelope.Nonce, 12, 12)
	if err != nil {
		return nil, err
	}
	ciphertext, err := decode(envelope.Ciphertext, 16, MaxContentBytes+16)
	if err != nil {
		return nil, err
	}
	signature, err := decode(envelope.Signature, 64, 64)
	if err != nil {
		return nil, err
	}
	signed := append(append(append([]byte(nil), aad...), nonce...), ciphertext...)
	if !ed25519.Verify(verifyKey, signed, signature) {
		return nil, ErrInvalid
	}
	aead, err := gcm(key)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrInvalid
	}
	return plaintext, nil
}
