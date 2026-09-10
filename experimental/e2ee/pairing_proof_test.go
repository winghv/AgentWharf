package e2ee

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudflare/circl/hpke"
)

func TestPairingRequiresPrivateKeyPossessionBeforeCommit(t *testing.T) {
	now := time.Now()
	invitation, offer, err := NewPairingInvitation("machine", now)
	if err != nil {
		t.Fatal(err)
	}
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, attacker, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, wrapPublic, err := GenerateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	identity := PairingIdentity{"device", base64.RawURLEncoding.EncodeToString(public), base64.RawURLEncoding.EncodeToString(wrapPublic)}
	if _, err := EncryptPairingIdentity(offer, identity, attacker, now); err == nil {
		t.Fatal("client accepted wrong signing key")
	}
	// A party possessing the invitation secret can construct its own valid HPKE
	// request. The endpoint must independently reject its false ownership proof.
	aad, pub, secret, err := pairingContext(offer, now)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := hpke.KEM_P256_HKDF_SHA256.Scheme().UnmarshalBinaryPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	info := sha256.Sum256(aad)
	sender, err := wrappingSuite.NewSender(pk, info[:])
	if err != nil {
		t.Fatal(err)
	}
	enc, sealer, err := sender.SetupPSK(rand.Reader, secret, []byte(offer.ID))
	if err != nil {
		t.Fatal(err)
	}
	proof := ed25519.Sign(attacker, pairingProofBytes(aad, identity))
	plaintext, err := json.Marshal(pairingProof{identity, base64.RawURLEncoding.EncodeToString(proof)})
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := sealer.Seal(plaintext, aad)
	if err != nil {
		t.Fatal(err)
	}
	request := WrappedKey{base64.RawURLEncoding.EncodeToString(enc), base64.RawURLEncoding.EncodeToString(ciphertext)}
	receipt, err := invitation.AcceptCommitted(request, now, func(PairingIdentity) error { t.Fatal("unproved identity reached enrollment"); return nil })
	if err == nil || receipt.Confirmation != "" {
		t.Fatal("endpoint accepted false ownership proof")
	}
}
