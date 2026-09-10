package e2ee

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"

	"github.com/cloudflare/circl/hpke"
)

// PairingOffer is local-only: never send Secret to the platform. Its complete
// serialized value is conveyed by the user's existing scan/paste pairing action.
type PairingOffer struct {
	Version   int    `json:"version"`
	ID        string `json:"id"`
	Machine   string `json:"machine"`
	PublicKey string `json:"public_key"`
	Secret    string `json:"secret"`
	ExpiresAt int64  `json:"expires_at"`
}

type PairingIdentity struct {
	Device      string `json:"device"`
	SigningKey  string `json:"signing_key"`
	WrappingKey string `json:"wrapping_key"`
}

// PairingInvitation is deliberately process-local; a restart cancels the offer.
// Production enrollment must atomically commit identity + consumed invitation.
type PairingInvitation struct {
	mu              sync.Mutex
	offer           PairingOffer
	private         []byte
	consumed        bool
	acceptedRequest WrappedKey
	acceptedReceipt PairingReceipt
}

func NewPairingInvitation(machine string, now time.Time) (*PairingInvitation, PairingOffer, error) {
	if !identifier.MatchString(machine) {
		return nil, PairingOffer{}, ErrInvalid
	}
	private, public, err := GenerateWrappingKey()
	if err != nil {
		return nil, PairingOffer{}, err
	}
	secret := make([]byte, 32)
	id := make([]byte, 16)
	if _, err := rand.Read(secret); err != nil {
		return nil, PairingOffer{}, err
	}
	if _, err := rand.Read(id); err != nil {
		return nil, PairingOffer{}, err
	}
	offer := PairingOffer{1, base64.RawURLEncoding.EncodeToString(id), machine, base64.RawURLEncoding.EncodeToString(public), base64.RawURLEncoding.EncodeToString(secret), now.Add(5 * time.Minute).UnixMilli()}
	return &PairingInvitation{offer: offer, private: private}, offer, nil
}

func pairingContext(offer PairingOffer, now time.Time) ([]byte, []byte, []byte, error) {
	if offer.Version != 1 || !identifier.MatchString(offer.Machine) || offer.ExpiresAt <= now.UnixMilli() || offer.ExpiresAt > now.Add(5*time.Minute).UnixMilli() {
		return nil, nil, nil, ErrInvalid
	}
	if _, err := decode(offer.ID, 16, 16); err != nil {
		return nil, nil, nil, err
	}
	public, err := decode(offer.PublicKey, 65, 65)
	if err != nil {
		return nil, nil, nil, err
	}
	secret, err := decode(offer.Secret, 32, 32)
	if err != nil {
		return nil, nil, nil, err
	}
	aad, err := json.Marshal([]any{"agentwharf.e2ee.pair.v1", offer.ID, offer.Machine, offer.PublicKey, offer.ExpiresAt})
	return aad, public, secret, err
}

func validatePairingIdentity(identity PairingIdentity) error {
	if !identifier.MatchString(identity.Device) {
		return ErrInvalid
	}
	if _, err := decode(identity.SigningKey, 32, 32); err != nil {
		return err
	}
	public, err := decode(identity.WrappingKey, 65, 65)
	if err != nil {
		return err
	}
	if _, err := hpke.KEM_P256_HKDF_SHA256.Scheme().UnmarshalBinaryPublicKey(public); err != nil {
		return ErrInvalid
	}
	return nil
}

// EncryptPairingIdentity proves possession of the out-of-band 256-bit secret.
// Account/machine authorization is separate and still required at enrollment.
type pairingProof struct {
	Identity PairingIdentity `json:"identity"`
	Proof    string          `json:"proof"`
}

func pairingProofBytes(aad []byte, identity PairingIdentity) []byte {
	canonical, _ := json.Marshal(identity)
	return append(append(append([]byte("agentwharf.e2ee.pair.proof.v1\x00"), aad...), 0), canonical...)
}

func EncryptPairingIdentity(offer PairingOffer, identity PairingIdentity, signingKey ed25519.PrivateKey, now time.Time) (WrappedKey, error) {
	if err := validatePairingIdentity(identity); err != nil {
		return WrappedKey{}, err
	}
	aad, public, secret, err := pairingContext(offer, now)
	if err != nil {
		return WrappedKey{}, err
	}
	pk, err := hpke.KEM_P256_HKDF_SHA256.Scheme().UnmarshalBinaryPublicKey(public)
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	info := sha256.Sum256(aad)
	sender, err := wrappingSuite.NewSender(pk, info[:])
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	enc, sealer, err := sender.SetupPSK(rand.Reader, secret, []byte(offer.ID))
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	signingPublic, err := decode(identity.SigningKey, 32, 32)
	if err != nil || len(signingKey) != ed25519.PrivateKeySize {
		return WrappedKey{}, ErrInvalid
	}
	proof := ed25519.Sign(signingKey, pairingProofBytes(aad, identity))
	if !ed25519.Verify(signingPublic, pairingProofBytes(aad, identity), proof) {
		return WrappedKey{}, ErrInvalid
	}
	plaintext, err := json.Marshal(pairingProof{identity, base64.RawURLEncoding.EncodeToString(proof)})
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	ciphertext, err := sealer.Seal(plaintext, aad)
	if err != nil {
		return WrappedKey{}, ErrInvalid
	}
	return WrappedKey{base64.RawURLEncoding.EncodeToString(enc), base64.RawURLEncoding.EncodeToString(ciphertext)}, nil
}

type PairingReceipt struct {
	Confirmation string `json:"confirmation"`
}

// Accept only tests decryption. Production enrollment uses AcceptCommitted.
func (inv *PairingInvitation) Accept(request WrappedKey, now time.Time) (PairingIdentity, error) {
	var accepted PairingIdentity
	_, err := inv.AcceptCommitted(request, now, func(identity PairingIdentity) error { accepted = identity; return nil })
	if err == nil && accepted.Device == "" {
		return PairingIdentity{}, ErrInvalid
	}
	return accepted, err
}

// AcceptCommitted returns a receipt only after the caller durably commits the
// identity under its independently verified local account/machine authorization.
// The commit callback must be bounded and idempotent by invitation ID. An
// ambiguous commit error fences this invitation; retry cannot enroll a new peer.
func (inv *PairingInvitation) AcceptCommitted(request WrappedKey, now time.Time, commit func(PairingIdentity) error) (PairingReceipt, error) {
	if commit == nil {
		return PairingReceipt{}, ErrInvalid
	}
	return inv.acceptEnrollment(request, now, func(identity PairingIdentity, _ PairingReceipt) error { return commit(identity) })
}

func (inv *PairingInvitation) acceptEnrollment(request WrappedKey, now time.Time, commit func(PairingIdentity, PairingReceipt) error) (PairingReceipt, error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if commit == nil || now.UnixMilli() >= inv.offer.ExpiresAt {
		return PairingReceipt{}, ErrInvalid
	}
	if inv.consumed {
		if request == inv.acceptedRequest && inv.acceptedReceipt.Confirmation != "" {
			return inv.acceptedReceipt, nil
		}
		return PairingReceipt{}, ErrInvalid
	}
	aad, _, secret, err := pairingContext(inv.offer, now)
	if err != nil {
		return PairingReceipt{}, err
	}
	enc, err := decode(request.Enc, 65, 65)
	if err != nil {
		return PairingReceipt{}, err
	}
	ciphertext, err := decode(request.Ciphertext, 16, 1024)
	if err != nil {
		return PairingReceipt{}, err
	}
	sk, err := hpke.KEM_P256_HKDF_SHA256.Scheme().UnmarshalBinaryPrivateKey(inv.private)
	if err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	info := sha256.Sum256(aad)
	receiver, err := wrappingSuite.NewReceiver(sk, info[:])
	if err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	opener, err := receiver.SetupPSK(enc, secret, []byte(inv.offer.ID))
	if err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	plaintext, err := opener.Open(ciphertext, aad)
	if err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	var proved pairingProof
	if json.Unmarshal(plaintext, &proved) != nil || validatePairingIdentity(proved.Identity) != nil {
		return PairingReceipt{}, ErrInvalid
	}
	identity := proved.Identity
	proof, err := decode(proved.Proof, 64, 64)
	if err != nil {
		return PairingReceipt{}, ErrInvalid
	}
	public, err := decode(identity.SigningKey, 32, 32)
	if err != nil || !ed25519.Verify(public, pairingProofBytes(aad, identity), proof) {
		return PairingReceipt{}, ErrInvalid
	}
	// Re-encoding ensures exact grammar, no duplicate/unknown members or alternate
	// encoding. The sender uses the same canonical Go/TS field order.
	canonicalProof, _ := json.Marshal(proved)
	if string(canonicalProof) != string(plaintext) {
		return PairingReceipt{}, ErrInvalid
	}
	confirmationKey := opener.Export([]byte("agentwharf.e2ee.pair.confirm.v1"), 32)
	defer clear(confirmationKey)
	mac := hmac.New(sha256.New, confirmationKey)
	canonical, _ := json.Marshal(identity)
	_, _ = mac.Write(canonical)
	receipt := PairingReceipt{Confirmation: base64.RawURLEncoding.EncodeToString(mac.Sum(nil))}
	inv.consumed = true
	clear(inv.private)
	inv.offer.Secret = ""
	if err := commit(identity, receipt); err != nil {
		return PairingReceipt{}, ErrJournal
	}
	inv.acceptedRequest = request
	inv.acceptedReceipt = receipt
	return receipt, nil
}
