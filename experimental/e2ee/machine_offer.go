package e2ee

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"time"
)

// MachineOffer is a local scan artifact, not a relay payload. Its signature
// binds the ephemeral invitation to the endpoint identity for later event and
// HPKE verification. Trust comes from the local scan, not this self-signature.
type MachineOffer struct {
	Offer    PairingOffer    `json:"offer"`
	Identity PairingIdentity `json:"identity"`
	Proof    string          `json:"proof"`
}

func machineOfferProof(offer PairingOffer, identity PairingIdentity) ([]byte, error) {
	return json.Marshal([]any{"agentwharf.e2ee.machine-offer.v1", offer.Version, offer.ID, offer.Machine, offer.PublicKey, offer.Secret, offer.ExpiresAt, identity.Device, identity.SigningKey, identity.WrappingKey})
}

func NewMachineOffer(machine string, identity LocalIdentity, now time.Time) (*PairingInvitation, MachineOffer, error) {
	public, err := identity.Public()
	if err != nil {
		return nil, MachineOffer{}, err
	}
	invitation, offer, err := NewPairingInvitation(machine, now)
	if err != nil {
		return nil, MachineOffer{}, err
	}
	message, err := machineOfferProof(offer, public)
	if err != nil {
		return nil, MachineOffer{}, ErrInvalid
	}
	defer clear(message)
	private := ed25519.NewKeyFromSeed(identity.SigningSeed)
	defer clear(private)
	proof := ed25519.Sign(private, message)
	return invitation, MachineOffer{offer, public, base64.RawURLEncoding.EncodeToString(proof)}, nil
}

func VerifyMachineOffer(offer MachineOffer, now time.Time) error {
	if _, _, _, err := pairingContext(offer.Offer, now); err != nil {
		return err
	}
	if err := validatePairingIdentity(offer.Identity); err != nil {
		return err
	}
	public, err := decode(offer.Identity.SigningKey, 32, 32)
	if err != nil {
		return err
	}
	signature, err := decode(offer.Proof, 64, 64)
	if err != nil {
		return err
	}
	message, err := machineOfferProof(offer.Offer, offer.Identity)
	if err != nil {
		return ErrInvalid
	}
	defer clear(message)
	if !ed25519.Verify(public, message, signature) {
		return ErrInvalid
	}
	return nil
}
