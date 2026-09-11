package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"sort"
)

// SessionMemberKey is one authorized device in a signed session directory. The
// signing key is the device's Ed25519 verify key in base64url form so a terminal
// can import it without another relay round trip.
type SessionMemberKey struct {
	Device     string `json:"device"`
	SigningKey string `json:"signing_key"`
	Control    bool   `json:"control"`
}

// SessionMembershipDirectory is the endpoint-signed list of devices authorized
// for one session epoch. A terminal verifies the signature with the pinned
// machine identity and then trusts only the listed keys for foreign command
// authorship; the relay cannot add a member.
type SessionMembershipDirectory struct {
	Machine   string             `json:"machine"`
	Session   string             `json:"session"`
	Epoch     int64              `json:"epoch"`
	KeyID     string             `json:"key_id"`
	Members   []SessionMemberKey `json:"members"`
	Signature string             `json:"signature"`
}

func (d SessionMembershipDirectory) signingBytes() ([]byte, error) {
	for _, value := range []string{d.Machine, d.Session, d.KeyID} {
		if !identifier.MatchString(value) {
			return nil, ErrInvalid
		}
	}
	if d.Epoch < 1 || d.Epoch >= 9007199254740991 || len(d.Members) < 1 || len(d.Members) > 32 {
		return nil, ErrInvalid
	}
	members := make([][]any, len(d.Members))
	for i, member := range d.Members {
		if !identifier.MatchString(member.Device) || (i > 0 && d.Members[i-1].Device >= member.Device) {
			return nil, ErrInvalid
		}
		if _, err := decode(member.SigningKey, ed25519.PublicKeySize, ed25519.PublicKeySize); err != nil {
			return nil, ErrInvalid
		}
		members[i] = []any{member.Device, member.SigningKey, member.Control}
	}
	return json.Marshal([]any{"agentwharf.e2ee.membership-directory.v1", d.Machine, d.Session, d.Epoch, d.KeyID, members})
}

// SignSessionMembershipDirectory returns the current epoch's authorized devices
// signed by the endpoint identity. Members are ordered by device id so any two
// terminals observing the same grants verify identical bytes.
func (v *SessionKeyVault) SignSessionMembershipDirectory(ctx context.Context, journal *CommandJournal, session string) (SessionMembershipDirectory, error) {
	if v == nil || journal == nil || journal.db != v.db {
		return SessionMembershipDirectory{}, ErrInvalid
	}
	epoch, keyID, err := journal.SessionState(ctx, session)
	if err != nil {
		return SessionMembershipDirectory{}, err
	}
	grants, err := journal.sessionGrants(ctx, session)
	if err != nil {
		return SessionMembershipDirectory{}, err
	}
	members := make([]SessionMemberKey, 0, len(grants))
	for _, grant := range grants {
		members = append(members, SessionMemberKey{Device: grant.DeviceID, SigningKey: base64.RawURLEncoding.EncodeToString(grant.VerifyKey), Control: grant.Control})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Device < members[j].Device })
	directory := SessionMembershipDirectory{Machine: v.public.Device, Session: session, Epoch: epoch, KeyID: keyID, Members: members}
	data, err := directory.signingBytes()
	if err != nil {
		return SessionMembershipDirectory{}, err
	}
	signing := ed25519.NewKeyFromSeed(v.identity.SigningSeed)
	defer clear(signing)
	directory.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(signing, data))
	return directory, nil
}

// VerifySessionMembershipDirectory checks the endpoint signature over the
// canonical directory bytes. Terminals run the same check with the pinned
// machine signing key before trusting any listed member.
func VerifySessionMembershipDirectory(directory SessionMembershipDirectory, machine ed25519.PublicKey) error {
	data, err := directory.signingBytes()
	if err != nil {
		return err
	}
	signature, err := decode(directory.Signature, ed25519.SignatureSize, ed25519.SignatureSize)
	if err != nil || !ed25519.Verify(machine, data, signature) {
		return ErrUnauthorized
	}
	return nil
}
