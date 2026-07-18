package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strings"
	"time"

	"github.com/winghv/agentwharf/store"
)

var (
	ErrInvalidScope = errors.New("invalid scope")
	ErrInvalidToken = errors.New("invalid token")
	ErrUnauthorized = errors.New("unauthorized")
)

type Kind string

const (
	KindSession Kind = "session"
	KindGroup   Kind = "group"
	KindAPI     Kind = "api"
)

type Access string

const (
	AccessControl Access = "control"
	AccessView    Access = "view"
	AccessAdapter Access = "adapter"
	AccessAll     Access = "*"
)

type Scope struct {
	Kind   Kind
	ID     string
	Access Access
}

type Principal struct {
	Subject string
	Scopes  []Scope
}

type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Principal, error)
	Authorize(ctx context.Context, principal Principal, scope Scope) error
}

type SessionAdmissionMode string

const (
	SessionAdmissionAttachOnly SessionAdmissionMode = "attach_only"
	SessionAdmissionCurrent    SessionAdmissionMode = "current"
)

type SessionAdmissionAction string

const (
	SessionAdmissionAttach     SessionAdmissionAction = "attach"
	SessionAdmissionStatus     SessionAdmissionAction = "status"
	SessionAdmissionHistory    SessionAdmissionAction = "history"
	SessionAdmissionSend       SessionAdmissionAction = "send"
	SessionAdmissionSettings   SessionAdmissionAction = "settings"
	SessionAdmissionRunControl SessionAdmissionAction = "run_control"
	SessionAdmissionPermission SessionAdmissionAction = "permission"
	SessionAdmissionRotation   SessionAdmissionAction = "rotation"
)

type SessionAdmissionClaim struct {
	SessionID string
	Provider  string
	ExpiresAt time.Time
}

type SessionAdmissionRequest struct {
	Principal Principal
	Claim     SessionAdmissionClaim
	Truth     store.SessionAdmissionTruth
}

type SessionAdmissionDecision struct {
	Mode      SessionAdmissionMode
	MayMutate bool
}

const attachGrantMaxTTL = 5 * time.Minute
const attachGrantClockSkew = 30 * time.Second
const maxAttachGrantStringBytes = 256

// AttachGrant is verified at Client-to-Hub ingress. It intentionally contains
// only bounded, platform-neutral authorization facts and never a raw bearer.
type AttachGrant struct {
	Audience           string
	JTI                string
	AttachID           string
	BootstrapSessionID string
	TargetSessionID    string
	Provider           string
	IssuedAt           time.Time
	ExpiresAt          time.Time
	DeliveryDeadline   time.Time
	GrantFence         int64
	Commit             AttachCommitMaterial
}

// BootstrapAuthority is the Store-derived current normal-hello snapshot.
// It must be reacquired by T18B inside its durable attach transaction.
type BootstrapAuthority struct {
	SessionID            string
	Provider             string
	CredentialGeneration int64
	ConnectionEpoch      int64
	AcceptedFence        int64
	Live                 bool
}

type AttachAuthorizationRequest struct {
	Principal        Principal
	Grant            AttachGrant
	Bootstrap        BootstrapAuthority
	ExpectedAudience string
}

// AttachAuthorization is the non-durable ingress result. T18B must pass its
// exact Bootstrap tuple to the Store transaction for final revalidation.
type AttachAuthorization struct {
	Grant     AttachGrant
	Bootstrap BootstrapAuthority
}

// AttachGrantVerifier verifies one opaque Client-to-Hub attach grant and
// returns only its bounded claims. Implementations must not persist or log the
// raw grant. The Hub does not depend on a platform token format or signer.
type AttachGrantVerifier interface {
	VerifyAttachGrant(ctx context.Context, rawGrant, expectedAudience string) (AttachGrant, error)
}

// AttachCommitMaterial is the bounded, non-secret output that Auth returns
// with a verified grant. Raw grants, bearer material, and HMAC keys never
// leave the Auth boundary.
type AttachCommitMaterial struct {
	JTIHash                      [32]byte
	Fingerprint                  AttachCommitFingerprint
	TargetCredentialLineageRef   string
	FirstDeliveryReferenceID     string
	FirstDeliveryReferenceDigest [32]byte
	proof                        *attachCommitProof
}

type attachCommitProof struct {
	grant                        attachGrantSnapshot
	JTIHash                      [32]byte
	Fingerprint                  AttachCommitFingerprint
	TargetCredentialLineageRef   string
	FirstDeliveryReferenceID     string
	FirstDeliveryReferenceDigest [32]byte
}

type attachGrantSnapshot struct {
	Audience, JTI, AttachID, BootstrapSessionID, TargetSessionID, Provider string
	IssuedAt, ExpiresAt, DeliveryDeadline                                  time.Time
	GrantFence                                                             int64
}

// AttachCommitFingerprint identifies the key and protocol domain used for a
// stable, versioned HMAC of a verified attach grant.
type AttachCommitFingerprint struct {
	Domain     string
	Version    int64
	Digest     [32]byte
	KeyVersion int64
}

const attachCommitFingerprintDomain = "agentwharf.attach-request.v1"

// HMACAttachCommitDeriver lets an Auth verifier derive Store-safe material
// from verified claims while keeping its HMAC key private.
type HMACAttachCommitDeriver struct {
	key        []byte
	keyVersion int64
}

func NewHMACAttachCommitDeriver(key []byte, keyVersion int64) (*HMACAttachCommitDeriver, error) {
	if len(key) == 0 || keyVersion <= 0 {
		return nil, ErrUnauthorized
	}
	return &HMACAttachCommitDeriver{key: append([]byte(nil), key...), keyVersion: keyVersion}, nil
}

func (d *HMACAttachCommitDeriver) DeriveAttachCommit(_ context.Context, grant AttachGrant) (AttachCommitMaterial, error) {
	if d == nil || len(d.key) == 0 || d.keyVersion <= 0 || grant.JTI == "" || grant.AttachID == "" ||
		grant.BootstrapSessionID == "" || grant.TargetSessionID == "" || grant.Provider == "" ||
		!boundedAttachGrantStrings(grant.JTI, grant.AttachID, grant.BootstrapSessionID, grant.TargetSessionID, grant.Provider) {
		return AttachCommitMaterial{}, ErrUnauthorized
	}
	grantFields := []string{
		grant.Audience, grant.JTI, grant.AttachID, grant.BootstrapSessionID, grant.TargetSessionID, grant.Provider,
		fmt.Sprintf("%d", grant.IssuedAt.UnixNano()), fmt.Sprintf("%d", grant.ExpiresAt.UnixNano()),
		fmt.Sprintf("%d", grant.DeliveryDeadline.UnixNano()), fmt.Sprintf("%d", grant.GrantFence),
	}
	lineage := d.digest("target-lineage/v1", grant.BootstrapSessionID, grant.TargetSessionID, grant.AttachID, grant.JTI)
	material := AttachCommitMaterial{
		JTIHash: d.digest("jti/v1", grant.JTI),
		Fingerprint: AttachCommitFingerprint{
			Domain: attachCommitFingerprintDomain, Version: 1, KeyVersion: d.keyVersion,
			Digest: d.digest("fingerprint/v1", grantFields...),
		},
		TargetCredentialLineageRef:   "hmac-v1:" + hex.EncodeToString(lineage[:]),
		FirstDeliveryReferenceID:     grant.AttachID,
		FirstDeliveryReferenceDigest: d.digest("first-delivery/v1", grantFields...),
	}
	material.proof = &attachCommitProof{
		grant: attachGrantSnapshotFrom(grant), JTIHash: material.JTIHash, Fingerprint: material.Fingerprint,
		TargetCredentialLineageRef:   material.TargetCredentialLineageRef,
		FirstDeliveryReferenceID:     material.FirstDeliveryReferenceID,
		FirstDeliveryReferenceDigest: material.FirstDeliveryReferenceDigest,
	}
	return material, nil
}

// ValidateAttachCommitMaterial accepts only a material value produced by the
// Auth HMAC derivation capability for these exact verified grant claims.
func ValidateAttachCommitMaterial(grant AttachGrant) error {
	material := grant.Commit
	proof := material.proof
	if proof == nil || proof.grant != attachGrantSnapshotFrom(grant) || proof.JTIHash != material.JTIHash ||
		proof.Fingerprint != material.Fingerprint || proof.TargetCredentialLineageRef != material.TargetCredentialLineageRef ||
		proof.FirstDeliveryReferenceID != material.FirstDeliveryReferenceID ||
		proof.FirstDeliveryReferenceDigest != material.FirstDeliveryReferenceDigest ||
		material.Fingerprint.Domain != attachCommitFingerprintDomain || material.Fingerprint.Version != 1 ||
		material.Fingerprint.KeyVersion < 1 || material.JTIHash == ([32]byte{}) ||
		material.Fingerprint.Digest == ([32]byte{}) || material.TargetCredentialLineageRef == "" ||
		len(material.TargetCredentialLineageRef) > maxAttachGrantStringBytes ||
		material.FirstDeliveryReferenceID != grant.AttachID || material.FirstDeliveryReferenceDigest == ([32]byte{}) {
		return ErrUnauthorized
	}
	return nil
}

func attachGrantSnapshotFrom(grant AttachGrant) attachGrantSnapshot {
	return attachGrantSnapshot{
		Audience: grant.Audience, JTI: grant.JTI, AttachID: grant.AttachID,
		BootstrapSessionID: grant.BootstrapSessionID, TargetSessionID: grant.TargetSessionID, Provider: grant.Provider,
		IssuedAt: grant.IssuedAt, ExpiresAt: grant.ExpiresAt, DeliveryDeadline: grant.DeliveryDeadline,
		GrantFence: grant.GrantFence,
	}
}

func (d *HMACAttachCommitDeriver) digest(domain string, fields ...string) [32]byte {
	mac := hmac.New(sha256.New, d.key)
	writeAttachCommitField(mac, domain)
	for _, field := range fields {
		writeAttachCommitField(mac, field)
	}
	var digest [32]byte
	copy(digest[:], mac.Sum(nil))
	return digest
}

func writeAttachCommitField(mac hash.Hash, field string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(field)))
	_, _ = mac.Write(length[:])
	_, _ = mac.Write([]byte(field))
}

// EvaluateAttachAuthorization validates the non-durable half of session.attach.
// It cannot authorize a delivery: T18B must repeat the Store-owned checks in its
// atomic commit before any attempt, credential, outbox, or event exists.
func EvaluateAttachAuthorization(request AttachAuthorizationRequest) error {
	grant := request.Grant
	bootstrap := request.Bootstrap
	now := time.Now()

	if request.ExpectedAudience == "" || grant.Audience != request.ExpectedAudience || !boundedAttachGrantStrings(
		request.ExpectedAudience, grant.Audience, grant.JTI, grant.AttachID, grant.BootstrapSessionID, grant.TargetSessionID, grant.Provider,
	) ||
		grant.JTI == "" || grant.AttachID == "" || grant.BootstrapSessionID == "" ||
		grant.TargetSessionID == "" || grant.Provider == "" ||
		grant.BootstrapSessionID == grant.TargetSessionID || !hasOnlyExactSessionControl(request.Principal, grant.TargetSessionID) {
		return ErrUnauthorized
	}
	if grant.IssuedAt.IsZero() || grant.ExpiresAt.IsZero() || grant.DeliveryDeadline.IsZero() ||
		!grant.ExpiresAt.After(grant.IssuedAt) || grant.ExpiresAt.Sub(grant.IssuedAt) > attachGrantMaxTTL ||
		grant.IssuedAt.After(now.Add(attachGrantClockSkew)) || grant.ExpiresAt.Before(now.Add(-attachGrantClockSkew)) ||
		grant.DeliveryDeadline.Before(grant.IssuedAt) || grant.DeliveryDeadline.After(grant.ExpiresAt.Add(attachGrantClockSkew)) ||
		now.After(grant.DeliveryDeadline) {
		return ErrUnauthorized
	}
	if !bootstrap.Live || bootstrap.SessionID != grant.BootstrapSessionID || bootstrap.Provider != grant.Provider ||
		bootstrap.CredentialGeneration <= 0 || bootstrap.ConnectionEpoch <= 0 || bootstrap.AcceptedFence <= 0 ||
		grant.GrantFence <= bootstrap.AcceptedFence {
		return ErrUnauthorized
	}
	return nil
}

func boundedAttachGrantStrings(values ...string) bool {
	for _, value := range values {
		if len(value) > maxAttachGrantStringBytes {
			return false
		}
	}
	return true
}

func hasOnlyExactSessionControl(principal Principal, sessionID string) bool {
	if principal.Subject == "" || len(principal.Scopes) != 1 {
		return false
	}
	scope := principal.Scopes[0]
	return scope.Kind == KindSession && scope.ID == sessionID && scope.Access == AccessControl
}

// EvaluateSessionAdmission is limited to exact control scope, provider-bound
// Auth claim, and Store-owned Session truth. It never consults platform state.
func EvaluateSessionAdmission(request SessionAdmissionRequest) (SessionAdmissionDecision, error) {
	claim := request.Claim
	truth := request.Truth
	if claim.SessionID == "" || claim.Provider == "" || !claim.ExpiresAt.After(time.Now()) || claim.ExpiresAt.After(time.Now().Add(5*time.Minute)) || truth.SessionID != claim.SessionID || !hasExactSessionControl(request.Principal, claim.SessionID) {
		return SessionAdmissionDecision{}, ErrUnauthorized
	}
	if !truth.Exists {
		if truth.Complete || truth.Terminal || truth.Conflicting || truth.Live {
			return SessionAdmissionDecision{}, ErrUnauthorized
		}
		return SessionAdmissionDecision{Mode: SessionAdmissionAttachOnly}, nil
	}
	if !truth.Complete || truth.Terminal || truth.Conflicting || !truth.Live {
		return SessionAdmissionDecision{}, ErrUnauthorized
	}
	return SessionAdmissionDecision{Mode: SessionAdmissionCurrent, MayMutate: true}, nil
}

func (decision SessionAdmissionDecision) Allows(action SessionAdmissionAction) bool {
	switch decision.Mode {
	case SessionAdmissionAttachOnly:
		return action == SessionAdmissionAttach || action == SessionAdmissionStatus
	case SessionAdmissionCurrent:
		if !decision.MayMutate {
			return false
		}
		switch action {
		case SessionAdmissionAttach,
			SessionAdmissionStatus,
			SessionAdmissionHistory,
			SessionAdmissionSend,
			SessionAdmissionSettings,
			SessionAdmissionRunControl,
			SessionAdmissionPermission,
			SessionAdmissionRotation:
			return true
		}
	}
	return false
}

func hasExactSessionControl(principal Principal, sessionID string) bool {
	for _, scope := range principal.Scopes {
		if scope.Kind == KindSession && scope.ID == sessionID && scope.Access == AccessControl {
			return true
		}
	}
	return false
}

type SessionCredentialLineageKind string

const (
	SessionCredentialBootstrapInitial SessionCredentialLineageKind = "bootstrap_initial"
	SessionCredentialTargetAttach     SessionCredentialLineageKind = "target_attach"
)

type SessionCredentialLineage struct {
	Kind     SessionCredentialLineageKind
	AttachID string
	JTI      string
}

type SessionCredentialRequest struct {
	SessionID    string
	Lineage      SessionCredentialLineage
	Generation   int64
	RotationID   string
	RevocationID string
	ExpiresAt    time.Time
}

type PreparedSessionCredential struct {
	Bearer       string
	SessionID    string
	Lineage      SessionCredentialLineage
	Generation   int64
	RotationID   string
	RevocationID string
	ExpiresAt    time.Time
	Scope        Scope
}

// SessionCredentialIssuer prepares one in-memory Adapter bearer. It makes no
// persistent side effect; the caller must discard the result on transaction failure.
type SessionCredentialIssuer interface {
	PrepareSessionCredential(ctx context.Context, request SessionCredentialRequest) (PreparedSessionCredential, error)
}

func ParseScope(raw string) (Scope, error) {
	parts := strings.Split(raw, ":")
	switch {
	case len(parts) == 2 && parts[0] == string(KindAPI) && parts[1] == string(AccessAll):
		return API(), nil
	case len(parts) == 3 && parts[0] == string(KindSession) && parts[1] != "":
		switch parts[2] {
		case string(AccessControl):
			return SessionControl(parts[1]), nil
		case string(AccessView):
			return SessionView(parts[1]), nil
		case string(AccessAdapter):
			return SessionAdapter(parts[1]), nil
		default:
			return Scope{}, fmt.Errorf("%w: %q", ErrInvalidScope, raw)
		}
	case len(parts) == 3 && parts[0] == string(KindGroup) && parts[1] != "" && parts[2] == string(AccessControl):
		return GroupControl(parts[1]), nil
	default:
		return Scope{}, fmt.Errorf("%w: %q", ErrInvalidScope, raw)
	}
}

func SessionControl(sessionID string) Scope {
	return Scope{Kind: KindSession, ID: sessionID, Access: AccessControl}
}

func SessionView(sessionID string) Scope {
	return Scope{Kind: KindSession, ID: sessionID, Access: AccessView}
}

func SessionAdapter(sessionID string) Scope {
	return Scope{Kind: KindSession, ID: sessionID, Access: AccessAdapter}
}

func GroupControl(groupID string) Scope {
	return Scope{Kind: KindGroup, ID: groupID, Access: AccessControl}
}

func API() Scope {
	return Scope{Kind: KindAPI, Access: AccessAll}
}

func (s Scope) String() string {
	if s.Kind == KindAPI && s.Access == AccessAll {
		return "api:*"
	}
	return strings.Join([]string{string(s.Kind), s.ID, string(s.Access)}, ":")
}

func (s Scope) Validate() error {
	switch s.Kind {
	case KindAPI:
		if s.ID == "" && s.Access == AccessAll {
			return nil
		}
	case KindSession:
		if s.ID != "" && (s.Access == AccessControl || s.Access == AccessView || s.Access == AccessAdapter) {
			return nil
		}
	case KindGroup:
		if s.ID != "" && s.Access == AccessControl {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrInvalidScope, s)
}

func Authorize(principal Principal, requested Scope) error {
	if err := requested.Validate(); err != nil {
		return err
	}
	for _, granted := range principal.Scopes {
		if err := granted.Validate(); err != nil {
			continue
		}
		if allows(granted, requested) {
			return nil
		}
	}
	return fmt.Errorf("%w: subject %q lacks %s", ErrUnauthorized, principal.Subject, requested)
}

func allows(granted Scope, requested Scope) bool {
	if granted.Kind == KindAPI && granted.Access == AccessAll {
		return requested.Access != AccessAdapter
	}

	if granted.Kind != requested.Kind || granted.ID != requested.ID {
		return false
	}

	if granted.Access == requested.Access {
		return true
	}

	return granted.Kind == KindSession &&
		granted.Access == AccessControl &&
		requested.Access == AccessView
}
