package core

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

var (
	ErrInvalidAdapterConnectionConfig = errors.New("invalid adapter connection config")
	ErrInvalidHelloAck                = errors.New("invalid hello ack")
	ErrCredentialRotationUnavailable  = errors.New("credential rotation unavailable")
	ErrCredentialRotationConflict     = errors.New("credential rotation conflict")
	ErrCredentialRotationStale        = errors.New("stale credential rotation")
	ErrCredentialAuthorityLost        = errors.New("credential authority lost")
	ErrCredentialRecoveryRequired     = errors.New("credential recovery required")
	ErrCredentialTerminal             = errors.New("credential lineage is terminal")
)

type CredentialRotationStatus string

const (
	CredentialRotationPending  CredentialRotationStatus = "pending"
	CredentialRotationActive   CredentialRotationStatus = "active"
	CredentialRotationRecovery CredentialRotationStatus = "recovery_required"
	CredentialRotationRevoked  CredentialRotationStatus = "revoked"
	CredentialRotationTerminal CredentialRotationStatus = "terminal"
)

// CredentialRotationReceipt is reference-only. It never contains bearer
// material and is valid only for the exact Session, epoch and generation.
type CredentialRotationReceipt struct {
	RotationID string
	SessionID  string
	Epoch      int64
	Generation int64
	Status     CredentialRotationStatus
}

// CredentialRecoveryPermit authorizes only a bounded same-Session recovery
// exchange. It cannot route commands, events or Provider starts.
type CredentialRecoveryPermit struct {
	SessionID  string
	Epoch      int64
	Generation int64
}

type pendingCredentialRotation struct {
	id         string
	credential *SessionCredential
}

// CredentialRotation owns one Session's bootstrap or target lineage. Target
// attachment rotations are isolated by constructing one instance per Worker.
type CredentialRotation struct {
	sessionID string

	mu                 sync.Mutex
	epoch              int64
	active             *SessionCredential
	prior              *SessionCredential
	pending            *pendingCredentialRotation
	activeRotationID   string
	priorRecoveryUntil time.Time
	revoked            bool
	terminal           bool
	authorityLost      bool
}

func NewCredentialRotation(sessionID string, initial *SessionCredential, epoch int64) (*CredentialRotation, error) {
	if sessionID == "" || initial == nil || epoch < 1 {
		return nil, ErrCredentialRotationUnavailable
	}
	if err := initial.validate(sessionID, time.Now()); err != nil {
		return nil, err
	}
	return &CredentialRotation{sessionID: sessionID, epoch: epoch, active: initial}, nil
}

func (r *CredentialRotation) Prepare(rotationID string, credential *SessionCredential) error {
	if r == nil || rotationID == "" || credential == nil {
		return ErrCredentialRotationUnavailable
	}
	if err := credential.validate(r.sessionID, time.Now()); err != nil {
		return err
	}
	metadata, err := credential.Metadata()
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return err
	}
	if r.pending != nil {
		pendingMetadata, _ := r.pending.credential.Metadata()
		if r.pending.id == rotationID && sameCredentialMetadata(pendingMetadata, metadata) {
			return nil
		}
		return ErrCredentialRotationConflict
	}
	activeMetadata, _ := r.active.Metadata()
	if metadata.Generation <= activeMetadata.Generation {
		return ErrCredentialRotationStale
	}
	r.pending = &pendingCredentialRotation{id: rotationID, credential: credential}
	return nil
}

func (r *CredentialRotation) PossessionAck(rotationID string, acceptedEpoch int64) (CredentialRotationReceipt, error) {
	if r == nil {
		return CredentialRotationReceipt{}, ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return CredentialRotationReceipt{}, err
	}
	if r.pending == nil || r.pending.id != rotationID {
		return CredentialRotationReceipt{}, ErrCredentialRotationStale
	}
	if acceptedEpoch != r.epoch {
		return CredentialRotationReceipt{}, ErrCredentialRotationStale
	}
	metadata, _ := r.pending.credential.Metadata()
	return CredentialRotationReceipt{RotationID: rotationID, SessionID: r.sessionID, Epoch: r.epoch, Generation: metadata.Generation, Status: CredentialRotationPending}, nil
}

func (r *CredentialRotation) Activate(receipt CredentialRotationReceipt) error {
	if r == nil {
		return ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return err
	}
	if r.activeRotationID == receipt.RotationID && receipt.Status == CredentialRotationActive {
		if r.active == nil || receipt.SessionID != r.sessionID || receipt.Epoch != r.epoch {
			return ErrCredentialRotationStale
		}
		metadata, _ := r.active.Metadata()
		if receipt.Generation != metadata.Generation {
			return ErrCredentialRotationStale
		}
		return nil
	}
	if r.pending == nil || receipt.RotationID != r.pending.id || receipt.SessionID != r.sessionID || receipt.Epoch != r.epoch || receipt.Status != CredentialRotationPending {
		return ErrCredentialRotationStale
	}
	metadata, _ := r.pending.credential.Metadata()
	if metadata.Generation != receipt.Generation {
		return ErrCredentialRotationStale
	}
	r.prior = r.active
	r.priorRecoveryUntil = time.Now().Add(30 * time.Second)
	r.active = r.pending.credential
	r.pending = nil
	r.activeRotationID = receipt.RotationID
	return nil
}

func (r *CredentialRotation) RetryActivation(rotationID string) (CredentialRotationReceipt, error) {
	if r == nil {
		return CredentialRotationReceipt{}, ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return CredentialRotationReceipt{}, err
	}
	if r.activeRotationID != rotationID || r.active == nil {
		return CredentialRotationReceipt{}, ErrCredentialRotationStale
	}
	metadata, _ := r.active.Metadata()
	return CredentialRotationReceipt{RotationID: rotationID, SessionID: r.sessionID, Epoch: r.epoch, Generation: metadata.Generation, Status: CredentialRotationActive}, nil
}

func (r *CredentialRotation) Reconnect(epoch, generation int64) error {
	if r == nil || epoch < 1 || generation < 1 {
		return ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.revoked || r.terminal {
		return ErrCredentialTerminal
	}
	if r.active == nil {
		return ErrCredentialRecoveryRequired
	}
	metadata, _ := r.active.Metadata()
	if generation != metadata.Generation || epoch <= r.epoch {
		return ErrCredentialRotationStale
	}
	r.epoch = epoch
	r.authorityLost = false
	return nil
}

func (r *CredentialRotation) ActiveReceipt() (CredentialRotationReceipt, error) {
	if r == nil {
		return CredentialRotationReceipt{}, ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return CredentialRotationReceipt{}, err
	}
	metadata, _ := r.active.Metadata()
	return CredentialRotationReceipt{SessionID: r.sessionID, Epoch: r.epoch, Generation: metadata.Generation, Status: CredentialRotationActive}, nil
}

func (r *CredentialRotation) RecoveryPermit() (CredentialRecoveryPermit, error) {
	if r == nil {
		return CredentialRecoveryPermit{}, ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return CredentialRecoveryPermit{}, err
	}
	if r.prior == nil || time.Now().After(r.priorRecoveryUntil) {
		return CredentialRecoveryPermit{}, ErrCredentialRecoveryRequired
	}
	metadata, _ := r.prior.Metadata()
	return CredentialRecoveryPermit{SessionID: r.sessionID, Epoch: r.epoch, Generation: metadata.Generation}, nil
}

func (r *CredentialRotation) Authorize(now time.Time) error {
	if r == nil {
		return ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return err
	}
	if r.active == nil {
		return ErrCredentialRecoveryRequired
	}
	metadata, _ := r.active.Metadata()
	if !metadata.ExpiresAt.After(now) {
		r.authorityLost = true
		return ErrSessionCredentialExpired
	}
	return nil
}

func (r *CredentialRotation) withAuthorized(fn func() error) error {
	if r == nil || fn == nil {
		return ErrCredentialRotationUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.authorityLocked(); err != nil {
		return err
	}
	if r.active == nil {
		return ErrCredentialRecoveryRequired
	}
	metadata, _ := r.active.Metadata()
	if !metadata.ExpiresAt.After(time.Now()) {
		r.authorityLost = true
		return ErrSessionCredentialExpired
	}
	return fn()
}

func (r *CredentialRotation) MarkAuthorityLost() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.authorityLost = true
	r.pending = nil
	r.mu.Unlock()
}

func (r *CredentialRotation) Revoke() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.revoked = true
	r.pending = nil
	r.active = nil
	r.prior = nil
	r.mu.Unlock()
}

func (r *CredentialRotation) Terminal() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.terminal = true
	r.pending = nil
	r.active = nil
	r.prior = nil
	r.mu.Unlock()
}

func (r *CredentialRotation) authorityLocked() error {
	if r.revoked {
		return ErrCredentialAuthorityLost
	}
	if r.terminal {
		return ErrCredentialTerminal
	}
	if r.authorityLost {
		return ErrCredentialAuthorityLost
	}
	return nil
}

func sameCredentialMetadata(left, right SessionCredentialMetadata) bool {
	return left.SessionID == right.SessionID && left.Generation == right.Generation && left.ExpiresAt.Equal(right.ExpiresAt) &&
		left.Lineage == right.Lineage
}

type AdapterConnectionConfig struct {
	SessionID       string
	Provider        string
	Token           string
	ProtocolVersion int
}

type AdapterConnectionState struct {
	cfg AdapterConnectionConfig

	mu       sync.Mutex
	accepted bool
}

func NewAdapterConnectionState(cfg AdapterConnectionConfig) (*AdapterConnectionState, error) {
	if cfg.SessionID == "" {
		return nil, fmt.Errorf("%w: session_id is required", ErrInvalidAdapterConnectionConfig)
	}
	if cfg.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", ErrInvalidAdapterConnectionConfig)
	}
	if cfg.Token == "" {
		return nil, fmt.Errorf("%w: token is required", ErrInvalidAdapterConnectionConfig)
	}
	if cfg.ProtocolVersion == 0 {
		cfg.ProtocolVersion = protocol.ProtocolVersion
	}
	if cfg.ProtocolVersion != protocol.ProtocolVersion && cfg.ProtocolVersion != protocol.ProtocolVersionV2 {
		return nil, fmt.Errorf("%w: unsupported protocol version", ErrInvalidAdapterConnectionConfig)
	}
	return &AdapterConnectionState{cfg: cfg}, nil
}

func (s *AdapterConnectionState) Hello() protocol.Hello {
	s.mu.Lock()
	resume := s.accepted
	s.mu.Unlock()

	return protocol.Hello{
		ProtocolVersion: s.cfg.ProtocolVersion,
		Role:            protocol.RoleAdapter,
		Token:           s.cfg.Token,
		SessionID:       s.cfg.SessionID,
		Provider:        s.cfg.Provider,
		Resume:          resume,
	}
}

func (s *AdapterConnectionState) MarkAccepted(ack protocol.HelloAck) (protocol.SessionSummary, error) {
	if ack.ProtocolVersion != s.cfg.ProtocolVersion {
		return protocol.SessionSummary{}, fmt.Errorf("%w: protocol version %d", ErrInvalidHelloAck, ack.ProtocolVersion)
	}
	if s.cfg.ProtocolVersion == protocol.ProtocolVersion && (ack.Capabilities != nil || ack.ConnectionAuthority != nil) {
		return protocol.SessionSummary{}, fmt.Errorf("%w: v1 acknowledgement includes capabilities", ErrInvalidHelloAck)
	}
	if s.cfg.ProtocolVersion == protocol.ProtocolVersionV2 && !validConnectionAuthorityReceipt(ack.ConnectionAuthority, s.cfg.SessionID) {
		return protocol.SessionSummary{}, fmt.Errorf("%w: v2 acknowledgement omits or corrupts connection authority", ErrInvalidHelloAck)
	}
	if len(ack.Sessions) != 1 {
		return protocol.SessionSummary{}, fmt.Errorf("%w: expected one session summary", ErrInvalidHelloAck)
	}

	summary := ack.Sessions[0]
	if summary.SessionID != s.cfg.SessionID {
		return protocol.SessionSummary{}, fmt.Errorf("%w: session_id %q", ErrInvalidHelloAck, summary.SessionID)
	}
	if summary.Provider != "" && summary.Provider != s.cfg.Provider {
		return protocol.SessionSummary{}, fmt.Errorf("%w: provider %q", ErrInvalidHelloAck, summary.Provider)
	}

	s.mu.Lock()
	s.accepted = true
	s.mu.Unlock()
	return summary, nil
}

func validConnectionAuthorityReceipt(receipt *protocol.ConnectionAuthorityReceipt, sessionID string) bool {
	return receipt != nil && receipt.SessionID == sessionID && receipt.ConnectionEpoch > 0 && receipt.CredentialGeneration > 0 &&
		receipt.AcceptedFence > 0 && receipt.WriterLeaseID != "" && receipt.ExpiresAt > 0
}
