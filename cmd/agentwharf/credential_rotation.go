package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

const (
	credentialRotationLeadTime      = 5 * time.Minute
	credentialRotationRetryInterval = 10 * time.Second
)

type credentialRotationManager struct {
	mu            sync.Mutex
	session       string
	epoch         int64
	expires       time.Time
	pending       string
	pendingGen    int64
	lastRequestAt time.Time
	bootstrapDone bool
	write         func(protocol.Frame) error
	credentials   *adapterCredentialSet
	authority     func() *protocol.ConnectionAuthorityReceipt
	leadTime      time.Duration
	retryInterval time.Duration
}

func newCredentialRotationManager(ctx context.Context, authority *protocol.ConnectionAuthorityReceipt, write func(protocol.Frame) error, credentials *adapterCredentialSet, currentAuthority func() *protocol.ConnectionAuthorityReceipt) *credentialRotationManager {
	if authority == nil || authority.SessionID == "" || authority.ConnectionEpoch < 1 || authority.ExpiresAt <= 0 || write == nil {
		return nil
	}
	m := &credentialRotationManager{
		session: authority.SessionID, epoch: authority.ConnectionEpoch,
		expires: time.UnixMilli(authority.ExpiresAt), write: write,
		credentials: credentials, authority: currentAuthority,
		leadTime: credentialRotationLeadTime, retryInterval: credentialRotationRetryInterval,
	}
	go m.run(ctx)
	return m
}

func (m *credentialRotationManager) run(ctx context.Context) {
	ticker := time.NewTicker(m.rotationRetryInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = m.requestIfDue(time.Now())
		}
	}
}

// requestIfDue bootstraps one rotation after admission, then renews inside the
// expiry window. It resends the same idempotent request until the Hub advances
// authority, so a lost credential or activation frame can still converge.
func (m *credentialRotationManager) requestIfDue(now time.Time) error {
	if m == nil {
		return nil
	}
	m.refreshAuthority()
	m.mu.Lock()
	if !now.Before(m.expires) {
		m.mu.Unlock()
		return nil
	}
	if m.bootstrapDone && now.Before(m.expires.Add(-m.rotationLeadTime())) {
		m.mu.Unlock()
		return nil
	}
	if !m.lastRequestAt.IsZero() && now.Sub(m.lastRequestAt) < m.rotationRetryInterval() {
		m.mu.Unlock()
		return nil
	}
	rotationID := m.pending
	if rotationID == "" {
		var err error
		rotationID, err = randomToken()
		if err != nil {
			m.mu.Unlock()
			return err
		}
		m.pending = rotationID
		m.pendingGen = 0
	}
	m.lastRequestAt = now
	write := m.write
	m.mu.Unlock()
	return write(&protocol.CredentialRotationRequest{RotationID: rotationID})
}

func (m *credentialRotationManager) rotationLeadTime() time.Duration {
	if m.leadTime > 0 {
		return m.leadTime
	}
	return credentialRotationLeadTime
}

func (m *credentialRotationManager) rotationRetryInterval() time.Duration {
	if m.retryInterval > 0 {
		return m.retryInterval
	}
	return credentialRotationRetryInterval
}

func (m *credentialRotationManager) handle(ctx context.Context, frame protocol.Frame) error {
	if m == nil {
		return errors.New("credential rotation unavailable")
	}
	switch typed := frame.(type) {
	case *protocol.CredentialRotationCredential:
		return m.handleCredential(ctx, typed)
	case *protocol.CredentialRotationActivation:
		return m.handleActivation(typed)
	default:
		return errors.New("unsupported credential rotation frame")
	}
}

func (m *credentialRotationManager) handleCredential(_ context.Context, credential *protocol.CredentialRotationCredential) error {
	if credential == nil || credential.SessionID != m.session || credential.Generation < 1 || credential.ExpiresAt <= time.Now().UnixMilli() {
		return errors.New("invalid credential rotation credential")
	}
	m.mu.Lock()
	if m.pending == "" || credential.RotationID != m.pending {
		m.mu.Unlock()
		return errors.New("unexpected credential rotation credential")
	}
	m.pendingGen = credential.Generation
	rotationID, epoch, write := m.pending, m.epoch, m.write
	m.mu.Unlock()
	if m.credentials != nil {
		m.credentials.recordPending(credential.Credential, credential.Generation)
	}
	if err := write(&protocol.CredentialRotationPossession{SessionID: m.session, RotationID: rotationID, Generation: credential.Generation, AcceptedEpoch: epoch}); err != nil {
		return fmt.Errorf("send credential rotation possession: %w", err)
	}
	return nil
}

func (m *credentialRotationManager) handleActivation(activation *protocol.CredentialRotationActivation) error {
	if activation == nil || activation.Generation < 1 || activation.ConnectionEpoch < 1 || activation.AcceptedFence < 1 {
		return errors.New("invalid credential rotation activation")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending == "" || activation.RotationID != m.pending || activation.ConnectionEpoch <= m.epoch {
		return errors.New("unexpected credential rotation activation")
	}
	m.epoch = activation.ConnectionEpoch
	// The server sends the new expiry in the credential frame. The activation
	// itself intentionally carries only the non-secret authority tuple.
	// Keep the old expiry only until the next credential frame updates it.
	m.pending = ""
	m.pendingGen = 0
	m.lastRequestAt = time.Time{}
	m.bootstrapDone = true
	if m.credentials != nil {
		m.credentials.activate(activation.Generation)
	}
	return nil
}

func (m *credentialRotationManager) recordCredentialExpiry(frame *protocol.CredentialRotationCredential) {
	if m == nil || frame == nil {
		return
	}
	m.mu.Lock()
	if frame.RotationID == m.pending && frame.ExpiresAt > 0 {
		m.expires = time.UnixMilli(frame.ExpiresAt)
	}
	m.mu.Unlock()
}

func (m *credentialRotationManager) refreshAuthority() {
	if m == nil || m.authority == nil {
		return
	}
	authority := m.authority()
	if authority == nil || authority.SessionID != m.session || authority.ConnectionEpoch < 1 || authority.ExpiresAt <= 0 {
		return
	}
	m.mu.Lock()
	if authority.ConnectionEpoch > m.epoch {
		m.epoch = authority.ConnectionEpoch
	}
	if m.pending != "" && m.pendingGen > 0 && authority.CredentialGeneration >= m.pendingGen {
		m.pending = ""
		m.pendingGen = 0
		m.lastRequestAt = time.Time{}
		m.bootstrapDone = true
	}
	if m.pending != "" && m.pendingGen > 0 && m.credentials != nil && m.credentials.pendingGeneration() != m.pendingGen {
		m.pending = ""
		m.pendingGen = 0
		m.lastRequestAt = time.Time{}
	}
	if expires := time.UnixMilli(authority.ExpiresAt); expires.After(time.Now()) {
		m.expires = expires
	}
	m.mu.Unlock()
}
