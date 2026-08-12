package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

const credentialRotationLeadTime = 5 * time.Minute

type credentialRotationManager struct {
	mu          sync.Mutex
	session     string
	epoch       int64
	expires     time.Time
	pending     string
	pendingGen  int64
	write       func(protocol.Frame) error
	credentials *adapterCredentialSet
	authority   func() *protocol.ConnectionAuthorityReceipt
}

func newCredentialRotationManager(ctx context.Context, authority *protocol.ConnectionAuthorityReceipt, write func(protocol.Frame) error, credentials *adapterCredentialSet, currentAuthority func() *protocol.ConnectionAuthorityReceipt) *credentialRotationManager {
	if authority == nil || authority.SessionID == "" || authority.ConnectionEpoch < 1 || authority.ExpiresAt <= 0 || write == nil {
		return nil
	}
	m := &credentialRotationManager{session: authority.SessionID, epoch: authority.ConnectionEpoch, expires: time.UnixMilli(authority.ExpiresAt), write: write, credentials: credentials, authority: currentAuthority}
	go m.run(ctx)
	return m
}

func (m *credentialRotationManager) run(ctx context.Context) {
	for {
		m.refreshAuthority()
		m.mu.Lock()
		wait := time.Until(m.expires.Add(-credentialRotationLeadTime))
		if wait < time.Second {
			wait = time.Second
		}
		m.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		m.mu.Lock()
		if m.pending != "" || time.Until(m.expires) <= 0 {
			m.mu.Unlock()
			continue
		}
		rotationID, err := randomToken()
		if err != nil {
			m.mu.Unlock()
			continue
		}
		m.pending = rotationID
		m.pendingGen = 0
		write := m.write
		m.mu.Unlock()
		if err := write(&protocol.CredentialRotationRequest{RotationID: rotationID}); err != nil {
			m.mu.Lock()
			if m.pending == rotationID {
				m.pending = ""
				m.pendingGen = 0
			}
			m.mu.Unlock()
		}
	}
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
	}
	if m.pending != "" && m.pendingGen > 0 && m.credentials != nil && m.credentials.pendingGeneration() != m.pendingGen {
		m.pending = ""
		m.pendingGen = 0
	}
	if expires := time.UnixMilli(authority.ExpiresAt); expires.After(time.Now()) {
		m.expires = expires
	}
	m.mu.Unlock()
}
