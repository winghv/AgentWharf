package main

import (
	"context"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

func TestCredentialRotationManagerCompletesInPlace(t *testing.T) {
	frames := make(chan protocol.Frame, 4)
	now := time.Now().Add(10 * time.Minute)
	credentials := newAdapterCredentialSet("old-token", &protocol.ConnectionAuthorityReceipt{CredentialGeneration: 1})
	m := &credentialRotationManager{
		session:     "ses_1",
		epoch:       3,
		expires:     now,
		write:       func(frame protocol.Frame) error { frames <- frame; return nil },
		credentials: credentials,
	}
	m.mu.Lock()
	m.pending = "rot_1"
	m.mu.Unlock()
	credential := &protocol.CredentialRotationCredential{SessionID: "ses_1", RotationID: "rot_1", Generation: 2, Credential: "opaque", ExpiresAt: now.Add(15 * time.Minute).UnixMilli()}
	if err := m.handle(context.Background(), credential); err != nil {
		t.Fatal(err)
	}
	possession, ok := (<-frames).(*protocol.CredentialRotationPossession)
	if !ok || possession.SessionID != "ses_1" || possession.RotationID != "rot_1" || possession.AcceptedEpoch != 3 {
		t.Fatalf("possession = %#v", possession)
	}
	m.recordCredentialExpiry(credential)
	if err := m.handle(context.Background(), &protocol.CredentialRotationActivation{RotationID: "rot_1", Generation: 2, ConnectionEpoch: 4, AcceptedFence: 9}); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending != "" || m.epoch != 4 || !m.expires.Equal(time.UnixMilli(credential.ExpiresAt)) {
		t.Fatalf("manager state = pending:%q epoch:%d expires:%v", m.pending, m.epoch, m.expires)
	}
	if candidates := credentials.candidates(); len(candidates) == 0 || candidates[0] != "opaque" {
		t.Fatalf("credential candidates = %#v", candidates)
	}
}

func TestCredentialRotationManagerRejectsUnexpectedFrames(t *testing.T) {
	m := &credentialRotationManager{session: "ses_1", epoch: 1, write: func(protocol.Frame) error { return nil }}
	if err := m.handle(context.Background(), &protocol.CredentialRotationActivation{RotationID: "rot", Generation: 2, ConnectionEpoch: 2, AcceptedFence: 1}); err == nil {
		t.Fatal("unexpected activation accepted")
	}
	if err := m.handle(context.Background(), &protocol.CredentialRotationCredential{SessionID: "ses_other", RotationID: "rot", Generation: 2, Credential: "opaque", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}); err == nil {
		t.Fatal("credential for another session accepted")
	}
}

func TestCredentialRotationManagerConvergesAfterLostActivationFrame(t *testing.T) {
	now := time.Now()
	authority := &protocol.ConnectionAuthorityReceipt{
		SessionID: "ses_1", ConnectionEpoch: 4, CredentialGeneration: 2,
		AcceptedFence: 4, WriterLeaseID: "lease", ExpiresAt: now.Add(15 * time.Minute).UnixMilli(),
	}
	m := &credentialRotationManager{
		session: "ses_1", epoch: 3, expires: now.Add(time.Minute),
		pending: "rot_1", pendingGen: 2,
		authority: func() *protocol.ConnectionAuthorityReceipt { return authority },
	}
	m.refreshAuthority()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending != "" || m.pendingGen != 0 || m.epoch != 4 || !m.expires.Equal(time.UnixMilli(authority.ExpiresAt)) {
		t.Fatalf("manager state = pending:%q generation:%d epoch:%d expires:%v", m.pending, m.pendingGen, m.epoch, m.expires)
	}
}

func TestCredentialRotationManagerRetriesAfterPendingCredentialWasNotActivated(t *testing.T) {
	now := time.Now()
	credentials := newAdapterCredentialSet("old-token", &protocol.ConnectionAuthorityReceipt{CredentialGeneration: 1})
	credentials.recordPending("new-token", 2)
	credentials.converge("old-token", 1)
	m := &credentialRotationManager{
		session: "ses_1", epoch: 3, expires: now.Add(15 * time.Minute),
		pending: "rot_1", pendingGen: 2, credentials: credentials,
		authority: func() *protocol.ConnectionAuthorityReceipt {
			return &protocol.ConnectionAuthorityReceipt{SessionID: "ses_1", ConnectionEpoch: 4, CredentialGeneration: 1, ExpiresAt: now.Add(15 * time.Minute).UnixMilli()}
		},
	}
	m.refreshAuthority()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending != "" || m.pendingGen != 0 {
		t.Fatalf("abandoned rotation remains pending: %q generation=%d", m.pending, m.pendingGen)
	}
}
