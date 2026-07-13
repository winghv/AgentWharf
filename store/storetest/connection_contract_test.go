package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestConnectionContract(t *testing.T) {
	ConnectionContract(t, ConnectionHarness{Open: func(t *testing.T) store.AdapterConnectionStore {
		t.Helper()
		return &memoryConnectionStore{connections: make(map[string]store.AdapterConnection)}
	}, Invalidate: func(t *testing.T, connections store.AdapterConnectionStore, terminal bool) {
		t.Helper()
		memory := connections.(*memoryConnectionStore)
		memory.mu.Lock()
		defer memory.mu.Unlock()
		connection := memory.connections["ses_connection"]
		now := time.Now()
		if terminal {
			connection.TerminalAt = &now
		} else {
			connection.RevokedAt = &now
		}
		memory.connections[connection.SessionID] = connection
	}})
}

type memoryConnectionStore struct {
	mu          sync.Mutex
	connections map[string]store.AdapterConnection
}

func (s *memoryConnectionStore) WithAdapterConnectionTransaction(_ context.Context, fn func(store.AdapterConnectionStore) error) error {
	s.mu.Lock()
	connections := make(map[string]store.AdapterConnection, len(s.connections))
	for sessionID, connection := range s.connections {
		connections[sessionID] = connection
	}
	s.mu.Unlock()

	tx := &memoryConnectionStore{connections: connections}
	if err := fn(tx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections = tx.connections
	return nil
}

func (s *memoryConnectionStore) Append(context.Context, string, []store.PendingEvent) (int64, error) {
	return 0, errors.New("events outside connection contract")
}
func (s *memoryConnectionStore) Replay(context.Context, string, int64, func(store.Event) error) error {
	return errors.New("events outside connection contract")
}
func (s *memoryConnectionStore) LatestSeq(context.Context, string) (int64, error) { return 0, nil }
func (s *memoryConnectionStore) AdapterConnection(_ context.Context, id string) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok {
		return store.AdapterConnection{}, errors.New("missing connection")
	}
	return c, nil
}
func (s *memoryConnectionStore) InitializeAdapterConnection(_ context.Context, r store.AdapterConnectionInitialize) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.SessionID == "" || r.ActiveCredentialGeneration < 1 || !r.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.AdapterConnection{}, errors.New("invalid initialize")
	}
	if c, ok := s.connections[r.SessionID]; ok {
		return c, nil
	}
	c := store.AdapterConnection{SessionID: r.SessionID, ActiveCredentialGeneration: r.ActiveCredentialGeneration, CredentialGenerationHighWatermark: r.ActiveCredentialGeneration, ActiveCredentialExpiresAt: r.ActiveCredentialExpiresAt}
	s.connections[r.SessionID] = c
	return c, nil
}
func (s *memoryConnectionStore) AcceptAdapterHello(_ context.Context, id string, h store.AdapterHello) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok || c.RevokedAt != nil || c.TerminalAt != nil || !c.ActiveCredentialExpiresAt.After(time.Now()) || h.CredentialGeneration != c.ActiveCredentialGeneration {
		return store.AdapterConnection{}, errors.New("stale hello")
	}
	c.ConnectionEpoch++
	c.AcceptedFence++
	s.connections[id] = c
	return c, nil
}
func (s *memoryConnectionStore) ValidateAdapterAdmission(_ context.Context, id string, a store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok || c.RevokedAt != nil || c.TerminalAt != nil || !c.ActiveCredentialExpiresAt.After(time.Now()) || a.CredentialGeneration != c.ActiveCredentialGeneration || a.ConnectionEpoch != c.ConnectionEpoch || a.AcceptedFence != c.AcceptedFence || a.GrantFence <= c.AcceptedFence {
		return store.AdapterConnection{}, errors.New("stale adapter admission")
	}
	return c, nil
}
func (s *memoryConnectionStore) PrepareAdapterCredentialRotation(_ context.Context, id string, r store.AdapterCredentialRotation) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok || c.RevokedAt != nil || c.TerminalAt != nil || !c.ActiveCredentialExpiresAt.After(time.Now()) || r.ExpectedActiveCredentialGeneration != c.ActiveCredentialGeneration || r.ExpectedEpoch != c.ConnectionEpoch || c.PendingCredentialGeneration != nil || r.PendingGeneration <= c.CredentialGenerationHighWatermark || !r.ExpiresAt.After(time.Now()) || r.RotationID == "" {
		return store.AdapterConnection{}, errors.New("invalid rotation")
	}
	c.PendingCredentialGeneration = &r.PendingGeneration
	c.PendingCredentialExpiresAt = &r.ExpiresAt
	c.RotationID = &r.RotationID
	c.CredentialGenerationHighWatermark = r.PendingGeneration
	s.connections[id] = c
	return c, nil
}
func (s *memoryConnectionStore) ActivateAdapterCredential(_ context.Context, id string, a store.AdapterCredentialActivation) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok || c.RevokedAt != nil || c.TerminalAt != nil || !c.ActiveCredentialExpiresAt.After(time.Now()) || a.ExpectedActiveCredentialGeneration != c.ActiveCredentialGeneration || a.ExpectedEpoch != c.ConnectionEpoch || c.PendingCredentialGeneration == nil || *c.PendingCredentialGeneration != a.PendingGeneration || c.PendingCredentialExpiresAt == nil || !c.PendingCredentialExpiresAt.After(time.Now()) || c.RotationID == nil || *c.RotationID != a.RotationID {
		return store.AdapterConnection{}, errors.New("invalid activation")
	}
	prior := c.ActiveCredentialGeneration
	c.ActiveCredentialGeneration = a.PendingGeneration
	c.ActiveCredentialExpiresAt = *c.PendingCredentialExpiresAt
	c.PriorRecoveryGeneration = &prior
	c.PendingCredentialGeneration = nil
	c.PendingCredentialExpiresAt = nil
	c.RotationID = nil
	c.ConnectionEpoch++
	c.AcceptedFence++
	s.connections[id] = c
	return c, nil
}
