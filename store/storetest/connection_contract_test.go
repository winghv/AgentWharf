package storetest

import (
	"context"
	"errors"
	"fmt"
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
func (s *memoryConnectionStore) RefreshAdapterCredentialBeforeHello(_ context.Context, id string, r store.AdapterCredentialPreHelloRefresh) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	now := time.Now()
	exact := r.ActiveCredentialExpiresAt.Equal(c.ActiveCredentialExpiresAt) && r.ActiveCredentialExpiresAt.After(now)
	replacement := !c.ActiveCredentialExpiresAt.After(now) && r.ActiveCredentialExpiresAt.After(now) && r.ActiveCredentialExpiresAt.After(c.ActiveCredentialExpiresAt)
	if !ok || c.ConnectionEpoch != 0 || c.AcceptedFence != 0 || c.RevokedAt != nil || c.TerminalAt != nil || r.ExpectedActiveCredentialGeneration != c.ActiveCredentialGeneration || (!exact && !replacement) {
		return store.AdapterConnection{}, errors.New("invalid pre-hello refresh")
	}
	c.ActiveCredentialExpiresAt = r.ActiveCredentialExpiresAt
	s.connections[id] = c
	return c, nil
}
func (s *memoryConnectionStore) TerminateAdapterConnectionBeforeHello(_ context.Context, id string, r store.AdapterConnectionPreHelloTermination) (store.AdapterConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok || c.ConnectionEpoch != 0 || c.AcceptedFence != 0 || r.ExpectedActiveCredentialGeneration != c.ActiveCredentialGeneration {
		return store.AdapterConnection{}, errors.New("invalid pre-hello termination")
	}
	if c.RevokedAt == nil && c.TerminalAt == nil {
		now := time.Now()
		c.RevokedAt, c.TerminalAt = &now, &now
		s.connections[id] = c
	}
	if c.RevokedAt == nil || c.TerminalAt == nil {
		return store.AdapterConnection{}, errors.New("conflicting pre-hello termination")
	}
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

func TestPreHelloLifecycleContract(t *testing.T) {
	ctx := context.Background()
	connections := &memoryConnectionStore{connections: make(map[string]store.AdapterConnection)}
	init := store.AdapterConnectionInitialize{SessionID: "ses_refresh", ActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(100 * time.Millisecond)}
	if _, err := connections.InitializeAdapterConnection(ctx, init); err != nil {
		t.Fatal(err)
	}
	early := store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := connections.RefreshAdapterCredentialBeforeHello(ctx, init.SessionID, early); err == nil {
		t.Fatal("live credential refresh succeeded")
	}
	time.Sleep(150 * time.Millisecond)
	refresh := store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	refreshed, err := connections.RefreshAdapterCredentialBeforeHello(ctx, init.SessionID, refresh)
	if err != nil || !refreshed.ActiveCredentialExpiresAt.Equal(refresh.ActiveCredentialExpiresAt) {
		t.Fatalf("refresh = %+v, %v", refreshed, err)
	}
	if exact, err := connections.RefreshAdapterCredentialBeforeHello(ctx, init.SessionID, refresh); err != nil || exact != refreshed {
		t.Fatalf("exact refresh = %+v, %v", exact, err)
	}
	for _, invalid := range []store.AdapterCredentialPreHelloRefresh{
		{ExpectedActiveCredentialGeneration: 2, ActiveCredentialExpiresAt: refresh.ActiveCredentialExpiresAt},
		{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(-time.Minute)},
		{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: refresh.ActiveCredentialExpiresAt.Add(-time.Second)},
		{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: refresh.ActiveCredentialExpiresAt.Add(time.Second)},
	} {
		if _, err := connections.RefreshAdapterCredentialBeforeHello(ctx, init.SessionID, invalid); err == nil {
			t.Fatalf("invalid refresh succeeded: %+v", invalid)
		}
	}
	if _, err := connections.AcceptAdapterHello(ctx, init.SessionID, store.AdapterHello{CredentialGeneration: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := connections.RefreshAdapterCredentialBeforeHello(ctx, init.SessionID, refresh); err == nil {
		t.Fatal("post-hello refresh succeeded")
	}
	if _, err := connections.TerminateAdapterConnectionBeforeHello(ctx, init.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 3}); err == nil {
		t.Fatal("post-hello termination succeeded")
	}

	terminationInit := store.AdapterConnectionInitialize{SessionID: "ses_terminate", ActiveCredentialGeneration: 4, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := connections.InitializeAdapterConnection(ctx, terminationInit); err != nil {
		t.Fatal(err)
	}
	request := store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 4}
	terminated, err := connections.TerminateAdapterConnectionBeforeHello(ctx, terminationInit.SessionID, request)
	if err != nil || terminated.RevokedAt == nil || terminated.TerminalAt == nil || !terminated.RevokedAt.Equal(*terminated.TerminalAt) {
		t.Fatalf("termination = %+v, %v", terminated, err)
	}
	if exact, err := connections.TerminateAdapterConnectionBeforeHello(ctx, terminationInit.SessionID, request); err != nil || exact != terminated {
		t.Fatalf("exact termination = %+v, %v", exact, err)
	}
	if _, err := connections.AcceptAdapterHello(ctx, terminationInit.SessionID, store.AdapterHello{CredentialGeneration: 4}); err == nil {
		t.Fatal("late hello succeeded")
	}

	rollback := errors.New("rollback")
	refreshRollback := store.AdapterConnectionInitialize{SessionID: "ses_refresh_rollback", ActiveCredentialGeneration: 5, ActiveCredentialExpiresAt: time.Now().Add(100 * time.Millisecond)}
	if _, err := connections.InitializeAdapterConnection(ctx, refreshRollback); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	before := connections.connections[refreshRollback.SessionID]
	if err := connections.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
		if _, err := tx.RefreshAdapterCredentialBeforeHello(ctx, refreshRollback.SessionID, store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 5, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) || connections.connections[refreshRollback.SessionID] != before {
		t.Fatalf("refresh rollback = %v", err)
	}
	terminateRollback := store.AdapterConnectionInitialize{SessionID: "ses_terminate_rollback", ActiveCredentialGeneration: 6, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := connections.InitializeAdapterConnection(ctx, terminateRollback); err != nil {
		t.Fatal(err)
	}
	before = connections.connections[terminateRollback.SessionID]
	if err := connections.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
		if _, err := tx.TerminateAdapterConnectionBeforeHello(ctx, terminateRollback.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 6}); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) || connections.connections[terminateRollback.SessionID] != before {
		t.Fatalf("termination rollback = %v", err)
	}

	for _, terminal := range []bool{false, true} {
		id := fmt.Sprintf("ses_invalidated_%v", terminal)
		if _, err := connections.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: id, ActiveCredentialGeneration: 7, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		invalidated := connections.connections[id]
		now := time.Now()
		if terminal {
			invalidated.TerminalAt = &now
		} else {
			invalidated.RevokedAt = &now
		}
		connections.connections[id] = invalidated
		if _, err := connections.RefreshAdapterCredentialBeforeHello(ctx, id, store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 7, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err == nil {
			t.Fatal("invalidated refresh succeeded")
		}
		if _, err := connections.TerminateAdapterConnectionBeforeHello(ctx, id, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 7}); err == nil {
			t.Fatal("invalidated termination succeeded")
		}
	}

	raceInit := store.AdapterConnectionInitialize{SessionID: "ses_terminate_hello_race", ActiveCredentialGeneration: 8, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := connections.InitializeAdapterConnection(ctx, raceInit); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var terminateErr, helloErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, terminateErr = connections.TerminateAdapterConnectionBeforeHello(ctx, raceInit.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 8})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, helloErr = connections.AcceptAdapterHello(ctx, raceInit.SessionID, store.AdapterHello{CredentialGeneration: 8})
	}()
	close(start)
	wg.Wait()
	if (terminateErr == nil) == (helloErr == nil) {
		t.Fatalf("terminate/hello race: terminate=%v hello=%v", terminateErr, helloErr)
	}
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
