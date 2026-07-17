package hub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/store"
)

func TestCurrentBootstrapAuthoritySerializesDisconnect(t *testing.T) {
	fakeStore := &liveAuthorityStore{started: make(chan struct{}), release: make(chan struct{})}
	adapter := &adapterConnection{sessionID: "ses_bootstrap", provider: "claude-code", admission: store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: 1, AcceptedFence: 1, GrantFence: 2}}
	handler := &webSocketHandler{adapterAuthority: &adapterDispatchAuthority{store: fakeStore}, adapterAdmissionLocks: make(map[string]chan struct{}), adapters: map[string]*adapterConnection{"ses_bootstrap": adapter}}
	grant := auth.AttachGrant{BootstrapSessionID: "ses_bootstrap", Provider: "claude-code", GrantFence: 2}
	resolved := make(chan error, 1)
	go func() { _, err := handler.CurrentBootstrapAuthority(context.Background(), grant); resolved <- err }()
	<-fakeStore.started
	removed := make(chan struct{})
	go func() { handler.unregisterAdapter(adapter); close(removed) }()
	select {
	case <-removed:
		t.Fatal("unregister completed while resolver held admission lock")
	case <-time.After(20 * time.Millisecond):
	}
	close(fakeStore.release)
	if err := <-resolved; err != nil {
		t.Fatalf("CurrentBootstrapAuthority() = %v", err)
	}
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("unregister deadlocked after resolver")
	}
	if handler.adapters[adapter.sessionID] != nil {
		t.Fatal("adapter remained mapped after unregister")
	}
	if _, err := handler.CurrentBootstrapAuthority(context.Background(), grant); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("disconnected authority = %v", err)
	}
	grant.Provider = "other"
	if _, err := handler.CurrentBootstrapAuthority(context.Background(), grant); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("provider mismatch = %v", err)
	}
}

type liveAuthorityStore struct {
	store.AdapterConnectionTransactor
	store.AdapterGrantFenceStore
	started chan struct{}
	release chan struct{}
}

func (s *liveAuthorityStore) AppendAdapterEvents(context.Context, string, store.AdapterConnectionAdmission, []store.PendingEvent) (int64, error) {
	return 0, nil
}
func (s *liveAuthorityStore) ValidateAdapterAdmission(_ context.Context, _ string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	if admission.GrantFence != 2 {
		return store.AdapterConnection{}, errors.New("bad fence")
	}
	close(s.started)
	<-s.release
	return store.AdapterConnection{}, nil
}
