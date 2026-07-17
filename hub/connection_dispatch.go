package hub

import (
	"context"
	"errors"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

const adapterAuthorityPollInterval = 250 * time.Millisecond

var errAdapterAuthorityLost = errors.New("adapter authority lost")

type adapterDispatchStore interface {
	store.AdapterConnectionStore
	store.AdapterGrantFenceStore
	AppendAdapterEvents(context.Context, string, store.AdapterConnectionAdmission, []store.PendingEvent) (int64, error)
}
type adapterDispatchAuthority struct {
	store             adapterDispatchStore
	adapterCredential func(context.Context, string, auth.Principal, string) (int64, int64, bool, error)
}

func newAdapterDispatchAuthority(handshake *Handshake, candidate any) *adapterDispatchAuthority {
	if handshake == nil {
		return nil
	}
	authenticator, authOK := handshake.authenticator.(interface {
		AdapterCredential(context.Context, string, auth.Principal, string) (int64, int64, bool, error)
	})
	if candidate == nil {
		candidate = handshake.events
	}
	dispatchStore, storeOK := candidate.(adapterDispatchStore)
	if !authOK || !storeOK || dispatchStore == nil {
		return nil
	}
	return &adapterDispatchAuthority{store: dispatchStore, adapterCredential: authenticator.AdapterCredential}
}
func (a *adapterDispatchAuthority) admit(ctx context.Context, token string, principal auth.Principal, sessionID string) (store.AdapterConnectionAdmission, error) {
	generation, expiresAt, allowInitialize, err := a.adapterCredential(ctx, token, principal, sessionID)
	if err != nil || generation != 1 || expiresAt <= time.Now().UnixNano() {
		return store.AdapterConnectionAdmission{}, errAdapterAuthorityLost
	}
	if allowInitialize {
		if _, err = a.store.AdapterConnection(ctx, sessionID); err != nil {
			_, err = a.store.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
				SessionID: sessionID, ActiveCredentialGeneration: generation, ActiveCredentialExpiresAt: time.Unix(0, expiresAt),
			})
		}
	}
	if err != nil {
		return store.AdapterConnectionAdmission{}, errAdapterAuthorityLost
	}
	connection, err := a.store.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: generation})
	if err != nil {
		return store.AdapterConnectionAdmission{}, errAdapterAuthorityLost
	}
	grantFence, err := a.store.AllocateAdapterGrantFence(ctx)
	if err != nil {
		return store.AdapterConnectionAdmission{}, errAdapterAuthorityLost
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: generation, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence}
	return admission, nil
}
func (h *webSocketHandler) validateAdapter(ctx context.Context, adapter *adapterConnection) error {
	_, authorityErr := h.adapterAuthority.store.ValidateAdapterAdmission(ctx, adapter.sessionID, adapter.admission)
	h.mu.Lock()
	current := h.adapters[adapter.sessionID] == adapter
	h.mu.Unlock()
	if authorityErr != nil || !current {
		h.rejectAdapter(adapter)
		return errAdapterAuthorityLost
	}
	return nil
}
func (h *webSocketHandler) withAdapterEffect(ctx context.Context, adapter *adapterConnection, effect func() error) error {
	adapter.effectMu.Lock()
	defer adapter.effectMu.Unlock()
	if err := h.validateAdapter(ctx, adapter); err != nil {
		return err
	}
	return effect()
}

func (a *adapterDispatchAuthority) withAdmission(ctx context.Context, adapter *adapterConnection, effect func(context.Context) error) error {
	transactor, ok := a.store.(store.AdapterConnectionTransactor)
	if !ok {
		return errAdapterAuthorityLost
	}
	effectCtx, cancel := context.WithTimeout(ctx, adapterAuthorityPollInterval)
	defer cancel()
	return transactor.WithAdapterConnectionTransaction(effectCtx, func(tx store.AdapterConnectionStore) error {
		if _, err := tx.ValidateAdapterAdmission(effectCtx, adapter.sessionID, adapter.admission); err != nil {
			return errAdapterAuthorityLost
		}
		return effect(effectCtx)
	})
}
func (h *webSocketHandler) lockAdapterAdmission(sessionID string) (*adapterConnection, func()) {
	h.adapterAdmissionMu.Lock()
	previous := h.adapterAdmissionLocks[sessionID]
	current := make(chan struct{})
	h.adapterAdmissionLocks[sessionID] = current
	h.adapterAdmissionMu.Unlock()
	if previous != nil {
		<-previous
	}
	h.mu.Lock()
	adapter := h.adapters[sessionID]
	h.mu.Unlock()
	if adapter != nil {
		adapter.effectMu.Lock()
	}
	return adapter, func() {
		if adapter != nil {
			adapter.effectMu.Unlock()
		}
		h.adapterAdmissionMu.Lock()
		if h.adapterAdmissionLocks[sessionID] == current {
			delete(h.adapterAdmissionLocks, sessionID)
		}
		close(current)
		h.adapterAdmissionMu.Unlock()
	}
}
func (h *webSocketHandler) rejectAdapter(adapter *adapterConnection) {
	if adapter != nil {
		h.unregisterAdapter(adapter)
		adapter.conn.CloseNow()
	}
}
func (h *webSocketHandler) writeAdapterFrame(ctx context.Context, adapter *adapterConnection, frame protocol.Frame) error {
	return h.withAdapterEffect(ctx, adapter, func() error { return adapter.writeFrame(ctx, frame) })
}
func (h *webSocketHandler) watchAdapter(ctx context.Context, adapter *adapterConnection) {
	ticker := time.NewTicker(adapterAuthorityPollInterval)
	defer ticker.Stop()
	for range ticker.C {
		checkCtx, cancel := context.WithTimeout(ctx, adapterAuthorityPollInterval)
		err := h.validateAdapter(checkCtx, adapter)
		cancel()
		if err != nil || ctx.Err() != nil {
			return
		}
	}
}

type fencedAdapterEventStore struct {
	store.EventStore
	handler *webSocketHandler
	adapter *adapterConnection
}

func (s fencedAdapterEventStore) Append(ctx context.Context, _ string, events []store.PendingEvent) (firstSeq int64, err error) {
	err = s.handler.withAdapterEffect(ctx, s.adapter, func() error {
		var appendErr error
		firstSeq, appendErr = s.handler.adapterAuthority.store.AppendAdapterEvents(ctx, s.adapter.sessionID, s.adapter.admission, events)
		return appendErr
	})
	return
}
