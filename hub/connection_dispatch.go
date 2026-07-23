package hub

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

const adapterAuthorityPollInterval = 250 * time.Millisecond

var errAdapterAuthorityLost = errors.New("adapter authority lost")

type adapterDispatchStore interface {
	store.AdapterConnectionTransactor
	store.AdapterGrantFenceStore
	AppendAdapterEvents(context.Context, string, store.AdapterConnectionAdmission, []store.PendingEvent) (int64, error)
}
type adapterDispatchAuthority struct {
	store             adapterDispatchStore
	adapterCredential func(context.Context, string, auth.Principal, string) (int64, int64, bool, error)
}

type admittedAdapter struct {
	admission store.AdapterConnectionAdmission
	writer    store.SettingsWriter
}

func newAdapterDispatchAuthority(handshake *Handshake, candidate any) *adapterDispatchAuthority {
	if handshake == nil {
		return nil
	}
	authenticator, authOK := handshake.authenticator.(interface {
		AdapterCredential(context.Context, string, auth.Principal, string) (int64, int64, bool, error)
	})
	dispatchStore, storeOK := candidate.(adapterDispatchStore)
	if !authOK || !storeOK || dispatchStore == nil {
		return nil
	}
	return &adapterDispatchAuthority{store: dispatchStore, adapterCredential: authenticator.AdapterCredential}
}
func (a *adapterDispatchAuthority) authenticate(ctx context.Context, token string, principal auth.Principal, sessionID string) (int64, time.Time, bool, error) {
	generation, expiresAt, allowInitialize, err := a.adapterCredential(ctx, token, principal, sessionID)
	if err != nil || generation < 1 || expiresAt <= time.Now().UnixNano() {
		return 0, time.Time{}, false, errAdapterAuthorityLost
	}
	return generation, time.Unix(0, expiresAt), allowInitialize, nil
}

func (a *adapterDispatchAuthority) admit(ctx context.Context, sessionID string, generation int64, expiresAt time.Time, allowInitialize bool) (admittedAdapter, error) {
	if generation < 1 || !expiresAt.After(time.Now()) {
		return admittedAdapter{}, errAdapterAuthorityLost
	}
	var err error
	if allowInitialize {
		if _, err = a.store.AdapterConnection(ctx, sessionID); err != nil {
			_, err = a.store.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
				SessionID: sessionID, ActiveCredentialGeneration: generation, ActiveCredentialExpiresAt: expiresAt,
			})
		}
	}
	if err != nil {
		return admittedAdapter{}, errAdapterAuthorityLost
	}
	leaseID, err := newSettingsWriterLeaseID()
	if err != nil {
		return admittedAdapter{}, errAdapterAuthorityLost
	}
	connection, err := a.store.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: generation, WriterLeaseID: leaseID})
	if err != nil {
		return admittedAdapter{}, errAdapterAuthorityLost
	}
	grantFence, err := a.store.AllocateAdapterGrantFence(ctx)
	if err != nil {
		return admittedAdapter{}, errAdapterAuthorityLost
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: generation, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence}
	return admittedAdapter{admission: admission, writer: store.SettingsWriter{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: generation, LeaseID: leaseID}}, nil
}

func newSettingsWriterLeaseID() (string, error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(random[:]), nil
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
	effectCtx, cancel := context.WithTimeout(ctx, adapterAuthorityPollInterval)
	defer cancel()
	return a.store.WithAdapterConnectionTransaction(effectCtx, func(tx store.AdapterConnectionStore) error {
		validator, ok := tx.(interface {
			ValidateAdapterEffectAdmission(context.Context, string, store.AdapterConnectionAdmission) (store.AdapterConnection, error)
		})
		if !ok {
			return errAdapterAuthorityLost
		}
		if _, err := validator.ValidateAdapterEffectAdmission(effectCtx, adapter.sessionID, adapter.admission); err != nil {
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
		h.removeAdapter(adapter)
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
	err = s.handler.withSessionPublication(ctx, s.adapter.sessionID, func() error {
		return s.handler.withAdapterEffect(ctx, s.adapter, func() error {
			var appendErr error
			firstSeq, appendErr = s.handler.adapterAuthority.store.AppendAdapterEvents(ctx, s.adapter.sessionID, s.adapter.admission, events)
			return appendErr
		})
	})
	return
}

func (s fencedAdapterEventStore) publish(ctx context.Context, batch []pendingAdapterEvent, broadcast func(context.Context, protocol.Event)) error {
	return s.handler.withSessionPublication(ctx, s.adapter.sessionID, func() error {
		return s.handler.withAdapterEffect(ctx, s.adapter, func() error {
			pending := make([]store.PendingEvent, len(batch))
			for i, item := range batch {
				pending[i] = item.pending
			}
			firstSeq, err := s.handler.adapterAuthority.store.AppendAdapterEvents(ctx, s.adapter.sessionID, s.adapter.admission, pending)
			if err != nil {
				return err
			}
			for i, item := range batch {
				seq := firstSeq + int64(i)
				event := item.event
				event.Seq = &seq
				broadcast(ctx, event)
			}
			return nil
		})
	})
}
