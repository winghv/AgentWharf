package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

type controlEventStore struct {
	store.EncryptedProposedEventStore
	calls int
}

type controlEventAuthority struct{ adapterDispatchStore }

func (s *controlEventAuthority) ValidateAdapterAdmission(context.Context, string, store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	return store.AdapterConnection{}, nil
}

var errOpaqueControlCommit = errors.New("synthetic opaque commit failure")

func (s *controlEventStore) CommitEncryptedProposedEvent(context.Context, string, store.CommandAuthority, store.ProposedEventRequest) (store.ProposedEventReceipt, error) {
	s.calls++
	return store.ProposedEventReceipt{}, errOpaqueControlCommit
}

func TestEncryptedControlProposalBypassesPlaintextParsers(t *testing.T) {
	// A cancelled write context prevents the error-report transport from writing
	// to a socket; the Store probe independently confirms the chosen commit path.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"session.settings.capabilities", "session.settings.effective", "session.run.capabilities", "session.run.outcome", "session.file_references.capabilities", "session.file_references.outcome"} {
		t.Run(kind, func(t *testing.T) {
			payload := json.RawMessage(`{"private":"endpoint-owned"}`)
			if kind == "session.run.outcome" {
				payload = json.RawMessage(`{"cmd_id":"command","operation":"stop","outcome":"completed","completion_state":"ended","reason_code":null}`)
			}
			public, err := e2ee.ProjectPublicMetadata(kind, payload)
			if err != nil {
				t.Fatal(err)
			}
			packet, err := e2ee.SealPacket(e2ee.Context{Scope: "event", Session: "session", Sender: "machine", KeyID: "key", MessageID: "message", Type: kind}, make([]byte, 32), private, public, payload)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := json.Marshal(e2ee.EventWire{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: "message", Type: kind, Packet: packet})
			if err != nil {
				t.Fatal(err)
			}
			backend := &controlEventStore{}
			adapter := &adapterConnection{sessionID: "session", contentMode: protocol.ContentModeRequired}
			handler := &webSocketHandler{events: backend, adapterAuthority: &adapterDispatchAuthority{store: &controlEventAuthority{}}, adapters: map[string]*adapterConnection{"session": adapter}}
			err = handler.handleAdapterEvent(ctx, adapter, AcceptedPeer{SessionID: "session", ProtocolVersion: 2, ContentMode: protocol.ContentModeRequired}, &protocol.Event{SessionID: "session", Type: kind, Time: time.Now().UnixMilli(), ProposalID: "proposal", Payload: wire})
			if !errors.Is(err, errOpaqueControlCommit) || backend.calls != 1 {
				t.Fatalf("control event parsed instead of opaque commit: %v calls=%d", err, backend.calls)
			}
		})
	}
}
