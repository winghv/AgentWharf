package storetest

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	"github.com/winghv/agentwharf/store"
)

func TestProposalContract(t *testing.T) {
	ProposalContract(t, ProposalHarness{
		Open: func(t *testing.T) store.ProposedEventStore {
			t.Helper()
			return &memoryProposalStore{proposals: make(map[string]memoryProposal), events: make(map[string][]store.Event), authority: store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}}
		},
		Reopen: func(t *testing.T, current store.ProposedEventStore) store.ProposedEventStore { return current },
		Authority: func(t *testing.T, proposals store.ProposedEventStore) store.CommandAuthority {
			return proposals.(*memoryProposalStore).authority
		},
		Invalidate: func(t *testing.T, proposals store.ProposedEventStore, kind CommandAuthorityFailure) {
			proposals.(*memoryProposalStore).failure = kind
		},
	})
}

type memoryProposal struct {
	receipt  store.ProposedEventReceipt
	typeName string
	digest   [sha256.Size]byte
}

type memoryProposalStore struct {
	mu        sync.Mutex
	proposals map[string]memoryProposal
	events    map[string][]store.Event
	authority store.CommandAuthority
	failure   CommandAuthorityFailure
}

func (s *memoryProposalStore) Append(_ context.Context, sessionID string, events []store.PendingEvent) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := int64(len(s.events[sessionID]) + 1)
	for index, event := range events {
		s.events[sessionID] = append(s.events[sessionID], store.Event{SessionID: sessionID, Seq: first + int64(index), Type: event.Type, Time: event.Time, Payload: append([]byte(nil), event.Payload...)})
	}
	return first, nil
}

func (s *memoryProposalStore) Replay(_ context.Context, sessionID string, afterSeq int64, visit func(store.Event) error) error {
	s.mu.Lock()
	events := append([]store.Event(nil), s.events[sessionID]...)
	s.mu.Unlock()
	for _, event := range events {
		if event.Seq > afterSeq {
			if err := visit(event); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *memoryProposalStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.events[sessionID])), nil
}

func (s *memoryProposalStore) CommitProposedEvent(_ context.Context, sessionID string, authority store.CommandAuthority, request store.ProposedEventRequest) (store.ProposedEventReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != "" || authority != s.authority {
		return store.ProposedEventReceipt{}, errors.New("stale proposal authority")
	}
	if request.ProposalID == "" || request.Event.Type == "" {
		return store.ProposedEventReceipt{}, errors.New("invalid proposal")
	}
	key := sessionID + "\x00" + request.ProposalID
	digest := sha256.Sum256(request.Event.Payload)
	if existing, ok := s.proposals[key]; ok {
		if existing.typeName != request.Event.Type || existing.digest != digest {
			return store.ProposedEventReceipt{}, errors.New("conflicting proposal")
		}
		return existing.receipt, nil
	}
	seq := int64(len(s.events[sessionID]) + 1)
	receipt := store.ProposedEventReceipt{SessionID: sessionID, ProposalID: request.ProposalID, Seq: seq, Status: store.ProposedEventAccepted}
	s.proposals[key] = memoryProposal{receipt: receipt, typeName: request.Event.Type, digest: digest}
	s.events[sessionID] = append(s.events[sessionID], store.Event{SessionID: sessionID, Seq: seq, Type: request.Event.Type, Time: request.Event.Time, Payload: append([]byte(nil), request.Event.Payload...)})
	return receipt, nil
}
