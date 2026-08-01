package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestPendingCommandContract(t *testing.T) {
	PendingCommandContract(t, PendingCommandHarness{
		Open: func(t *testing.T) store.CommandLedgerStore {
			t.Helper()
			return &memoryCommandLedger{
				commands:  make(map[string]store.PendingCommand),
				events:    make(map[string][]store.Event),
				authority: store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1},
			}
		},
		Reopen: func(t *testing.T, current store.CommandLedgerStore) store.CommandLedgerStore {
			t.Helper()
			return current
		},
		Authority: func(t *testing.T, ledger store.CommandLedgerStore) store.CommandAuthority {
			t.Helper()
			return ledger.(*memoryCommandLedger).authority
		},
		Invalidate: func(t *testing.T, ledger store.CommandLedgerStore, kind CommandAuthorityFailure) {
			t.Helper()
			ledger.(*memoryCommandLedger).failure = kind
		},
	})
}

type memoryCommandLedger struct {
	mu        sync.Mutex
	commands  map[string]store.PendingCommand
	latest    map[string]int64
	events    map[string][]store.Event
	authority store.CommandAuthority
	failure   CommandAuthorityFailure
}

func (s *memoryCommandLedger) Append(_ context.Context, sessionID string, events []store.PendingEvent) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		s.latest = make(map[string]int64)
	}
	first := s.latest[sessionID] + 1
	s.latest[sessionID] += int64(len(events))
	return first, nil
}

func (s *memoryCommandLedger) Replay(_ context.Context, sessionID string, afterSeq int64, visit func(store.Event) error) error {
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

func (s *memoryCommandLedger) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest[sessionID], nil
}

func (s *memoryCommandLedger) CommitPendingCommand(_ context.Context, sessionID string, authority store.CommandAuthority, event store.PendingEvent, request store.PendingCommandRequest) (store.PendingCommandCommit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(authority) {
		return store.PendingCommandCommit{}, errors.New("stale command authority")
	}
	var payload struct {
		Role string `json:"role"`
	}
	if request.Type != "session.send" || event.Type != "session.message" || json.Unmarshal(event.Payload, &payload) != nil || payload.Role != "user" || !request.ExpiresAt.After(time.Now()) || request.ExpiresAt.After(time.Now().Add(30*time.Second)) {
		return store.PendingCommandCommit{}, errors.New("invalid pending command")
	}
	key := sessionID + "\x00" + request.CommandID
	if command, ok := s.commands[key]; ok {
		return store.PendingCommandCommit{Command: command, Duplicate: true}, nil
	}
	if s.latest == nil {
		s.latest = make(map[string]int64)
	}
	s.latest[sessionID]++
	command := store.PendingCommand{SessionID: sessionID, CommandID: request.CommandID, Type: request.Type, EventSeq: s.latest[sessionID], Status: store.PendingCommandPending, ExpiresAt: request.ExpiresAt}
	s.commands[key] = command
	s.events[sessionID] = append(s.events[sessionID], store.Event{SessionID: sessionID, Seq: command.EventSeq, Type: event.Type, Time: event.Time, Payload: append([]byte(nil), event.Payload...)})
	return store.PendingCommandCommit{Command: command}, nil
}

func (s *memoryCommandLedger) ClaimPendingCommand(_ context.Context, sessionID string, authority store.CommandAuthority, commandID string) (store.PendingCommandClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(authority) {
		return store.PendingCommandClaim{}, errors.New("stale command authority")
	}
	key := sessionID + "\x00" + commandID
	command, ok := s.commands[key]
	if !ok {
		return store.PendingCommandClaim{}, errors.New("pending command not found")
	}
	if !command.ExpiresAt.After(time.Now()) {
		return store.PendingCommandClaim{}, errors.New("pending command expired")
	}
	if command.Status != store.PendingCommandPending {
		return store.PendingCommandClaim{Command: command}, nil
	}
	command.Status = store.PendingCommandReceived
	s.commands[key] = command
	return store.PendingCommandClaim{Command: command, Claimed: true}, nil
}

func (s *memoryCommandLedger) ListPendingCommands(_ context.Context, sessionID string, authority store.CommandAuthority) ([]store.PendingCommand, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(authority) {
		return nil, errors.New("stale command authority")
	}
	commands := make([]store.PendingCommand, 0)
	for _, command := range s.commands {
		if command.SessionID == sessionID && (command.Status == store.PendingCommandPending || command.Status == store.PendingCommandReceived) && command.ExpiresAt.After(time.Now()) {
			commands = append(commands, command)
		}
	}
	return commands, nil
}

func (s *memoryCommandLedger) ResolvePendingCommand(_ context.Context, sessionID string, authority store.CommandAuthority, commandID string, status store.PendingCommandStatus) (store.PendingCommand, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(authority) {
		return store.PendingCommand{}, errors.New("stale command authority")
	}
	key := sessionID + "\x00" + commandID
	command, ok := s.commands[key]
	if !ok {
		return store.PendingCommand{}, errors.New("pending command not found")
	}
	if command.Status != store.PendingCommandReceived || (status != store.PendingCommandCompleted && status != store.PendingCommandOutcomeUnknown) {
		return store.PendingCommand{}, errors.New("invalid pending command outcome")
	}
	command.Status = status
	s.commands[key] = command
	return command, nil
}

func (s *memoryCommandLedger) ResolvePendingCommandUnknown(_ context.Context, sessionID string, commandID string) (store.PendingCommand, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sessionID + "\x00" + commandID
	command, ok := s.commands[key]
	if !ok || command.Status != store.PendingCommandReceived {
		return store.PendingCommand{}, errors.New("pending command is not received")
	}
	command.Status = store.PendingCommandOutcomeUnknown
	s.commands[key] = command
	return command, nil
}

func (s *memoryCommandLedger) authorized(authority store.CommandAuthority) bool {
	return s.failure == "" && authority == s.authority
}
