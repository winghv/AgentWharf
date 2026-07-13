package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestPendingCommandContract(t *testing.T) {
	PendingCommandContract(t, PendingCommandHarness{Open: func(t *testing.T) store.CommandLedgerStore {
		t.Helper()
		return &memoryCommandLedger{commands: make(map[string]store.PendingCommand)}
	}})
}

type memoryCommandLedger struct {
	mu       sync.Mutex
	commands map[string]store.PendingCommand
	latest   map[string]int64
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

func (s *memoryCommandLedger) Replay(context.Context, string, int64, func(store.Event) error) error {
	return nil
}

func (s *memoryCommandLedger) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest[sessionID], nil
}

func (s *memoryCommandLedger) CommitPendingCommand(_ context.Context, sessionID string, event store.PendingEvent, request store.PendingCommandRequest) (store.PendingCommandCommit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Type != "session.send" || event.Type != "session.message" || !request.ExpiresAt.After(time.Now()) {
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
	return store.PendingCommandCommit{Command: command}, nil
}

func (s *memoryCommandLedger) ClaimPendingCommand(_ context.Context, sessionID string, commandID string) (store.PendingCommandClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sessionID + "\x00" + commandID
	command, ok := s.commands[key]
	if !ok {
		return store.PendingCommandClaim{}, errors.New("pending command not found")
	}
	if command.Status != store.PendingCommandPending {
		return store.PendingCommandClaim{Command: command}, nil
	}
	command.Status = store.PendingCommandReceived
	s.commands[key] = command
	return store.PendingCommandClaim{Command: command, Claimed: true}, nil
}

func (s *memoryCommandLedger) ResolvePendingCommand(_ context.Context, sessionID string, commandID string, status store.PendingCommandStatus) (store.PendingCommand, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := sessionID + "\x00" + commandID
	command, ok := s.commands[key]
	if !ok {
		return store.PendingCommand{}, errors.New("pending command not found")
	}
	command.Status = status
	s.commands[key] = command
	return command, nil
}
