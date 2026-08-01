package storetest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestAttachAttemptContract(t *testing.T) {
	AttachAttemptContract(t, AttachAttemptHarness{
		Open: func(t *testing.T) store.AttachAttemptStore {
			t.Helper()
			return &memoryAttachAttemptStore{attempts: make(map[[32]byte]store.AttachAttempt)}
		},
		Reopen: func(t *testing.T, current store.AttachAttemptStore) store.AttachAttemptStore {
			t.Helper()
			return current
		},
	})
}

type memoryAttachAttemptStore struct {
	mu       sync.Mutex
	attempts map[[32]byte]store.AttachAttempt
}

func (s *memoryAttachAttemptStore) Append(context.Context, string, []store.PendingEvent) (int64, error) {
	return 0, errors.New("events are outside the attach-attempt contract")
}

func (s *memoryAttachAttemptStore) Replay(context.Context, string, int64, func(store.Event) error) error {
	return errors.New("events are outside the attach-attempt contract")
}

func (s *memoryAttachAttemptStore) LatestSeq(context.Context, string) (int64, error) { return 0, nil }

func (s *memoryAttachAttemptStore) CommitAttachAttempt(_ context.Context, request store.AttachAttemptRequest) (store.AttachAttemptCommit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validAttachAttemptRequest(request) {
		return store.AttachAttemptCommit{}, errors.New("invalid attach attempt")
	}
	attempt := attachAttemptFromRequest(request)
	if current, ok := s.attempts[request.Identity.JTIHash]; ok {
		if !reflect.DeepEqual(current, attempt) {
			return store.AttachAttemptCommit{}, errors.New("attach attempt is immutable")
		}
		return store.AttachAttemptCommit{Attempt: cloneAttachAttempt(current), Duplicate: true}, nil
	}
	s.attempts[request.Identity.JTIHash] = attempt
	return store.AttachAttemptCommit{Attempt: cloneAttachAttempt(attempt)}, nil
}

func (s *memoryAttachAttemptStore) AttachAttempt(_ context.Context, jtiHash [32]byte) (store.AttachAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt, ok := s.attempts[jtiHash]
	if !ok {
		return store.AttachAttempt{}, errors.New("attach attempt not found")
	}
	return cloneAttachAttempt(attempt), nil
}

func validAttachAttemptRequest(request store.AttachAttemptRequest) bool {
	identity := request.Identity
	if identity.JTIHash == ([32]byte{}) || len(identity.AttachID) == 0 || len(identity.AttachID) > 255 || len(identity.BootstrapSessionID) == 0 || len(identity.BootstrapSessionID) > 255 || len(identity.TargetSessionID) == 0 || len(identity.TargetSessionID) > 255 || identity.BootstrapSessionID == identity.TargetSessionID || len(identity.Provider) == 0 || len(identity.Provider) > 128 {
		return false
	}
	fingerprint := request.Fingerprint
	if fingerprint.Domain != "agentwharf.attach-request.v1" || fingerprint.Version != 1 || fingerprint.KeyVersion < 1 {
		return false
	}
	now := time.Now()
	if !request.ExpiresAt.After(now) || request.ExpiresAt.After(now.Add(5*time.Minute)) {
		return false
	}
	return (request.Outcome == store.AttachAttemptAccepted && request.IssuedCredentialGeneration != nil && *request.IssuedCredentialGeneration > 0) || (request.Outcome == store.AttachAttemptRejected && request.IssuedCredentialGeneration == nil)
}

func attachAttemptFromRequest(request store.AttachAttemptRequest) store.AttachAttempt {
	return cloneAttachAttempt(store.AttachAttempt(request))
}

func cloneAttachAttempt(attempt store.AttachAttempt) store.AttachAttempt {
	if attempt.IssuedCredentialGeneration != nil {
		generation := *attempt.IssuedCredentialGeneration
		attempt.IssuedCredentialGeneration = &generation
	}
	return attempt
}
