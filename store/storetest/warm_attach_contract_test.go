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

func TestWarmAttachContract(t *testing.T) {
	WarmAttachContract(t, WarmAttachHarness{
		Open: func(t *testing.T) store.WarmAttachStore {
			t.Helper()
			return &memoryWarmAttachStore{
				records:   make(map[[32]byte]memoryWarmAttachRecord),
				summaries: make(map[string]store.SessionAttentionSummary),
				latest:    make(map[string]int64),
			}
		},
		Fail: func(t *testing.T, warm store.WarmAttachStore, failure WarmAttachFailure) {
			t.Helper()
			warm.(*memoryWarmAttachStore).failure = failure
		},
		Expire: func(t *testing.T, warm store.WarmAttachStore) {
			t.Helper()
			memory := warm.(*memoryWarmAttachStore)
			memory.mu.Lock()
			memory.now = time.Now().Add(2 * time.Minute)
			memory.mu.Unlock()
		},
		Absent: func(t *testing.T, warm store.WarmAttachStore, request store.WarmAttachRequest) {
			t.Helper()
			memory := warm.(*memoryWarmAttachStore)
			memory.mu.Lock()
			defer memory.mu.Unlock()
			if _, exists := memory.records[request.Attempt.Identity.JTIHash]; exists {
				t.Fatal("rolled-back attach attempt remained durable")
			}
			if len(memory.records) != 0 || memory.latest[request.Attachment.Identity.TargetSessionID] != 0 {
				t.Fatalf("rollback left attachment, lineage, outbox, or event sequence: %+v", memory.records)
			}
			if _, exists := memory.summaries[request.Attachment.Identity.TargetSessionID]; exists {
				t.Fatal("rolled-back attention blocker remained durable")
			}
		},
	})
}

type memoryWarmAttachRecord struct {
	request store.WarmAttachRequest
	commit  store.WarmAttachCommit
}

type memoryWarmAttachStore struct {
	mu        sync.Mutex
	records   map[[32]byte]memoryWarmAttachRecord
	summaries map[string]store.SessionAttentionSummary
	latest    map[string]int64
	failure   WarmAttachFailure
	now       time.Time
}

func (s *memoryWarmAttachStore) Append(context.Context, string, []store.PendingEvent) (int64, error) {
	return 0, errors.New("events are internal to warm attach")
}

func (s *memoryWarmAttachStore) Replay(context.Context, string, int64, func(store.Event) error) error {
	return errors.New("events are internal to warm attach")
}

func (s *memoryWarmAttachStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest[sessionID], nil
}

func (s *memoryWarmAttachStore) AttentionSnapshot(_ context.Context, sessionIDs []string) ([]store.SessionAttentionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	summaries := make([]store.SessionAttentionSummary, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		if summary, ok := s.summaries[sessionID]; ok {
			summaries = append(summaries, cloneWarmAttachSummary(summary))
		}
	}
	return summaries, nil
}

func (s *memoryWarmAttachStore) CommitWarmAttach(_ context.Context, request store.WarmAttachRequest) (store.WarmAttachCommit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.validRequest(request) {
		return store.WarmAttachCommit{}, errors.New("invalid warm attach")
	}
	if record, ok := s.records[request.Attempt.Identity.JTIHash]; ok {
		if !reflect.DeepEqual(record.request, request) {
			return store.WarmAttachCommit{}, errors.New("warm attach is immutable")
		}
		result := cloneWarmAttachCommit(record.commit)
		result.Duplicate = true
		return result, nil
	}
	if s.failure == WarmAttachFailureAttempt {
		return store.WarmAttachCommit{}, errors.New("warm attach failpoint")
	}
	now := s.clock()
	expiresAt := request.Attachment.ExpiresAt
	issuedGeneration := *request.Attempt.IssuedCredentialGeneration
	attachment := store.Attachment{
		Identity: request.Attachment.Identity, Status: store.AttachmentJoinPending,
		DeliveryState: store.AttachmentDeliveryPending, ExpiresAt: &expiresAt,
	}
	if s.failure == WarmAttachFailureAttachment {
		return store.WarmAttachCommit{}, errors.New("warm attach failpoint")
	}
	eventSeq := s.latest[attachment.Identity.TargetSessionID] + 1
	outbox := store.WarmAttachOutbox{
		TargetSessionID: attachment.Identity.TargetSessionID, CommandID: request.FirstDelivery.CommandID,
		EventSeq: eventSeq, ReferenceID: request.FirstDelivery.ReferenceID,
		ReferenceDigest: request.FirstDelivery.ReferenceDigest, ExpiresAt: request.FirstDelivery.ExpiresAt,
	}
	if s.failure == WarmAttachFailureOutbox {
		return store.WarmAttachCommit{}, errors.New("warm attach failpoint")
	}
	reason := "join_pending"
	lastDurableEventAt := now
	lastClientCommandAt := now
	summary := store.SessionAttentionSummary{
		SessionID: attachment.Identity.TargetSessionID, State: "ready", LatestSeq: eventSeq,
		Blocker:        &store.AttentionBlocker{Kind: store.AttentionBlockerQueued, Reason: &reason, ExpiresAt: &expiresAt},
		SummaryVersion: 1, LastDurableEventAt: &lastDurableEventAt, LastClientCommandAt: &lastClientCommandAt,
		StateOfProjection: store.AttentionProjectionComplete,
	}
	if s.failure == WarmAttachFailureSummary {
		return store.WarmAttachCommit{}, errors.New("warm attach failpoint")
	}
	commit := store.WarmAttachCommit{
		Attempt: store.AttachAttempt{
			Identity: request.Attempt.Identity, Fingerprint: request.Attempt.Fingerprint, ExpiresAt: request.Attempt.ExpiresAt,
			Outcome: request.Attempt.Outcome, IssuedCredentialGeneration: &issuedGeneration,
		},
		Attachment: attachment,
		Outbox:     outbox,
		Summary:    summary,
	}
	s.records[request.Attempt.Identity.JTIHash] = memoryWarmAttachRecord{request: request, commit: cloneWarmAttachCommit(commit)}
	s.summaries[attachment.Identity.TargetSessionID] = cloneWarmAttachSummary(summary)
	s.latest[attachment.Identity.TargetSessionID] = eventSeq
	return cloneWarmAttachCommit(commit), nil
}

func (s *memoryWarmAttachStore) ExpireWarmAttach(_ context.Context, attachID string, expectedDeliveryVersion int64) (store.WarmAttachExpiry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hash, record := range s.records {
		if record.commit.Attachment.Identity.AttachID != attachID {
			continue
		}
		if record.commit.Attachment.DeliveryVersion != expectedDeliveryVersion || !record.commit.Attachment.ExpiresAt.Before(s.clock()) {
			return store.WarmAttachExpiry{}, errors.New("warm attach is not expirable")
		}
		attachment := record.commit.Attachment
		attachment.Status = store.AttachmentReauthorizationRequired
		attachment.DeliveryVersion++
		attachment.ExpiresAt = nil
		summary := cloneWarmAttachSummary(record.commit.Summary)
		summary.SummaryVersion++
		summary.Blocker = &store.AttentionBlocker{Kind: store.AttentionBlockerReauthorizationRequired}
		record.commit.Attachment = attachment
		record.commit.Summary = summary
		s.records[hash] = record
		s.summaries[attachment.Identity.TargetSessionID] = cloneWarmAttachSummary(summary)
		return store.WarmAttachExpiry{Attachment: attachment, Summary: summary}, nil
	}
	return store.WarmAttachExpiry{}, errors.New("warm attach not found")
}

func (s *memoryWarmAttachStore) validRequest(request store.WarmAttachRequest) bool {
	if request.Attempt.Identity.JTIHash == ([32]byte{}) || request.Attempt.Outcome != store.AttachAttemptAccepted || request.Attempt.IssuedCredentialGeneration == nil || *request.Attempt.IssuedCredentialGeneration < 1 || request.Attempt.Identity.AttachID == "" || request.Attempt.Identity.BootstrapSessionID == "" || request.Attempt.Identity.TargetSessionID == "" || request.Attempt.Identity.BootstrapSessionID == request.Attempt.Identity.TargetSessionID || request.Attempt.Fingerprint.Domain != "agentwharf.attach-request.v1" || request.Attempt.Fingerprint.Version != 1 || request.Attempt.Fingerprint.KeyVersion < 1 {
		return false
	}
	if request.Attachment.Identity.AttachID != request.Attempt.Identity.AttachID || request.Attachment.Identity.BootstrapSessionID != request.Attempt.Identity.BootstrapSessionID || request.Attachment.Identity.TargetSessionID != request.Attempt.Identity.TargetSessionID || request.Attachment.Identity.TargetCredentialLineageRef == "" || request.BootstrapAdmission.CredentialGeneration < 1 || request.BootstrapAdmission.ConnectionEpoch < 1 || request.BootstrapAdmission.GrantFence <= request.BootstrapAdmission.AcceptedFence || request.FirstDelivery.CommandID == "" || request.FirstDelivery.ReferenceID == "" {
		return false
	}
	now := s.clock()
	return request.Attempt.ExpiresAt.After(now) && request.Attachment.ExpiresAt.After(now) && request.FirstDelivery.ExpiresAt.Equal(request.Attachment.ExpiresAt) && !request.Attachment.ExpiresAt.After(request.Attempt.ExpiresAt)
}

func (s *memoryWarmAttachStore) clock() time.Time {
	if s.now.IsZero() {
		return time.Now()
	}
	return s.now
}

func cloneWarmAttachCommit(commit store.WarmAttachCommit) store.WarmAttachCommit {
	if commit.Attempt.IssuedCredentialGeneration != nil {
		generation := *commit.Attempt.IssuedCredentialGeneration
		commit.Attempt.IssuedCredentialGeneration = &generation
	}
	if commit.Attachment.ExpiresAt != nil {
		expiresAt := *commit.Attachment.ExpiresAt
		commit.Attachment.ExpiresAt = &expiresAt
	}
	commit.Summary = cloneWarmAttachSummary(commit.Summary)
	return commit
}

func cloneWarmAttachSummary(summary store.SessionAttentionSummary) store.SessionAttentionSummary {
	if summary.LastDurableEventAt != nil {
		value := *summary.LastDurableEventAt
		summary.LastDurableEventAt = &value
	}
	if summary.LastClientCommandAt != nil {
		value := *summary.LastClientCommandAt
		summary.LastClientCommandAt = &value
	}
	if summary.Blocker != nil {
		blocker := *summary.Blocker
		if blocker.Reason != nil {
			value := *blocker.Reason
			blocker.Reason = &value
		}
		if blocker.ExpiresAt != nil {
			value := *blocker.ExpiresAt
			blocker.ExpiresAt = &value
		}
		summary.Blocker = &blocker
	}
	return summary
}
