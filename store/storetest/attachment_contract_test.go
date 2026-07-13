package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestAttachmentContract(t *testing.T) {
	AttachmentContract(t, AttachmentHarness{
		Open: func(t *testing.T) store.AttachmentStore {
			t.Helper()
			return &memoryAttachmentStore{
				attachments: make(map[string]store.Attachment),
				byTarget:    make(map[string]string),
			}
		},
		Reopen: func(t *testing.T, current store.AttachmentStore) store.AttachmentStore {
			t.Helper()
			return current
		},
	})
}

type memoryAttachmentStore struct {
	mu          sync.Mutex
	attachments map[string]store.Attachment
	byTarget    map[string]string
}

func (s *memoryAttachmentStore) Append(_ context.Context, _ string, _ []store.PendingEvent) (int64, error) {
	return 0, errors.New("events are outside the attachment contract")
}

func (s *memoryAttachmentStore) Replay(_ context.Context, _ string, _ int64, _ func(store.Event) error) error {
	return errors.New("events are outside the attachment contract")
}

func (s *memoryAttachmentStore) LatestSeq(_ context.Context, _ string) (int64, error) {
	return 0, nil
}

func (s *memoryAttachmentStore) CreateAttachment(_ context.Context, request store.AttachmentCreate) (store.AttachmentCommit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if request.Identity.AttachID == "" || request.Identity.BootstrapSessionID == "" || request.Identity.TargetSessionID == "" || request.Identity.TargetCredentialLineageRef == "" || request.Identity.BootstrapSessionID == request.Identity.TargetSessionID || !validAttachmentExpiry(request.ExpiresAt) {
		return store.AttachmentCommit{}, errors.New("invalid attachment create")
	}
	if attachment, ok := s.attachments[request.Identity.AttachID]; ok {
		if attachment.Identity != request.Identity {
			return store.AttachmentCommit{}, errors.New("attachment identity is immutable")
		}
		return store.AttachmentCommit{Attachment: cloneAttachment(attachment), Summary: attachmentSummary(attachment, nil), Noop: true}, nil
	}
	if attachID, ok := s.byTarget[request.Identity.TargetSessionID]; ok {
		return store.AttachmentCommit{}, errors.New("target attachment already exists: " + attachID)
	}
	expiresAt := request.ExpiresAt
	attachment := store.Attachment{
		Identity:      request.Identity,
		Status:        store.AttachmentJoinPending,
		DeliveryState: store.AttachmentDeliveryPending,
		ExpiresAt:     &expiresAt,
	}
	s.attachments[attachment.Identity.AttachID] = attachment
	s.byTarget[attachment.Identity.TargetSessionID] = attachment.Identity.AttachID
	return store.AttachmentCommit{Attachment: cloneAttachment(attachment), Summary: attachmentSummary(attachment, nil)}, nil
}

func (s *memoryAttachmentStore) Attachment(_ context.Context, attachID string) (store.Attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attachment, ok := s.attachments[attachID]
	if !ok {
		return store.Attachment{}, errors.New("attachment not found")
	}
	return cloneAttachment(attachment), nil
}

func (s *memoryAttachmentStore) AttachmentForTarget(_ context.Context, targetSessionID string) (store.Attachment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attachID, ok := s.byTarget[targetSessionID]
	if !ok {
		return store.Attachment{}, errors.New("target attachment not found")
	}
	return cloneAttachment(s.attachments[attachID]), nil
}

func (s *memoryAttachmentStore) UpdateAttachment(_ context.Context, attachID string, expectedVersion int64, update store.AttachmentUpdate) (store.AttachmentMutation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attachment, ok := s.attachments[attachID]
	if !ok {
		return store.AttachmentMutation{}, errors.New("attachment not found")
	}
	if attachment.DeliveryVersion != expectedVersion {
		return store.AttachmentMutation{}, errors.New("attachment version conflict")
	}
	if attachment.Status == store.AttachmentStartReceived || attachment.Status == store.AttachmentCanceled {
		return store.AttachmentMutation{}, errors.New("terminal attachment cannot be reopened")
	}
	if !validAttachmentUpdate(attachment, update) {
		return store.AttachmentMutation{}, errors.New("invalid attachment update")
	}
	attachment.Status = update.Status
	attachment.DeliveryState = update.DeliveryState
	attachment.QueueReason = cloneString(update.QueueReason)
	attachment.ExpiresAt = cloneTime(update.ExpiresAt)
	attachment.BlockingSessionID = cloneString(update.BlockingSessionID)
	if update.Status == store.AttachmentCanceled {
		now := time.Now()
		attachment.CanceledAt = &now
	} else {
		attachment.CanceledAt = nil
	}
	attachment.DeliveryVersion++
	s.attachments[attachID] = attachment
	return store.AttachmentMutation{Attachment: cloneAttachment(attachment), Summary: attachmentSummary(attachment, update.Blocker)}, nil
}

func validAttachmentExpiry(expiresAt time.Time) bool {
	return expiresAt.After(time.Now()) && !expiresAt.After(time.Now().Add(30*time.Second))
}

func validAttachmentUpdate(current store.Attachment, update store.AttachmentUpdate) bool {
	if update.ExpiresAt != nil && (!validAttachmentExpiry(*update.ExpiresAt) || (current.ExpiresAt != nil && update.ExpiresAt.After(*current.ExpiresAt))) {
		return false
	}
	summaryMatches := func(kind store.AttachmentBlockerKind, allowReason, allowExpiry, allowBlocker, allowOperation bool) bool {
		blocker := update.Blocker
		if blocker == nil || blocker.Kind != kind || (!allowReason && blocker.Reason != nil) || (!allowExpiry && blocker.ExpiresAt != nil) || (!allowBlocker && blocker.BlockingSessionID != nil) || (!allowOperation && blocker.Operation != nil) {
			return false
		}
		if allowReason && (blocker.Reason == nil || update.QueueReason == nil || *blocker.Reason != *update.QueueReason) {
			return false
		}
		if allowExpiry && (blocker.ExpiresAt == nil || update.ExpiresAt == nil || !blocker.ExpiresAt.Equal(*update.ExpiresAt)) {
			return false
		}
		if allowBlocker && (blocker.BlockingSessionID == nil || update.BlockingSessionID == nil || *blocker.BlockingSessionID != *update.BlockingSessionID) {
			return false
		}
		return true
	}

	switch update.Status {
	case store.AttachmentQueued:
		return update.DeliveryState == store.AttachmentDeliveryPending && update.QueueReason != nil && update.ExpiresAt != nil && update.BlockingSessionID != nil && summaryMatches(store.AttachmentBlockerQueued, true, true, true, false)
	case store.AttachmentStartReceived:
		if update.QueueReason != nil || update.ExpiresAt != nil || update.BlockingSessionID != nil {
			return false
		}
		if update.DeliveryState == store.AttachmentDeliveryOutcomeUnknown {
			return summaryMatches(store.AttachmentBlockerOutcomeUnknown, false, false, false, true) && update.Blocker.Operation != nil && (*update.Blocker.Operation == "start" || *update.Blocker.Operation == "command")
		}
		return (update.DeliveryState == store.AttachmentDeliveryReceived || update.DeliveryState == store.AttachmentDeliveryCompleted) && update.Blocker == nil
	case store.AttachmentReauthorizationRequired:
		if update.QueueReason != nil || update.ExpiresAt != nil || update.BlockingSessionID != nil {
			return false
		}
		if update.DeliveryState == store.AttachmentDeliveryOutcomeUnknown {
			return summaryMatches(store.AttachmentBlockerOutcomeUnknown, false, false, false, true)
		}
		return update.DeliveryState == store.AttachmentDeliveryPending && summaryMatches(store.AttachmentBlockerReauthorizationRequired, false, false, false, false)
	case store.AttachmentCanceled:
		return update.QueueReason == nil && update.ExpiresAt == nil && update.BlockingSessionID == nil && summaryMatches(store.AttachmentBlockerNewRunRequired, false, false, false, false)
	default:
		return false
	}
}

func attachmentSummary(attachment store.Attachment, blocker *store.AttachmentBlocker) store.AttachmentSummary {
	return store.AttachmentSummary{
		AttachID:        attachment.Identity.AttachID,
		TargetSessionID: attachment.Identity.TargetSessionID,
		DeliveryVersion: attachment.DeliveryVersion,
		ExpiresAt:       cloneTime(attachment.ExpiresAt),
		Blocker:         cloneBlocker(blocker),
	}
}

func cloneAttachment(attachment store.Attachment) store.Attachment {
	attachment.QueueReason = cloneString(attachment.QueueReason)
	attachment.ExpiresAt = cloneTime(attachment.ExpiresAt)
	attachment.CanceledAt = cloneTime(attachment.CanceledAt)
	attachment.BlockingSessionID = cloneString(attachment.BlockingSessionID)
	return attachment
}

func cloneBlocker(blocker *store.AttachmentBlocker) *store.AttachmentBlocker {
	if blocker == nil {
		return nil
	}
	copy := *blocker
	copy.Reason = cloneString(blocker.Reason)
	copy.ExpiresAt = cloneTime(blocker.ExpiresAt)
	copy.BlockingSessionID = cloneString(blocker.BlockingSessionID)
	copy.Operation = cloneString(blocker.Operation)
	return &copy
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
