package hub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestActivityDispatcherPublishesOnlyCompleteNonExpiredStoreTruth(t *testing.T) {
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	pages := &activityPageStore{pages: []store.AttentionSummaryPage{{Summaries: []store.SessionAttentionSummary{
		{SessionID: "ses_complete", StateOfProjection: store.AttentionProjectionComplete, LastDurableEventAt: &now},
		{SessionID: "ses_expired", StateOfProjection: store.AttentionProjectionComplete, Blocker: &store.AttentionBlocker{ExpiresAt: timePointer(now.Add(-time.Second))}},
		{SessionID: "ses_incomplete", StateOfProjection: store.AttentionProjectionIncomplete},
	}}}}
	var published []store.SessionAttentionSummary
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(_ context.Context, summary store.SessionAttentionSummary) error {
		published = append(published, summary)
		return nil
	}), ActivityDispatcherConfig{Now: func() time.Time { return now }})
	if err := dispatcher.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if len(published) != 1 || published[0].SessionID != "ses_complete" || published[0].LastDurableEventAt == nil || !published[0].LastDurableEventAt.Equal(now) {
		t.Fatalf("published = %+v", published)
	}
}

func TestActivityDispatcherReplaysAfterCallbackFailure(t *testing.T) {
	pages := &activityPageStore{pages: []store.AttentionSummaryPage{{Summaries: []store.SessionAttentionSummary{{
		SessionID: "ses_retry", StateOfProjection: store.AttentionProjectionComplete,
	}}}}}
	attempts := 0
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(context.Context, store.SessionAttentionSummary) error {
		attempts++
		if attempts == 1 {
			return errors.New("sink unavailable")
		}
		return nil
	}), ActivityDispatcherConfig{})
	if err := dispatcher.DispatchOnce(context.Background()); err == nil {
		t.Fatal("callback failure was accepted")
	}
	if err := dispatcher.DispatchOnce(context.Background()); err != nil || attempts != 2 {
		t.Fatalf("retry error=%v attempts=%d", err, attempts)
	}
}

func TestActivityDispatcherRejectsUnprovablePageContinuation(t *testing.T) {
	cursor := "ses_skipped"
	pages := &activityPageStore{pages: []store.AttentionSummaryPage{{NextAfterSessionID: &cursor}}}
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(context.Context, store.SessionAttentionSummary) error {
		return nil
	}), ActivityDispatcherConfig{})
	if err := dispatcher.DispatchOnce(context.Background()); err == nil {
		t.Fatal("unprovable page continuation was accepted")
	}
}

type activityPageStore struct {
	pages []store.AttentionSummaryPage
	index int
}

func (s *activityPageStore) AttentionSnapshot(context.Context, []string) ([]store.SessionAttentionSummary, error) {
	return nil, nil
}
func (s *activityPageStore) AttentionSummaryPage(_ context.Context, _ store.AttentionSummaryPageRequest) (store.AttentionSummaryPage, error) {
	page := s.pages[s.index]
	if s.index < len(s.pages)-1 {
		s.index++
	}
	return page, nil
}

func timePointer(value time.Time) *time.Time { return &value }
