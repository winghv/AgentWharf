package hub

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestActivityDispatcherPublishesMinimalStoreProjectionIncludingIncompleteTruth(t *testing.T) {
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	pages := &activityPageStore{pages: []store.AttentionSummaryPage{{SnapshotAt: now, Summaries: []store.SessionAttentionSummary{
		{SessionID: "ses_complete", StateOfProjection: store.AttentionProjectionComplete, LastDurableEventAt: &now},
		{SessionID: "ses_expired", StateOfProjection: store.AttentionProjectionComplete, Blocker: &store.AttentionBlocker{ExpiresAt: timePointer(now.Add(-time.Second))}},
		{SessionID: "ses_incomplete", StateOfProjection: store.AttentionProjectionIncomplete},
	}}}}
	var published []ActivitySummary
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(_ context.Context, summary ActivitySummary) error {
		published = append(published, summary)
		return nil
	}), ActivityDispatcherConfig{})
	if err := dispatcher.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	if len(published) != 3 || published[0].SessionID != "ses_complete" || published[0].LastDurableEventAt == nil || !published[0].LastDurableEventAt.Equal(now) || published[0].StoreSnapshotAt.IsZero() {
		t.Fatalf("published = %+v", published)
	}
	if got := published[2]; got.SessionID != "ses_incomplete" || got.ProjectionState != store.AttentionProjectionIncomplete || !got.StoreSnapshotAt.Equal(now) {
		t.Fatalf("incomplete projection = %+v", got)
	}
}

func TestActivityDispatcherRunReturnsSinkFailureForSupervisor(t *testing.T) {
	pages := &activityPageStore{pages: []store.AttentionSummaryPage{{SnapshotAt: time.Now().UTC(), Summaries: []store.SessionAttentionSummary{{
		SessionID: "ses_retry", StateOfProjection: store.AttentionProjectionComplete,
	}}}}}
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(context.Context, ActivitySummary) error {
		return errors.New("sink unavailable")
	}), ActivityDispatcherConfig{})
	if err := dispatcher.Run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want sink failure for supervisor")
	}
}

func TestActivityDispatcherReplaysAfterCallbackFailure(t *testing.T) {
	pages := &activityPageStore{pages: []store.AttentionSummaryPage{{SnapshotAt: time.Now(), Summaries: []store.SessionAttentionSummary{{
		SessionID: "ses_retry", StateOfProjection: store.AttentionProjectionComplete,
	}}}}}
	attempts := 0
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(context.Context, ActivitySummary) error {
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
	pages := &activityPageStore{pages: []store.AttentionSummaryPage{{SnapshotAt: time.Now(), NextAfterSessionID: &cursor}}}
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(context.Context, ActivitySummary) error {
		return nil
	}), ActivityDispatcherConfig{})
	if err := dispatcher.DispatchOnce(context.Background()); err == nil {
		t.Fatal("unprovable page continuation was accepted")
	}
}

func TestActivityDispatcherRefreshCoalescesConcurrentRequests(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	pages := &blockingActivityPageStore{started: started, release: release}
	dispatcher := NewActivityDispatcher(pages, ActivitySinkFunc(func(context.Context, ActivitySummary) error {
		return nil
	}), ActivityDispatcherConfig{})

	first := make(chan error, 1)
	go func() { first <- dispatcher.RequestActivityRefresh(context.Background()) }()
	<-started
	second := make(chan error, 1)
	go func() { second <- dispatcher.RequestActivityRefresh(context.Background()) }()
	select {
	case err := <-second:
		t.Fatalf("coalesced request returned before scan completed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if calls := pages.calls.Load(); calls != 1 {
		t.Fatalf("Store scans = %d, want 1", calls)
	}
}

func TestActivityDispatcherRefreshReturnsStoreFailure(t *testing.T) {
	dispatcher := NewActivityDispatcher(&failingActivityPageStore{}, ActivitySinkFunc(func(context.Context, ActivitySummary) error {
		return nil
	}), ActivityDispatcherConfig{})
	if err := dispatcher.RequestActivityRefresh(context.Background()); err == nil {
		t.Fatal("refresh error = nil, want Store failure")
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

type blockingActivityPageStore struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *blockingActivityPageStore) AttentionSnapshot(context.Context, []string) ([]store.SessionAttentionSummary, error) {
	return nil, nil
}

func (s *blockingActivityPageStore) AttentionSummaryPage(context.Context, store.AttentionSummaryPageRequest) (store.AttentionSummaryPage, error) {
	s.calls.Add(1)
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return store.AttentionSummaryPage{SnapshotAt: time.Now().UTC()}, nil
}

type failingActivityPageStore struct{}

func (*failingActivityPageStore) AttentionSnapshot(context.Context, []string) ([]store.SessionAttentionSummary, error) {
	return nil, nil
}

func (*failingActivityPageStore) AttentionSummaryPage(context.Context, store.AttentionSummaryPageRequest) (store.AttentionSummaryPage, error) {
	return store.AttentionSummaryPage{}, errors.New("Store unavailable")
}
