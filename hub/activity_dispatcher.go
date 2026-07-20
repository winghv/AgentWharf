package hub

import (
	"context"
	"errors"
	"time"

	"github.com/winghv/agentwharf/store"
)

const defaultActivityDispatchInterval = time.Minute

// ActivitySink receives provider-neutral, Store-committed activity summaries.
// It deliberately has no Session command, credential, provider, Task, or VM
// input so dispatch cannot become another source of activity truth.
type ActivitySink interface {
	PublishActivitySummary(context.Context, store.SessionAttentionSummary) error
}

type ActivitySinkFunc func(context.Context, store.SessionAttentionSummary) error

func (fn ActivitySinkFunc) PublishActivitySummary(ctx context.Context, summary store.SessionAttentionSummary) error {
	return fn(ctx, summary)
}

type ActivityDispatcherConfig struct {
	Interval time.Duration
	Now      func() time.Time
}

// ActivityDispatcher performs bounded keyset rescans. It has one ticker for
// the whole Store and creates no Session-specific timers or goroutines.
type ActivityDispatcher struct {
	pages    store.AttentionSummaryPageStore
	sink     ActivitySink
	interval time.Duration
	now      func() time.Time
}

func NewActivityDispatcher(pages store.AttentionSummaryPageStore, sink ActivitySink, cfg ActivityDispatcherConfig) *ActivityDispatcher {
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultActivityDispatchInterval
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &ActivityDispatcher{pages: pages, sink: sink, interval: interval, now: now}
}

func (d *ActivityDispatcher) Run(ctx context.Context) error {
	if err := d.DispatchOnce(ctx); errors.Is(err, context.Canceled) {
		return err
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.DispatchOnce(ctx); errors.Is(err, context.Canceled) {
				return err
			}
		}
	}
}

func (d *ActivityDispatcher) DispatchOnce(ctx context.Context) error {
	if d == nil || d.pages == nil || d.sink == nil {
		return errors.New("activity dispatcher is not configured")
	}
	after := ""
	for {
		page, err := d.pages.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{
			AfterSessionID: after,
			Limit:          store.MaxAttentionSummaryPageSize,
		})
		if err != nil {
			return err
		}
		for _, summary := range page.Summaries {
			if summary.SessionID <= after {
				return errors.New("activity summary page is not strictly ordered")
			}
			after = summary.SessionID
			if summary.StateOfProjection != store.AttentionProjectionComplete || attentionBlockerExpired(summary.Blocker, d.now()) {
				continue
			}
			if err := d.sink.PublishActivitySummary(ctx, summary); err != nil {
				return err
			}
		}
		if page.NextAfterSessionID == nil {
			return nil
		}
		if len(page.Summaries) == 0 || *page.NextAfterSessionID == "" || *page.NextAfterSessionID != after {
			return errors.New("activity summary page continuation is invalid")
		}
	}
}

func attentionBlockerExpired(blocker *store.AttentionBlocker, now time.Time) bool {
	return blocker != nil && blocker.ExpiresAt != nil && !blocker.ExpiresAt.After(now)
}
