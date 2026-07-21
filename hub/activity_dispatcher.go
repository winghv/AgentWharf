package hub

import (
	"context"
	"errors"
	"time"

	"github.com/winghv/agentwharf/store"
)

const defaultActivityDispatchInterval = time.Minute

// ActivitySink receives provider-neutral, Store-committed activity summaries.
// It deliberately contains only durable summary facts, so dispatch cannot
// become another source of activity truth.
type ActivitySummary struct {
	SessionID           string
	State               string
	LastDurableSeq      int64
	LedgerVersion       int64
	LastDurableEventAt  *time.Time
	LastClientCommandAt *time.Time
	StoreSnapshotAt     time.Time
	ProjectionState     string
	BlockerKind         string
	BlockerExpiresAt    *time.Time
}

type ActivitySink interface {
	PublishActivitySummary(context.Context, ActivitySummary) error
}

type ActivitySinkFunc func(context.Context, ActivitySummary) error

func (fn ActivitySinkFunc) PublishActivitySummary(ctx context.Context, summary ActivitySummary) error {
	return fn(ctx, summary)
}

type ActivityDispatcherConfig struct {
	Interval time.Duration
}

// ActivityDispatcher performs bounded keyset rescans. It has one ticker for
// the whole Store and creates no Session-specific timers or goroutines.
type ActivityDispatcher struct {
	pages    store.AttentionSummaryPageStore
	sink     ActivitySink
	interval time.Duration
}

func NewActivityDispatcher(pages store.AttentionSummaryPageStore, sink ActivitySink, cfg ActivityDispatcherConfig) *ActivityDispatcher {
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultActivityDispatchInterval
	}
	return &ActivityDispatcher{pages: pages, sink: sink, interval: interval}
}

func (d *ActivityDispatcher) Run(ctx context.Context) error {
	if err := d.DispatchOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.DispatchOnce(ctx); err != nil {
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
		if page.SnapshotAt.IsZero() {
			return errors.New("activity summary page snapshot is missing")
		}
		for _, summary := range page.Summaries {
			if summary.SessionID <= after {
				return errors.New("activity summary page is not strictly ordered")
			}
			after = summary.SessionID
			activity := ActivitySummary{SessionID: summary.SessionID, State: summary.State, LastDurableSeq: summary.LatestSeq,
				LedgerVersion: summary.SummaryVersion, LastDurableEventAt: summary.LastDurableEventAt, LastClientCommandAt: summary.LastClientCommandAt,
				StoreSnapshotAt: page.SnapshotAt.UTC(), ProjectionState: summary.StateOfProjection}
			if summary.Blocker != nil {
				activity.BlockerKind, activity.BlockerExpiresAt = summary.Blocker.Kind, summary.Blocker.ExpiresAt
			}
			if err := d.sink.PublishActivitySummary(ctx, activity); err != nil {
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
