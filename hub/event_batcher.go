package hub

import (
	"context"
	"fmt"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

type adapterEventBatcherConfig struct {
	Store       store.EventStore
	SessionID   string
	Window      time.Duration
	MaxEvents   int
	Broadcast   func(context.Context, protocol.Event)
	ReportError func(context.Context, error)
}

type adapterEventBatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	store       store.EventStore
	sessionID   string
	window      time.Duration
	maxEvents   int
	queue       chan pendingAdapterEvent
	broadcast   func(context.Context, protocol.Event)
	reportError func(context.Context, error)
}

type pendingAdapterEvent struct {
	event   protocol.Event
	pending store.PendingEvent
}

func newAdapterEventBatcher(cfg adapterEventBatcherConfig) *adapterEventBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	maxEvents := cfg.MaxEvents
	if maxEvents <= 0 {
		maxEvents = adapterEventBatchMaxEvents
	}
	window := cfg.Window
	if window <= 0 {
		window = adapterEventBatchWindow
	}
	b := &adapterEventBatcher{
		ctx:         ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
		store:       cfg.Store,
		sessionID:   cfg.SessionID,
		window:      window,
		maxEvents:   maxEvents,
		queue:       make(chan pendingAdapterEvent, maxEvents),
		broadcast:   cfg.Broadcast,
		reportError: cfg.ReportError,
	}
	go b.run()
	return b
}

func (b *adapterEventBatcher) Enqueue(ctx context.Context, ev protocol.Event, pending store.PendingEvent) error {
	item := pendingAdapterEvent{
		event: protocol.Event{
			Type:      ev.Type,
			SessionID: ev.SessionID,
			Time:      ev.Time,
			Payload:   clonePayload(ev.Payload),
		},
		pending: store.PendingEvent{
			Type:    pending.Type,
			Time:    pending.Time,
			Payload: clonePayload(pending.Payload),
		},
	}

	select {
	case b.queue <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

func (b *adapterEventBatcher) Close() {
	b.cancel()
	<-b.done
}

func (b *adapterEventBatcher) run() {
	defer close(b.done)

	var (
		batch  []pendingAdapterEvent
		timer  *time.Timer
		timerC <-chan time.Time
	)
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	flush := func(ctx context.Context) {
		if len(batch) == 0 {
			return
		}
		b.flush(ctx, batch)
		batch = nil
		stopTimer()
	}
	drainQueue := func() {
		for {
			select {
			case item := <-b.queue:
				batch = append(batch, item)
			default:
				return
			}
		}
	}

	for {
		select {
		case item := <-b.queue:
			batch = append(batch, item)
			if len(batch) == 1 {
				timer = time.NewTimer(b.window)
				timerC = timer.C
			}
			if len(batch) >= b.maxEvents {
				flush(b.ctx)
			}
		case <-timerC:
			flush(b.ctx)
		case <-b.ctx.Done():
			drainQueue()
			flush(context.Background())
			return
		}
	}
}

func (b *adapterEventBatcher) flush(ctx context.Context, batch []pendingAdapterEvent) {
	pending := make([]store.PendingEvent, len(batch))
	for i, item := range batch {
		pending[i] = item.pending
	}

	firstSeq, err := b.store.Append(ctx, b.sessionID, pending)
	if err != nil {
		if b.reportError != nil {
			b.reportError(ctx, fmt.Errorf("persist event: %w", err))
		}
		return
	}
	for i, item := range batch {
		seq := firstSeq + int64(i)
		out := item.event
		out.Seq = &seq
		if b.broadcast != nil {
			b.broadcast(ctx, out)
		}
	}
}
