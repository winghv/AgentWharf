package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestAdapterEventBatcherFlushesAtMaxEvents(t *testing.T) {
	st := newBatcherTestStore()
	broadcasts := make(chan protocol.Event, 2)
	batcher := newAdapterEventBatcher(adapterEventBatcherConfig{
		Store:     st,
		SessionID: "ses_1",
		Window:    time.Hour,
		MaxEvents: 2,
		Broadcast: func(_ context.Context, ev protocol.Event) {
			broadcasts <- ev
		},
	})
	defer batcher.Close()

	enqueueBatcherEvent(t, batcher, 1)
	enqueueBatcherEvent(t, batcher, 2)

	first := readBatcherBroadcast(t, broadcasts)
	second := readBatcherBroadcast(t, broadcasts)
	if first.Seq == nil || *first.Seq != 1 || string(first.Payload) != `{"n":1}` {
		t.Fatalf("first broadcast = %+v payload=%s", first, string(first.Payload))
	}
	if second.Seq == nil || *second.Seq != 2 || string(second.Payload) != `{"n":2}` {
		t.Fatalf("second broadcast = %+v payload=%s", second, string(second.Payload))
	}

	calls := st.calls()
	if len(calls) != 1 || len(calls[0]) != 2 {
		t.Fatalf("append calls = %+v, want one batch of two events", calls)
	}
}

func TestAdapterEventBatcherDrainsQueueOnClose(t *testing.T) {
	st := newBatcherTestStore()
	batcher := newAdapterEventBatcher(adapterEventBatcherConfig{
		Store:     st,
		SessionID: "ses_1",
		Window:    time.Hour,
		MaxEvents: 64,
	})

	enqueueBatcherEvent(t, batcher, 1)
	batcher.Close()

	calls := st.calls()
	if len(calls) != 1 || len(calls[0]) != 1 {
		t.Fatalf("append calls = %+v, want queued event flushed on close", calls)
	}
}

type batcherTestStore struct {
	mu     sync.Mutex
	latest int64
	events [][]store.PendingEvent
}

func newBatcherTestStore() *batcherTestStore {
	return &batcherTestStore{}
}

func (s *batcherTestStore) Append(_ context.Context, _ string, evs []store.PendingEvent) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	firstSeq := s.latest + 1
	copied := make([]store.PendingEvent, len(evs))
	copy(copied, evs)
	s.events = append(s.events, copied)
	s.latest += int64(len(evs))
	return firstSeq, nil
}

func (s *batcherTestStore) Replay(context.Context, string, int64, func(store.Event) error) error {
	return nil
}

func (s *batcherTestStore) LatestSeq(context.Context, string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest, nil
}

func (s *batcherTestStore) calls() [][]store.PendingEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([][]store.PendingEvent, len(s.events))
	for i, events := range s.events {
		copied[i] = append([]store.PendingEvent(nil), events...)
	}
	return copied
}

func enqueueBatcherEvent(t *testing.T, batcher *adapterEventBatcher, n int) {
	t.Helper()

	payload := json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))
	if err := batcher.Enqueue(context.Background(), protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      int64(2000 + n),
		Payload:   payload,
	}, store.PendingEvent{
		Type:    "session.message",
		Time:    time.UnixMilli(int64(2000 + n)),
		Payload: payload,
	}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
}

func readBatcherBroadcast(t *testing.T, broadcasts <-chan protocol.Event) protocol.Event {
	t.Helper()

	select {
	case ev := <-broadcasts:
		return ev
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batcher broadcast")
		return protocol.Event{}
	}
}
