package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

type Harness func(t *testing.T) store.EventStore

type HistoryHarness struct {
	Open        func(t *testing.T) store.HistoryStore
	Reopen      func(t *testing.T, current store.HistoryStore) store.HistoryStore
	PruneBefore func(t *testing.T, current store.HistoryStore, sessionID string, beforeSeq int64)
}

func Contract(t *testing.T, newStore Harness) {
	t.Helper()

	t.Run("append replay and latest", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_basic"
		first, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.tool_call", 2),
		})
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if first != 1 {
			t.Fatalf("first seq = %d, want 1", first)
		}

		latest, err := st.LatestSeq(ctx, sessionID)
		if err != nil {
			t.Fatalf("LatestSeq() error = %v", err)
		}
		if latest != 2 {
			t.Fatalf("latest = %d, want 2", latest)
		}

		got := replayAll(t, st, sessionID, 0)
		assertSeqs(t, got, []int64{1, 2})
		if got[0].SessionID != sessionID || got[0].Type != "session.message" {
			t.Fatalf("first replayed event = %+v", got[0])
		}
		if wantTime := time.UnixMilli(1764937200001); !got[0].Time.Equal(wantTime) {
			t.Fatalf("first replayed event time = %s, want %s", got[0].Time, wantTime)
		}
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(got[1].Payload, &payload); err != nil {
			t.Fatalf("payload is invalid JSON: %v", err)
		}
		if payload.N != 2 {
			t.Fatalf("payload.n = %d, want 2", payload.N)
		}
	})

	t.Run("replay after seq", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_replay"
		if _, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.message", 2),
			pending("session.message", 3),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		got := replayAll(t, st, sessionID, 1)
		assertSeqs(t, got, []int64{2, 3})
	})

	t.Run("append empty batch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		first, err := st.Append(ctx, "ses_contract_empty", nil)
		if err != nil {
			t.Fatalf("Append(empty) error = %v", err)
		}
		if first != 0 {
			t.Fatalf("first seq = %d, want 0", first)
		}
	})

	t.Run("sessions are independent", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		if first, err := st.Append(ctx, "ses_contract_a", []store.PendingEvent{pending("session.message", 1)}); err != nil || first != 1 {
			t.Fatalf("Append(session a) = %d, %v; want 1, nil", first, err)
		}
		if first, err := st.Append(ctx, "ses_contract_b", []store.PendingEvent{pending("session.message", 1)}); err != nil || first != 1 {
			t.Fatalf("Append(session b) = %d, %v; want 1, nil", first, err)
		}
	})

	t.Run("concurrent append has no gaps or duplicates", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_concurrent"
		const writers = 16
		const batchSize = 8

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, writers)
		for writer := range writers {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				<-start
				events := make([]store.PendingEvent, 0, batchSize)
				for i := range batchSize {
					events = append(events, pending("session.message", writer*batchSize+i))
				}
				_, err := st.Append(ctx, sessionID, events)
				if err != nil {
					errs <- err
				}
			}(writer)
		}

		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("Append() concurrent error = %v", err)
		}

		got := replayAll(t, st, sessionID, 0)
		wantTotal := writers * batchSize
		if len(got) != wantTotal {
			t.Fatalf("replayed %d events, want %d", len(got), wantTotal)
		}
		for i, ev := range got {
			wantSeq := int64(i + 1)
			if ev.Seq != wantSeq {
				t.Fatalf("event[%d].Seq = %d, want %d", i, ev.Seq, wantSeq)
			}
		}
	})

	t.Run("replay callback error stops scan", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_callback_error"
		if _, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.message", 2),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		wantErr := errors.New("stop replay")
		var calls int
		err := st.Replay(ctx, sessionID, 0, func(store.Event) error {
			calls++
			return wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("Replay() error = %v, want %v", err, wantErr)
		}
		if calls != 1 {
			t.Fatalf("callback calls = %d, want 1", calls)
		}
	})
}

func HistoryContract(t *testing.T, harness HistoryHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.PruneBefore == nil {
		t.Fatal("history contract harness must provide open, reopen, and prune callbacks")
	}

	t.Run("empty page and limit bounds", func(t *testing.T) {
		st := harness.Open(t)
		page := historyPage(t, st, "ses_history_empty", nil, 1)
		assertHistoryPage(t, page, nil, 0, nil, store.RetentionComplete)
		for _, limit := range []int{0, 101} {
			if _, err := st.History(context.Background(), "ses_history_empty", nil, limit); err == nil {
				t.Fatalf("History(limit=%d) unexpectedly succeeded", limit)
			}
		}
	})

	t.Run("exclusive cursor preserves ascending bounded order", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_cursor"
		appendHistoryEvents(t, st, sessionID, 5)

		page := historyPage(t, st, sessionID, nil, 2)
		assertHistoryPage(t, page, []int64{4, 5}, 5, int64Pointer(4), store.RetentionComplete)
		page = historyPage(t, st, sessionID, page.NextBeforeSeq, 2)
		assertHistoryPage(t, page, []int64{2, 3}, 5, int64Pointer(2), store.RetentionComplete)
		page = historyPage(t, st, sessionID, page.NextBeforeSeq, 2)
		assertHistoryPage(t, page, []int64{1}, 5, nil, store.RetentionComplete)
	})

	t.Run("retention gap remains explicit after prune", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_gap"
		appendHistoryEvents(t, st, sessionID, 5)
		harness.PruneBefore(t, st, sessionID, 4)
		page := historyPage(t, st, sessionID, nil, 100)
		assertHistoryPage(t, page, []int64{4, 5}, 5, nil, store.RetentionGap)
	})

	t.Run("concurrent append remains sequenced and pageable", func(t *testing.T) {
		st := harness.Open(t)
		const (
			sessionID = "ses_history_concurrent"
			writers   = 8
			batchSize = 4
		)
		start := make(chan struct{})
		errs := make(chan error, writers)
		var wg sync.WaitGroup
		for writer := range writers {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				<-start
				events := make([]store.PendingEvent, 0, batchSize)
				for index := range batchSize {
					events = append(events, pending("session.message", writer*batchSize+index))
				}
				_, err := st.Append(context.Background(), sessionID, events)
				errs <- err
			}(writer)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent Append() error = %v", err)
			}
		}

		page := historyPage(t, st, sessionID, nil, writers*batchSize)
		want := make([]int64, writers*batchSize)
		for index := range want {
			want[index] = int64(index + 1)
		}
		assertHistoryPage(t, page, want, int64(writers*batchSize), nil, store.RetentionComplete)
	})

	t.Run("reopen retains the same historical truth", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_reopen"
		appendHistoryEvents(t, st, sessionID, 3)
		st = harness.Reopen(t, st)
		page := historyPage(t, st, sessionID, nil, 100)
		assertHistoryPage(t, page, []int64{1, 2, 3}, 3, nil, store.RetentionComplete)
	})
}

func appendHistoryEvents(t *testing.T, st store.EventStore, sessionID string, count int) {
	t.Helper()
	events := make([]store.PendingEvent, 0, count)
	for index := range count {
		events = append(events, pending("session.message", index))
	}
	if _, err := st.Append(context.Background(), sessionID, events); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func historyPage(t *testing.T, st store.HistoryStore, sessionID string, beforeSeq *int64, limit int) store.HistoryPage {
	t.Helper()
	page, err := st.History(context.Background(), sessionID, beforeSeq, limit)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	return page
}

func assertHistoryPage(t *testing.T, page store.HistoryPage, wantSeqs []int64, latestSeq int64, nextBeforeSeq *int64, retentionState string) {
	t.Helper()
	if page.LatestSeq != latestSeq {
		t.Fatalf("page latest seq = %d, want %d", page.LatestSeq, latestSeq)
	}
	if page.RetentionState != retentionState {
		t.Fatalf("page retention state = %q, want %q", page.RetentionState, retentionState)
	}
	if (page.NextBeforeSeq == nil) != (nextBeforeSeq == nil) || (page.NextBeforeSeq != nil && *page.NextBeforeSeq != *nextBeforeSeq) {
		t.Fatalf("page next before seq = %v, want %v", page.NextBeforeSeq, nextBeforeSeq)
	}
	if len(page.Events) != len(wantSeqs) {
		t.Fatalf("page event count = %d, want %d", len(page.Events), len(wantSeqs))
	}
	for index, event := range page.Events {
		if event.Seq != wantSeqs[index] {
			t.Fatalf("page event[%d] seq = %d, want %d", index, event.Seq, wantSeqs[index])
		}
		if index > 0 && page.Events[index-1].Seq >= event.Seq {
			t.Fatalf("page events are not strictly ascending: %d then %d", page.Events[index-1].Seq, event.Seq)
		}
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func pending(eventType string, n int) store.PendingEvent {
	payload := json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))
	return store.PendingEvent{
		Type:    eventType,
		Time:    time.UnixMilli(1764937200000 + int64(n)),
		Payload: payload,
	}
}

func replayAll(t *testing.T, st store.EventStore, sessionID string, afterSeq int64) []store.Event {
	t.Helper()

	var got []store.Event
	if err := st.Replay(context.Background(), sessionID, afterSeq, func(ev store.Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	return got
}

func assertSeqs(t *testing.T, got []store.Event, want []int64) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}
	for i, ev := range got {
		if ev.Seq != want[i] {
			t.Fatalf("event[%d].Seq = %d, want %d", i, ev.Seq, want[i])
		}
	}
}
