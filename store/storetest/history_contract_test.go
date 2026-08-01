package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/winghv/agentwharf/store"
)

func TestHistoryContract(t *testing.T) {
	st := newMemoryHistoryStore()
	HistoryContract(t, HistoryHarness{
		Open: func(t *testing.T) store.HistoryStore {
			t.Helper()
			return st
		},
		Reopen: func(t *testing.T, current store.HistoryStore) store.HistoryStore {
			t.Helper()
			if current != st {
				t.Fatal("reopen received an unexpected store")
			}
			return st
		},
		PruneBefore: func(t *testing.T, current store.HistoryStore, sessionID string, beforeSeq int64) {
			t.Helper()
			if current != st {
				t.Fatal("prune received an unexpected store")
			}
			st.pruneBefore(sessionID, beforeSeq)
		},
	})
}

type memoryHistoryStore struct {
	mu     sync.Mutex
	events map[string][]store.Event
}

func newMemoryHistoryStore() *memoryHistoryStore {
	return &memoryHistoryStore{events: make(map[string][]store.Event)}
}

func (s *memoryHistoryStore) Append(_ context.Context, sessionID string, events []store.PendingEvent) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	firstSeq := int64(len(s.events[sessionID]) + 1)
	for index, event := range events {
		s.events[sessionID] = append(s.events[sessionID], store.Event{
			SessionID: sessionID,
			Seq:       firstSeq + int64(index),
			Type:      event.Type,
			Time:      event.Time,
			Payload:   append(json.RawMessage(nil), event.Payload...),
		})
	}
	return firstSeq, nil
}

func (s *memoryHistoryStore) Replay(_ context.Context, sessionID string, afterSeq int64, visit func(store.Event) error) error {
	if visit == nil {
		return errors.New("replay callback is nil")
	}

	s.mu.Lock()
	events := append([]store.Event(nil), s.events[sessionID]...)
	s.mu.Unlock()
	for _, event := range events {
		if event.Seq <= afterSeq {
			continue
		}
		if err := visit(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *memoryHistoryStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if events := s.events[sessionID]; len(events) > 0 {
		return events[len(events)-1].Seq, nil
	}
	return 0, nil
}

func (s *memoryHistoryStore) History(_ context.Context, sessionID string, beforeSeq *int64, limit int) (store.HistoryPage, error) {
	if limit < 1 || limit > 100 {
		return store.HistoryPage{}, errors.New("history limit is out of range")
	}

	s.mu.Lock()
	events := append([]store.Event(nil), s.events[sessionID]...)
	s.mu.Unlock()
	page := store.HistoryPage{RetentionState: store.RetentionComplete}
	if len(events) == 0 {
		return page, nil
	}
	page.LatestSeq = events[len(events)-1].Seq
	upper := page.LatestSeq + 1
	if beforeSeq != nil && *beforeSeq < upper {
		upper = *beforeSeq
	}
	for index := len(events) - 1; index >= 0 && len(page.Events) < limit; index-- {
		event := events[index]
		if event.Seq < upper {
			page.Events = append(page.Events, event)
		}
	}
	for left, right := 0, len(page.Events)-1; left < right; left, right = left+1, right-1 {
		page.Events[left], page.Events[right] = page.Events[right], page.Events[left]
	}
	if len(page.Events) > 0 && page.Events[0].Seq > events[0].Seq {
		next := page.Events[0].Seq
		page.NextBeforeSeq = &next
	}
	if events[0].Seq > 1 {
		page.RetentionState = store.RetentionGap
	}
	return page, nil
}

func (s *memoryHistoryStore) pruneBefore(sessionID string, beforeSeq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.events[sessionID]
	first := sort.Search(len(events), func(index int) bool {
		return events[index].Seq >= beforeSeq
	})
	s.events[sessionID] = append([]store.Event(nil), events[first:]...)
}
