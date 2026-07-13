package store

import (
	"context"
	"encoding/json"
	"time"
)

type PendingEvent struct {
	Type    string
	Time    time.Time
	Payload json.RawMessage
}

type Event struct {
	SessionID string
	Seq       int64
	Type      string
	Time      time.Time
	Payload   json.RawMessage
}

type EventStore interface {
	Append(ctx context.Context, sessionID string, evs []PendingEvent) (firstSeq int64, err error)
	Replay(ctx context.Context, sessionID string, afterSeq int64, fn func(Event) error) error
	LatestSeq(ctx context.Context, sessionID string) (int64, error)
}

// HistoryStore is the optional v2 extension for bounded historical backfill.
// EventStore remains the v1-compatible minimum used by replay-only callers.
type HistoryStore interface {
	EventStore
	History(ctx context.Context, sessionID string, beforeSeq *int64, limit int) (HistoryPage, error)
}

const (
	RetentionComplete = "complete"
	RetentionGap      = "retention_gap"
)

type HistoryPage struct {
	Events         []Event
	LatestSeq      int64
	NextBeforeSeq  *int64
	RetentionState string
}
