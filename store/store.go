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

type PendingCommandStatus string

const (
	PendingCommandPending        PendingCommandStatus = "pending"
	PendingCommandReceived       PendingCommandStatus = "received"
	PendingCommandCompleted      PendingCommandStatus = "completed"
	PendingCommandOutcomeUnknown PendingCommandStatus = "outcome_unknown"
)

type PendingCommandRequest struct {
	CommandID string
	Type      string
	ExpiresAt time.Time
}

// PendingCommand is a reference-only delivery ledger record. The referenced
// durable event, rather than this record, owns any command body.
type PendingCommand struct {
	SessionID string
	CommandID string
	Type      string
	EventSeq  int64
	Status    PendingCommandStatus
	ExpiresAt time.Time
}

type PendingCommandCommit struct {
	Command   PendingCommand
	Duplicate bool
}

type PendingCommandClaim struct {
	Command PendingCommand
	Claimed bool
}

// CommandLedgerStore is the optional durable delivery extension. It appends
// the user event and its reference-only command ledger entry atomically.
type CommandLedgerStore interface {
	EventStore
	CommitPendingCommand(ctx context.Context, sessionID string, event PendingEvent, request PendingCommandRequest) (PendingCommandCommit, error)
	ClaimPendingCommand(ctx context.Context, sessionID string, commandID string) (PendingCommandClaim, error)
	ResolvePendingCommand(ctx context.Context, sessionID string, commandID string, status PendingCommandStatus) (PendingCommand, error)
}
