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

type AttachmentStatus string

const (
	AttachmentJoinPending             AttachmentStatus = "join_pending"
	AttachmentQueued                  AttachmentStatus = "queued"
	AttachmentStartReceived           AttachmentStatus = "start_received"
	AttachmentReauthorizationRequired AttachmentStatus = "reauthorization_required"
	AttachmentCanceled                AttachmentStatus = "canceled"
)

type AttachmentDeliveryState string

const (
	AttachmentDeliveryPending        AttachmentDeliveryState = "pending"
	AttachmentDeliveryReceived       AttachmentDeliveryState = "received"
	AttachmentDeliveryCompleted      AttachmentDeliveryState = "completed"
	AttachmentDeliveryOutcomeUnknown AttachmentDeliveryState = "outcome_unknown"
)

type AttachmentBlockerKind string

const (
	AttachmentBlockerQueued                  AttachmentBlockerKind = "queued"
	AttachmentBlockerReauthorizationRequired AttachmentBlockerKind = "reauthorization_required"
	AttachmentBlockerNewRunRequired          AttachmentBlockerKind = "new_run_required"
	AttachmentBlockerOutcomeUnknown          AttachmentBlockerKind = "outcome_unknown"
)

type AttachmentIdentity struct {
	AttachID                   string
	BootstrapSessionID         string
	TargetSessionID            string
	TargetCredentialLineageRef string
}

type Attachment struct {
	Identity          AttachmentIdentity
	Status            AttachmentStatus
	DeliveryState     AttachmentDeliveryState
	DeliveryVersion   int64
	QueueReason       *string
	ExpiresAt         *time.Time
	CanceledAt        *time.Time
	BlockingSessionID *string
}

type AttachmentBlocker struct {
	Kind              AttachmentBlockerKind
	Reason            *string
	ExpiresAt         *time.Time
	BlockingSessionID *string
	Operation         *string
}

type AttachmentSummary struct {
	AttachID        string
	TargetSessionID string
	DeliveryVersion int64
	ExpiresAt       *time.Time
	Blocker         *AttachmentBlocker
}

type AttachmentCreate struct {
	Identity  AttachmentIdentity
	ExpiresAt time.Time
}

type AttachmentUpdate struct {
	Status            AttachmentStatus
	DeliveryState     AttachmentDeliveryState
	QueueReason       *string
	ExpiresAt         *time.Time
	BlockingSessionID *string
	Blocker           *AttachmentBlocker
}

type AttachmentCommit struct {
	Attachment Attachment
	Summary    AttachmentSummary
	Noop       bool
}

type AttachmentMutation struct {
	Attachment Attachment
	Summary    AttachmentSummary
}
type AttachmentStore interface {
	EventStore
	CreateAttachment(ctx context.Context, request AttachmentCreate) (AttachmentCommit, error)
	Attachment(ctx context.Context, attachID string) (Attachment, error)
	AttachmentForTarget(ctx context.Context, targetSessionID string) (Attachment, error)
	UpdateAttachment(ctx context.Context, attachID string, expectedVersion int64, update AttachmentUpdate) (AttachmentMutation, error)
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

// CommandAuthority identifies the exact Adapter connection that may mutate a
// delivery record. The Store validates current generation, revocation, expiry,
// and terminal state from its own durable truth in the same transaction.
type CommandAuthority struct {
	ConnectionEpoch      int64
	CredentialGeneration int64
}

type ProposedEventStatus string

const ProposedEventAccepted ProposedEventStatus = "accepted"

type ProposedEventRequest struct {
	ProposalID string
	Event      PendingEvent
}

// ProposedEventReceipt is reference-only acknowledgement truth. The durable
// EventStore event, not the receipt, owns the proposed event payload.
type ProposedEventReceipt struct {
	SessionID  string
	ProposalID string
	Seq        int64
	Status     ProposedEventStatus
}

// ProposedEventStore is the optional Adapter proposal extension. The Store
// atomically verifies current authority, persists one durable event, and maps a
// proposal ID to its authoritative sequence without duplicating event content.
type ProposedEventStore interface {
	EventStore
	CommitProposedEvent(ctx context.Context, sessionID string, authority CommandAuthority, proposal ProposedEventRequest) (ProposedEventReceipt, error)
}

// CommandLedgerStore is the optional durable delivery extension. It appends
// the user event and its reference-only command ledger entry atomically.
type CommandLedgerStore interface {
	EventStore
	CommitPendingCommand(ctx context.Context, sessionID string, authority CommandAuthority, event PendingEvent, request PendingCommandRequest) (PendingCommandCommit, error)
	ClaimPendingCommand(ctx context.Context, sessionID string, authority CommandAuthority, commandID string) (PendingCommandClaim, error)
	ResolvePendingCommand(ctx context.Context, sessionID string, authority CommandAuthority, commandID string, status PendingCommandStatus) (PendingCommand, error)
}
