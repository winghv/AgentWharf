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

// SessionAdmissionTruth is the Store-owned summary used to distinguish a
// fresh target from existing complete Session truth without platform lookup.
type SessionAdmissionTruth struct {
	SessionID   string
	Provider    string
	Exists      bool
	Complete    bool
	Terminal    bool
	Conflicting bool
	Live        bool
}

type SessionAdmissionTruthStore interface {
	EventStore
	SessionAdmissionTruth(ctx context.Context, sessionID string) (SessionAdmissionTruth, error)
}

// HistoryStore is the optional v2 extension for bounded historical backfill.
// EventStore remains the v1-compatible minimum used by replay-only callers.
type HistoryStore interface {
	EventStore
	History(ctx context.Context, sessionID string, beforeSeq *int64, limit int) (HistoryPage, error)
}

const (
	AttentionBlockerQueued                  = "queued"
	AttentionBlockerReauthorizationRequired = "reauthorization_required"
	AttentionBlockerNewRunRequired          = "new_run_required"
	AttentionBlockerOutcomeUnknown          = "outcome_unknown"
	AttentionPermissionPending              = "pending"
	AttentionProjectionComplete             = "complete"
	AttentionProjectionIncomplete           = "incomplete"
)

// AttentionBlocker is bounded, provider-neutral ledger evidence. It carries
// neither a provider object nor any command, content, credential, Task, or Run.
type AttentionBlocker struct {
	Kind              string
	Reason            *string
	ExpiresAt         *time.Time
	BlockingSessionID *string
	Operation         *string
}

// AttentionPermission is a pending, opaque permission-request reference.
type AttentionPermission struct {
	ID     string
	Status string
}

// SessionAttentionSummary is the EventStore-owned durable projection for a
// single Session. LatestSeq and SummaryVersion are independent domains: the
// former records durable-event truth, while the latter records ledger-only
// evidence. The activity timestamps are original Store-clock values only.
type SessionAttentionSummary struct {
	SessionID           string
	LatestSeq           int64
	State               string
	Permission          *AttentionPermission
	TerminalOutcome     *string
	LatestChangeSeq     *int64
	Blocker             *AttentionBlocker
	SummaryVersion      int64
	LastDurableEventAt  *time.Time
	LastClientCommandAt *time.Time
	StateOfProjection   string
}

// AttentionSummaryStore is separate so v1 stores need not claim v2 support.
// Reads occur only after the Auth boundary has validated exact membership.
type AttentionSummaryStore interface {
	AttentionSnapshot(ctx context.Context, sessionIDs []string) ([]SessionAttentionSummary, error)
}

const MaxAttentionSummaryPageSize = 100

// AttentionSummaryPageRequest is a bounded, read-only keyset scan over the
// Store-owned projection. AfterSessionID is the exclusive last key returned by
// a prior page; it is never interpreted as a platform identifier.
type AttentionSummaryPageRequest struct {
	AfterSessionID string
	Limit          int
}

// AttentionSummaryPage contains an ordered page and an exclusive continuation
// key. A nil continuation means the scan is complete.
type AttentionSummaryPage struct {
	Summaries          []SessionAttentionSummary
	NextAfterSessionID *string
	SnapshotAt         time.Time
}

// AttentionSummaryPageStore exposes bounded restart-safe scans without
// granting callers any mutation or access to EventStore implementation state.
type AttentionSummaryPageStore interface {
	AttentionSummaryStore
	AttentionSummaryPage(context.Context, AttentionSummaryPageRequest) (AttentionSummaryPage, error)
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

type AdapterConnectionCredentialState string

const (
	AdapterConnectionCredentialActive        AdapterConnectionCredentialState = "active"
	AdapterConnectionCredentialPending       AdapterConnectionCredentialState = "pending"
	AdapterConnectionCredentialPriorRecovery AdapterConnectionCredentialState = "prior_recovery"
)

type AdapterConnection struct {
	SessionID                         string
	ConnectionEpoch                   int64
	AcceptedFence                     int64
	ActiveCredentialGeneration        int64
	CredentialGenerationHighWatermark int64
	ActiveCredentialExpiresAt         time.Time
	PendingCredentialGeneration       *int64
	PendingCredentialExpiresAt        *time.Time
	PriorRecoveryGeneration           *int64
	RotationID                        *string
	RevokedAt                         *time.Time
	TerminalAt                        *time.Time
}

type AdapterConnectionInitialize struct {
	SessionID                  string
	ActiveCredentialGeneration int64
	ActiveCredentialExpiresAt  time.Time
}

type AdapterCredentialPreHelloRefresh struct {
	ExpectedActiveCredentialGeneration int64
	ActiveCredentialExpiresAt          time.Time
}

type AdapterConnectionPreHelloTermination struct {
	ExpectedActiveCredentialGeneration int64
}

type AdapterHello struct {
	CredentialGeneration int64
	WriterLeaseID        string
}

// AdapterConnectionAdmission binds an opaque grant fence to the exact live
// Adapter connection. A caller cannot use a tuple that hello or activation has
// already fenced.
type AdapterConnectionAdmission struct {
	CredentialGeneration int64
	ConnectionEpoch      int64
	AcceptedFence        int64
	GrantFence           int64
}

// ConnectionAuthorityReceipt is the Store-proven, non-secret live connection
// tuple that a v2 Hub may relay only to the authenticated Adapter which owns
// it. It is a fencing receipt, never a bearer or Provider configuration.
type ConnectionAuthorityReceipt struct {
	SessionID            string
	ConnectionEpoch      int64
	CredentialGeneration int64
	AcceptedFence        int64
	WriterLeaseID        string
	ExpiresAt            time.Time
}

type AdapterCredentialRotation struct {
	ExpectedActiveCredentialGeneration int64
	ExpectedEpoch                      int64
	PendingGeneration                  int64
	ExpiresAt                          time.Time
	RotationID                         string
}

type AdapterCredentialActivation struct {
	ExpectedActiveCredentialGeneration int64
	ExpectedEpoch                      int64
	PendingGeneration                  int64
	RotationID                         string
}

// AdapterConnectionStore owns opaque Session connection fencing and credential
// lineage truth. Normal hello and pending activation each advance the epoch and
// accepted fence atomically against current Store-owned revocation, expiry, and
// terminal state; callers cannot allocate epochs or accepted fences. A changed
// pre-hello refresh is valid only after Store-clock expiry; exact retry may read
// the same future expiry. Termination atomically revokes and terminalizes only a
// never-connected exact generation.
type AdapterConnectionStore interface {
	EventStore
	InitializeAdapterConnection(ctx context.Context, request AdapterConnectionInitialize) (AdapterConnection, error)
	RefreshAdapterCredentialBeforeHello(ctx context.Context, sessionID string, refresh AdapterCredentialPreHelloRefresh) (AdapterConnection, error)
	TerminateAdapterConnectionBeforeHello(ctx context.Context, sessionID string, termination AdapterConnectionPreHelloTermination) (AdapterConnection, error)
	AcceptAdapterHello(ctx context.Context, sessionID string, hello AdapterHello) (AdapterConnection, error)
	ValidateAdapterAdmission(ctx context.Context, sessionID string, admission AdapterConnectionAdmission) (AdapterConnection, error)
	PrepareAdapterCredentialRotation(ctx context.Context, sessionID string, rotation AdapterCredentialRotation) (AdapterConnection, error)
	ActivateAdapterCredential(ctx context.Context, sessionID string, activation AdapterCredentialActivation) (AdapterConnection, error)
	AdapterConnection(ctx context.Context, sessionID string) (AdapterConnection, error)
}

// AdapterConnectionTransactor is optional. Implementations use their native
// transaction primitive, while callbacks depend only on Store interfaces. A
// failed callback leaves connection initialization and admission state intact.
type AdapterConnectionTransactor interface {
	AdapterConnectionStore
	WithAdapterConnectionTransaction(ctx context.Context, fn func(AdapterConnectionStore) error) error
}

// AdapterConnectionAuthorityReceiptStore proves that the exact admission and
// writer lease still join to current durable connection truth in one Store
// transaction. Implementations must reject stale, expired, revoked, terminal,
// or replaced tuples rather than returning a downgraded receipt.
type AdapterConnectionAuthorityReceiptStore interface {
	AdapterConnectionStore
	IssueAdapterConnectionAuthorityReceipt(context.Context, string, AdapterConnectionAdmission, SettingsWriter) (ConnectionAuthorityReceipt, error)
}

// ProviderStartAdmission is the Hub-derived, exact live writer tuple used to
// turn one reserved WorkspaceLease into durable start_received. It intentionally
// excludes Provider configuration, paths, content, and credentials.
type ProviderStartAdmission struct {
	SessionID string
	Admission AdapterConnectionAdmission
	Writer    SettingsWriter
}

// ProviderStartAdmissionStore linearizes the final connection and workspace
// checks with the durable start_received transition. Implementations must
// reject a missing, replaced, revoked, terminal, expired, or quarantined lease.
type ProviderStartAdmissionStore interface {
	EventStore
	RecordProviderStartAdmission(context.Context, ProviderStartAdmission) (WorkspaceLease, error)
	WithProviderStartAdmission(context.Context, ProviderStartAdmission, func(context.Context) error) (WorkspaceLease, error)
}

type AttachAttemptOutcome string

const (
	AttachAttemptAccepted AttachAttemptOutcome = "accepted"
	AttachAttemptRejected AttachAttemptOutcome = "rejected"
)

// AttachAttemptIdentity contains only opaque, pre-hashed idempotency and
// admission identities. It never carries a raw JTI, grant, credential, or
// platform identity.
type AttachAttemptIdentity struct {
	JTIHash            [32]byte
	AttachID           string
	BootstrapSessionID string
	TargetSessionID    string
	Provider           string
}

// AttachAttemptFingerprint is the Hub-computed HMAC metadata. The Store
// records its version and digest, but neither accepts nor retains key material.
type AttachAttemptFingerprint struct {
	Domain     string
	Version    int64
	Digest     [32]byte
	KeyVersion int64
}

type AttachAttemptRequest struct {
	Identity                   AttachAttemptIdentity
	Fingerprint                AttachAttemptFingerprint
	ExpiresAt                  time.Time
	Outcome                    AttachAttemptOutcome
	IssuedCredentialGeneration *int64
}

// AttachAttempt is immutable admission history. It is intentionally separate
// from current attachment, delivery, credential, and connection state.
type AttachAttempt struct {
	Identity                   AttachAttemptIdentity
	Fingerprint                AttachAttemptFingerprint
	ExpiresAt                  time.Time
	Outcome                    AttachAttemptOutcome
	IssuedCredentialGeneration *int64
}

type AttachAttemptCommit struct {
	Attempt   AttachAttempt
	Duplicate bool
}

// AttachAttemptStore records one immutable, non-secret admission result for a
// JTI hash. Exact retries return that original result; a changed request fails
// without altering current attachment or connection truth.
type AttachAttemptStore interface {
	EventStore
	CommitAttachAttempt(ctx context.Context, request AttachAttemptRequest) (AttachAttemptCommit, error)
	AttachAttempt(ctx context.Context, jtiHash [32]byte) (AttachAttempt, error)
}

// WarmAttachFirstDelivery is the reference-only first message/outbox record.
// It deliberately contains neither command content nor a credential or grant.
type WarmAttachFirstDelivery struct {
	CommandID       string
	ReferenceID     string
	ReferenceDigest [32]byte
	ExpiresAt       time.Time
}

// WarmAttachTargetActivation is the non-secret credential lineage truth that
// becomes durable with an accepted warm attach. The bearer itself never enters
// the Store, an event, or an outbox record.
type WarmAttachTargetActivation struct {
	Generation int64
	ExpiresAt  time.Time
}

// WarmAttachRequest binds the live bootstrap admission tuple to the immutable
// attempt, target attachment lineage, and first reference-only delivery.
// Implementations recheck target lifecycle and conflict truth in the same
// transaction rather than accepting either as caller-provided input.
type WarmAttachRequest struct {
	Attempt            AttachAttemptRequest
	Attachment         AttachmentCreate
	TargetActivation   WarmAttachTargetActivation
	BootstrapAdmission AdapterConnectionAdmission
	FirstDelivery      WarmAttachFirstDelivery
}

// WarmAttachOutbox is durable, reference-only work released after commit.
// EventSeq remains the Hub-issued durable-event sequence, not a ledger version.
type WarmAttachOutbox struct {
	TargetSessionID string
	CommandID       string
	EventSeq        int64
	ReferenceID     string
	ReferenceDigest [32]byte
	ExpiresAt       time.Time
}

type WarmAttachCommit struct {
	Attempt          AttachAttempt
	Attachment       Attachment
	TargetActivation WarmAttachTargetActivation
	Outbox           WarmAttachOutbox
	Summary          SessionAttentionSummary
	Duplicate        bool
}

type WarmAttachExpiry struct {
	Attachment Attachment
	Summary    SessionAttentionSummary
}

// WarmAttachStore commits all warm-attach truth in one transaction. A failed
// commit exposes neither a partial outbox nor a summary blocker, and successful
// expiry replaces the queued blocker with reauthorization-required truth.
type WarmAttachStore interface {
	EventStore
	AttentionSummaryStore
	CommitWarmAttach(ctx context.Context, request WarmAttachRequest) (WarmAttachCommit, error)
	ExpireWarmAttach(ctx context.Context, attachID string, expectedDeliveryVersion int64) (WarmAttachExpiry, error)
}

// WarmAttachTargetActivationStore rechecks the initial target lineage against
// Store-owned time immediately before credential delivery. It is deliberately
// separate from normal Adapter admission, which requires a successful hello.
type WarmAttachTargetActivationStore interface {
	ValidateWarmAttachTargetActivation(ctx context.Context, sessionID string, activation WarmAttachTargetActivation) error
}

// WorkspaceLeaseKey is derived by trusted Hub/Auth code from an immutable
// bootstrap attachment root or an authenticated opaque workspace claim.
// Client, Adapter, and Provider input never selects this key.
type WorkspaceLeaseKey [32]byte

type WorkspaceLeaseStatus string

const (
	WorkspaceLeaseReserved      WorkspaceLeaseStatus = "reserved"
	WorkspaceLeaseStartReceived WorkspaceLeaseStatus = "start_received"
	WorkspaceLeaseQuarantined   WorkspaceLeaseStatus = "quarantined"
	WorkspaceLeaseReleased      WorkspaceLeaseStatus = "released"
)

// WorkspaceLeaseOwner is the complete, non-secret authority tuple for one
// writer. A Store implementation rechecks it against durable live authority
// before every owner-controlled transition.
type WorkspaceLeaseOwner struct {
	WorkerID             string
	SessionID            string
	ConnectionEpoch      int64
	CredentialGeneration int64
	LeaseID              string
}

// WorkspaceLeaseChildScope is an Auth-verified, opaque authorization for one
// isolated child workspace. Nil means the immutable default workspace root.
// CapabilityDigest is a non-secret reference, never a bearer or raw capability.
type WorkspaceLeaseChildScope struct {
	ParentKey        WorkspaceLeaseKey
	CapabilityDigest [32]byte
	ExpiresAt        time.Time
}

type WorkspaceLeaseReserve struct {
	Key        WorkspaceLeaseKey
	ChildScope *WorkspaceLeaseChildScope
	Owner      WorkspaceLeaseOwner
	ExpiresAt  time.Time
}

// WorkspaceLease is durable pre-spawn ownership and recovery truth. It holds
// no provider object, credential, path, command, or user content.
type WorkspaceLease struct {
	Key        WorkspaceLeaseKey
	ChildScope *WorkspaceLeaseChildScope
	Owner      WorkspaceLeaseOwner
	Status     WorkspaceLeaseStatus
	Version    int64
	ExpiresAt  time.Time
}

// WorkspaceLeaseStore owns writer reservation and quarantine truth. Only a
// trusted fixed-entry cleanup path may release a lease after it has proven the
// entire prior tree quiescent; uncertainty must transition to quarantine.
type WorkspaceLeaseStore interface {
	EventStore
	ReserveWorkspaceLease(ctx context.Context, reserve WorkspaceLeaseReserve) (WorkspaceLease, error)
	WorkspaceLease(ctx context.Context, key WorkspaceLeaseKey) (WorkspaceLease, error)
	RecordWorkspaceStartReceived(ctx context.Context, key WorkspaceLeaseKey, expectedVersion int64, owner WorkspaceLeaseOwner) (WorkspaceLease, error)
	QuarantineWorkspaceLease(ctx context.Context, key WorkspaceLeaseKey, expectedVersion int64) (WorkspaceLease, error)
	ReleaseWorkspaceLeaseAfterQuiescence(ctx context.Context, key WorkspaceLeaseKey, expectedVersion int64, owner WorkspaceLeaseOwner) (WorkspaceLease, error)
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
	ListPendingCommands(ctx context.Context, sessionID string, authority CommandAuthority) ([]PendingCommand, error)
	ClaimPendingCommand(ctx context.Context, sessionID string, authority CommandAuthority, commandID string) (PendingCommandClaim, error)
	ResolvePendingCommand(ctx context.Context, sessionID string, authority CommandAuthority, commandID string, status PendingCommandStatus) (PendingCommand, error)
	ResolvePendingCommandUnknown(ctx context.Context, sessionID string, commandID string) (PendingCommand, error)
}

type SettingsWriter struct {
	ConnectionEpoch      int64
	CredentialGeneration int64
	LeaseID              string
}
type SettingsCapability struct {
	SessionID                 string
	EventSeq                  int64
	Fingerprint               string
	EffectiveModelID          string
	EffectivePermissionModeID string
	Version                   int64
	Writer                    SettingsWriter
}
type SettingsCapabilityUpdate struct {
	EventSeq                  int64
	Fingerprint               string
	EffectiveModelID          string
	EffectivePermissionModeID string
	Writer                    SettingsWriter
}
type SettingsCommandStatus string

const (
	SettingsCommandDeliveryPending SettingsCommandStatus = "delivery_pending"
	SettingsCommandPending         SettingsCommandStatus = "pending"
	SettingsCommandRecoveryPending SettingsCommandStatus = "recovery_pending"
	SettingsCommandApplied         SettingsCommandStatus = "applied"
	SettingsCommandRejected        SettingsCommandStatus = "rejected"
	SettingsCommandTimeout         SettingsCommandStatus = "timeout"
	SettingsCommandUnsupported     SettingsCommandStatus = "unsupported"
	SettingsCommandStaleCapability SettingsCommandStatus = "stale_capability"
	SettingsCommandOutcomeUnknown  SettingsCommandStatus = "outcome_unknown"
	SettingsCommandMismatched      SettingsCommandStatus = "mismatched_effective"
)

type SettingsCommandRequest struct {
	CommandID                 string
	RequestFingerprint        string
	RequestedModelID          *string
	RequestedPermissionModeID *string
	Writer                    SettingsWriter
}
type SettingsCommand struct {
	SessionID                 string
	CommandID                 string
	RequestFingerprint        string
	RequestedModelID          *string
	RequestedPermissionModeID *string
	ReservationVersion        int64
	DeliveryDeadline          time.Time
	OperationDeadline         *time.Time
	Writer                    SettingsWriter
	ReservedCapability        SettingsCapability
	Status                    SettingsCommandStatus
	TerminalEventSeq          *int64
}
type SettingsCommandReserve struct {
	Command   SettingsCommand
	Duplicate bool
}
type SettingsCommandFinalize struct {
	ReservationVersion  int64
	ExpectedStatus      SettingsCommandStatus
	Writer              *SettingsWriter
	Outcome             SettingsCommandStatus
	ReasonCode          *string
	EffectiveCapability SettingsCapability
}

func SettingsTerminalEventPayload(command SettingsCommand, capability SettingsCapability, outcome SettingsCommandStatus, reason *string) ([]byte, error) {
	return json.Marshal(struct {
		CommandID                 string                `json:"cmd_id"`
		RequestFingerprint        string                `json:"request_fingerprint"`
		EffectiveFingerprint      string                `json:"effective_fingerprint"`
		Outcome                   SettingsCommandStatus `json:"outcome"`
		EffectiveModelID          string                `json:"effective_model_id"`
		EffectivePermissionModeID string                `json:"effective_permission_mode_id"`
		ReasonCode                *string               `json:"reason_code"`
	}{command.CommandID, command.RequestFingerprint, capability.Fingerprint, outcome, capability.EffectiveModelID, capability.EffectivePermissionModeID, reason})
}

type SettingsCommandStore interface {
	EventStore
	PublishSettingsCapability(ctx context.Context, sessionID string, update SettingsCapabilityUpdate) (SettingsCapability, error)
	SettingsCommandReserve(ctx context.Context, sessionID string, request SettingsCommandRequest) (SettingsCommandReserve, error)
	AcknowledgeSettingsCommandDelivery(ctx context.Context, sessionID string, commandID string, reservationVersion int64, writer SettingsWriter) (SettingsCommand, error)
	FinalizeSettingsCommand(ctx context.Context, sessionID string, commandID string, finalize SettingsCommandFinalize) (SettingsCommand, error)
	RecoverSettingsCommand(ctx context.Context, sessionID string, commandID string, priorWriter SettingsWriter) (SettingsCommand, error)
	SettingsCommand(ctx context.Context, sessionID string, commandID string) (SettingsCommand, error)
	PendingSettingsCommands(ctx context.Context, sessionID string) ([]SettingsCommand, error)
}

// RunControlOperation is deliberately limited to the two v2 completion
// controls. It is Store metadata, never a Provider value.
type RunControlOperation string

const (
	RunControlInterrupt RunControlOperation = "interrupt"
	RunControlStop      RunControlOperation = "stop"
)

type RunControlOutcome string

const (
	RunControlPending        RunControlOutcome = "pending"
	RunControlCompleted      RunControlOutcome = "completed"
	RunControlRejected       RunControlOutcome = "rejected"
	RunControlTimeout        RunControlOutcome = "timeout"
	RunControlUnsupported    RunControlOutcome = "unsupported"
	RunControlOutcomeUnknown RunControlOutcome = "outcome_unknown"
)

// RunControlWriter is the trusted, connection-lifecycle-bound tuple. The
// opaque lease is shared with the existing adapter writer fence, not exposed
// in a protocol payload.
type RunControlWriter = SettingsWriter

type RunControlCapability struct {
	SessionID          string
	EventSeq           int64
	Version            int64
	InterruptSupported bool
	StopSupported      bool
	Writer             RunControlWriter
}

type RunControlCapabilityUpdate struct {
	EventSeq           int64
	InterruptSupported bool
	StopSupported      bool
	Writer             RunControlWriter
}

type RunControlRequest struct {
	CommandID          string
	Operation          RunControlOperation
	PreControlState    string
	PreControlStateSeq int64
	Writer             RunControlWriter
}

type RunControlReservation struct {
	SessionID          string
	CommandID          string
	Operation          RunControlOperation
	CapabilityVersion  int64
	ReservationVersion int64
	PreControlState    string
	PreControlStateSeq int64
	Writer             RunControlWriter
	Deadline           time.Time
	Outcome            RunControlOutcome
	TerminalEventSeq   *int64
}

type RunControlReserve struct {
	Reservation RunControlReservation
	Duplicate   bool
}

type RunControlFinalize struct {
	ReservationVersion int64
	Writer             *RunControlWriter
	Outcome            RunControlOutcome
	ReasonCode         *string
}

// RunControlStore owns the durable capability/reservation/outcome ledger.
// The Hub remains responsible for authorization and routing; it cannot create
// an outcome by appending a public event directly.
type RunControlStore interface {
	EventStore
	PublishRunControlCapability(ctx context.Context, sessionID string, update RunControlCapabilityUpdate) (RunControlCapability, error)
	RunControlReserve(ctx context.Context, sessionID string, request RunControlRequest) (RunControlReserve, error)
	RunControlFinalize(ctx context.Context, sessionID, commandID string, finalize RunControlFinalize) (RunControlReservation, error)
	RecoverRunControl(ctx context.Context, sessionID, commandID string, reason string) (RunControlReservation, error)
	RunControl(ctx context.Context, sessionID, commandID string) (RunControlReservation, error)
	PendingRunControls(ctx context.Context, sessionID string) ([]RunControlReservation, error)
}

// FileReferenceWriter is the trusted Adapter connection tuple. It is never a
// client-visible field and must be revalidated against the live connection.
type FileReferenceWriter = SettingsWriter

type FileReferenceCapability struct {
	SessionID   string
	EventSeq    int64
	Fingerprint string
	Writer      FileReferenceWriter
}

type FileReferenceCapabilityUpdate struct {
	EventSeq    int64
	Fingerprint string
	Writer      FileReferenceWriter
}

type FileReferenceCommandStatus string

const (
	FileReferenceDeliveryPending FileReferenceCommandStatus = "delivery_pending"
	FileReferencePending         FileReferenceCommandStatus = "pending"
	FileReferenceDelivered       FileReferenceCommandStatus = "delivered"
	FileReferenceRejected        FileReferenceCommandStatus = "rejected"
	FileReferenceOutcomeUnknown  FileReferenceCommandStatus = "outcome_unknown"
)

// FileReferenceCommand intentionally contains no path, filename, digest, byte
// count, media type or file content. Those fields live only in the durable
// user message and the Adapter's trusted workspace boundary.
type FileReferenceCommand struct {
	SessionID             string
	CommandID             string
	MessageID             string
	CapabilityFingerprint string
	RequestFingerprint    string
	ReferenceCount        int
	ReservationVersion    int64
	DeliveryDeadline      time.Time
	Writer                *FileReferenceWriter
	Status                FileReferenceCommandStatus
	TerminalEventSeq      *int64
}

type FileReferenceCommandRequest struct {
	CommandID             string
	MessageID             string
	CapabilityFingerprint string
	RequestFingerprint    string
	ReferenceCount        int
}

type FileReferenceCommandReserve struct {
	Command   FileReferenceCommand
	Duplicate bool
}

type FileReferenceCommandFinalize struct {
	ReservationVersion int64
	Writer             *FileReferenceWriter
	Outcome            FileReferenceCommandStatus
	ReasonCode         *string
	ReferenceIndex     *int
}

func ValidFileReferenceTerminal(outcome FileReferenceCommandStatus, referenceCount int, reason *string, index *int) bool {
	if referenceCount < 1 || referenceCount > 8 {
		return false
	}
	if outcome == FileReferenceDelivered {
		return reason == nil && index == nil
	}
	if outcome == FileReferenceRejected {
		if reason == nil || index == nil || *index < 0 || *index >= referenceCount {
			return false
		}
		switch *reason {
		case "missing", "removed", "stale_reference", "size_changed", "media_type_changed", "image_unsupported", "provider_unsupported", "unsafe_path", "quarantined", "access_denied":
			return true
		}
		return false
	}
	if outcome == FileReferenceOutcomeUnknown {
		if reason == nil || index != nil {
			return false
		}
		switch *reason {
		case "delivery_unconfirmed", "writer_lost", "adapter_deadline":
			return true
		}
	}
	return false
}

// FileReferenceCommandStore atomically appends an accepted user message and
// its opaque command ledger record. Hub routing and protocol decoding remain
// outside this Store boundary.
type FileReferenceCommandStore interface {
	EventStore
	PublishFileReferenceCapability(ctx context.Context, sessionID string, update FileReferenceCapabilityUpdate) (FileReferenceCapability, error)
	CommitFileReferenceCommand(ctx context.Context, sessionID string, message PendingEvent, request FileReferenceCommandRequest) (FileReferenceCommandReserve, error)
	AcknowledgeFileReferenceDelivery(ctx context.Context, sessionID, commandID string, reservationVersion int64, writer FileReferenceWriter) (FileReferenceCommand, error)
	FinalizeFileReferenceCommand(ctx context.Context, sessionID, commandID string, finalize FileReferenceCommandFinalize) (FileReferenceCommand, error)
	RecoverFileReferenceCommand(ctx context.Context, sessionID, commandID, reason string) (FileReferenceCommand, error)
	FileReferenceCommand(ctx context.Context, sessionID, commandID string) (FileReferenceCommand, error)
	PendingFileReferenceCommands(ctx context.Context, sessionID string) ([]FileReferenceCommand, error)
}
