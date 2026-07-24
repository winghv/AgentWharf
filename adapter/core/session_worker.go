package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrInvalidSessionWorkerConfig = errors.New("invalid session worker config")

var (
	ErrDurableReceiptRequired          = errors.New("durable receipt is required")
	ErrInvalidDurableReceipt           = errors.New("invalid durable receipt")
	ErrUnsupportedSessionWorkerCommand = errors.New("unsupported session worker command")
	ErrUnsafeCommandReplay             = errors.New("unsafe command replay")
)

const durableReceiptRecoveryTimeout = time.Second

type CommandRoutingStatus string

const (
	CommandRoutingAccepted  CommandRoutingStatus = "accepted"
	CommandRoutingRejected  CommandRoutingStatus = "rejected"
	CommandRoutingDuplicate CommandRoutingStatus = "duplicate"
)

// CommandRoutingReceipt is the hop-by-hop command.ack result. It is not a
// durable operation result and never authorizes a Provider outcome.
type CommandRoutingReceipt struct {
	CommandID string
	Status    CommandRoutingStatus
	Reason    string
}

type LedgerOperationStatus string

const (
	LedgerOperationPending        LedgerOperationStatus = "pending"
	LedgerOperationCompleted      LedgerOperationStatus = "completed"
	LedgerOperationOutcomeUnknown LedgerOperationStatus = "outcome_unknown"
)

// LedgerOperationReceipt is the reference-only durable command operation
// identity. Operation ID/version are deliberately distinct from cmd_id.
type LedgerOperationReceipt struct {
	OperationID string
	Version     int64
	Status      LedgerOperationStatus
}

type EventProposalStatus string

const EventProposalAccepted EventProposalStatus = "accepted"

// EventProposalReceipt is the reference-only EventStore sequence assigned to
// one exact proposal ID. A retry returns the original sequence.
type EventProposalReceipt struct {
	ProposalID string
	Seq        int64
	Status     EventProposalStatus
}

type CommandOutcome string

const (
	CommandOutcomeCompleted      CommandOutcome = "completed"
	CommandOutcomeOutcomeUnknown CommandOutcome = "outcome_unknown"
)

type SessionWorkerCommand struct {
	CommandID string
	Type      string
}

// SessionWorkerDurableReceipts is the narrow Adapter-side seam to the
// Hub-owned Store contracts. It accepts only bounded opaque IDs and statuses;
// command/event content and credentials stay with the caller's trusted Hub
// path. Implementations must preserve exact retry identity and fail closed on
// stale authority.
type SessionWorkerDurableReceipts interface {
	PrepareProviderStart(context.Context, string, int) error
	ConfirmProviderStarted(context.Context, string, int) error
	PrepareCommand(context.Context, string, SessionWorkerCommand) (CommandRoutingReceipt, LedgerOperationReceipt, error)
	FinalizeCommand(context.Context, string, SessionWorkerCommand, LedgerOperationReceipt, CommandOutcome) error
	CommitEventProposal(context.Context, string, string) (EventProposalReceipt, error)
}

// SessionWorker is the single-Session owner for the existing Provider process
// lifecycle and its receipt-gated command/event side effects. Hub connection
// and credential ownership remain outside this boundary.
type SessionWorker struct {
	sessionID  string
	provider   *ProcessSupervisor
	receipts   SessionWorkerDurableReceipts
	commandMu  sync.Mutex
	proposalMu sync.Mutex
}

type SessionWorkerConfig struct {
	SessionID                 string
	Provider                  ProcessConfig
	RecoveryStartHandleSource RecoveryStartHandleSource
	RecoveryStartHandle       *RecoveryStartHandle
	DurableReceipts           SessionWorkerDurableReceipts
}

// RecoveryStartHandleSource exposes only the current opaque reference. The
// source owns lifecycle truth; callers cannot inspect or derive its value.
type RecoveryStartHandleSource interface {
	RecoveryStartHandle() (RecoveryStartHandle, error)
}

// BindRecoveryStartAdmission fences every child start against the last
// Store-admitted handle while delegating the actual prepare/started handshake
// to the existing T42B2 admission.
func BindRecoveryStartAdmission(provider ProcessConfig, source RecoveryStartHandleSource) (ProcessConfig, error) {
	return bindRecoveryStartAdmission(provider, source, nil)
}

func bindRecoveryStartAdmission(provider ProcessConfig, source RecoveryStartHandleSource, expected *RecoveryStartHandle) (ProcessConfig, error) {
	if source == nil || provider.StartAdmission == nil {
		return ProcessConfig{}, ErrRecoveryAuthorityLost
	}
	bound := &recoveryBoundProcessStartAdmission{source: source, delegate: provider.StartAdmission}
	if expected != nil {
		if expected.value == "" {
			return ProcessConfig{}, ErrRecoveryAuthorityLost
		}
		bound.expected = *expected
		bound.ready = true
	}
	provider.StartAdmission = bound
	return provider, nil
}

type recoveryBoundProcessStartAdmission struct {
	source   RecoveryStartHandleSource
	delegate ProcessStartAdmission

	mu       sync.Mutex
	expected RecoveryStartHandle
	ready    bool
}

func (a *recoveryBoundProcessStartAdmission) PrepareProcessStart(ctx context.Context, attempt int) error {
	if a == nil || a.source == nil || a.delegate == nil || attempt < 1 {
		return ErrRecoveryAuthorityLost
	}
	if err := a.verifyCurrent(); err != nil {
		return err
	}
	if err := a.delegate.PrepareProcessStart(ctx, attempt); err != nil {
		return err
	}
	return nil
}

func (a *recoveryBoundProcessStartAdmission) ConfirmProcessStarted(ctx context.Context, attempt int) error {
	if a == nil || a.source == nil || a.delegate == nil || attempt < 1 {
		return ErrRecoveryAuthorityLost
	}
	if err := a.delegate.ConfirmProcessStarted(ctx, attempt); err != nil {
		return err
	}
	handle, err := a.source.RecoveryStartHandle()
	if err != nil {
		return fmt.Errorf("%w: admitted child has no recovery handle", ErrRecoveryAuthorityLost)
	}
	a.mu.Lock()
	a.expected = handle
	a.ready = true
	a.mu.Unlock()
	return nil
}

func (a *recoveryBoundProcessStartAdmission) verifyCurrent() error {
	handle, err := a.source.RecoveryStartHandle()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		if !a.ready {
			// The first child has no committed handle yet. T42B2's delegate
			// will establish it before this wrapper permits a later retry.
			return nil
		}
		return fmt.Errorf("%w: current recovery handle unavailable", ErrRecoveryAuthorityLost)
	}
	if !a.ready {
		return nil
	}
	if a.expected.value != handle.value {
		return fmt.Errorf("%w: recovery handle replaced", ErrRecoveryAuthorityLost)
	}
	return nil
}

func NewSessionWorker(cfg SessionWorkerConfig) (*SessionWorker, error) {
	return newSessionWorker(cfg, execProcessRunner{})
}

func newSessionWorker(cfg SessionWorkerConfig, runner processRunner) (*SessionWorker, error) {
	if cfg.SessionID == "" {
		return nil, ErrInvalidSessionWorkerConfig
	}
	if cfg.RecoveryStartHandleSource != nil {
		if cfg.Provider.StartAdmission == nil {
			return nil, ErrRecoveryAuthorityLost
		}
		bound, err := bindRecoveryStartAdmission(cfg.Provider, cfg.RecoveryStartHandleSource, cfg.RecoveryStartHandle)
		if err != nil {
			return nil, err
		}
		cfg.Provider = bound
	}
	if cfg.DurableReceipts != nil {
		cfg.Provider.StartAdmission = &durableReceiptStartAdmission{
			sessionID: cfg.SessionID,
			receipts:  cfg.DurableReceipts,
			delegate:  cfg.Provider.StartAdmission,
		}
	}
	provider, err := newProcessSupervisor(cfg.Provider, runner)
	if err != nil {
		return nil, err
	}
	return &SessionWorker{sessionID: cfg.SessionID, provider: provider, receipts: cfg.DurableReceipts}, nil
}

func (w *SessionWorker) SessionID() string {
	if w == nil {
		return ""
	}
	return w.sessionID
}

func (w *SessionWorker) Events() <-chan ProcessEvent {
	if w == nil || w.provider == nil {
		return nil
	}
	return w.provider.Events()
}

func (w *SessionWorker) Run(ctx context.Context) error {
	if w == nil || w.provider == nil {
		return ErrInvalidSessionWorkerConfig
	}
	return w.provider.Run(ctx)
}

func (w *SessionWorker) Stop(ctx context.Context) error {
	if w == nil || w.provider == nil {
		return ErrInvalidSessionWorkerConfig
	}
	return w.provider.Stop(ctx)
}

// DeliverCommand waits for both the routing acknowledgement and durable
// ledger operation receipt before invoking the Provider side effect. Any
// ambiguous result is finalized as outcome_unknown and is never replayed.
func (w *SessionWorker) DeliverCommand(ctx context.Context, command SessionWorkerCommand, apply func(context.Context) error) (CommandRoutingReceipt, error) {
	if w == nil || w.receipts == nil || apply == nil {
		return CommandRoutingReceipt{}, ErrDurableReceiptRequired
	}
	if command.CommandID == "" {
		return CommandRoutingReceipt{}, fmt.Errorf("%w: command ID is required", ErrInvalidDurableReceipt)
	}
	if !supportedSessionWorkerCommand(command.Type) {
		return CommandRoutingReceipt{}, ErrUnsupportedSessionWorkerCommand
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.commandMu.Lock()
	defer w.commandMu.Unlock()
	routing, operation, err := w.receipts.PrepareCommand(ctx, w.sessionID, command)
	if err != nil {
		return CommandRoutingReceipt{}, fmt.Errorf("prepare durable command: %w", err)
	}
	if routing.CommandID != command.CommandID || !validCommandRoutingStatus(routing.Status) {
		return CommandRoutingReceipt{}, fmt.Errorf("%w: command routing receipt", ErrInvalidDurableReceipt)
	}
	if routing.Status != CommandRoutingAccepted {
		return routing, nil
	}
	if operation.OperationID == "" || operation.Version < 1 || operation.Status != LedgerOperationPending {
		return CommandRoutingReceipt{}, fmt.Errorf("%w: ledger operation receipt", ErrInvalidDurableReceipt)
	}

	if err := apply(ctx); err != nil {
		return routing, w.finalizeCommandUnknown(command, operation, err)
	}
	if err := w.receipts.FinalizeCommand(ctx, w.sessionID, command, operation, CommandOutcomeCompleted); err != nil {
		return routing, w.finalizeCommandUnknown(command, operation, err)
	}
	return routing, nil
}

// ProposeEvent commits an exact opaque proposal ID before publishing its
// authoritative sequence. Repeating the same ID therefore returns the Store's
// original sequence and never invents a new event identity.
func (w *SessionWorker) ProposeEvent(ctx context.Context, proposalID string, publish func(int64) error) (EventProposalReceipt, error) {
	if w == nil || w.receipts == nil || publish == nil {
		return EventProposalReceipt{}, ErrDurableReceiptRequired
	}
	if proposalID == "" {
		return EventProposalReceipt{}, fmt.Errorf("%w: proposal ID is required", ErrInvalidDurableReceipt)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.proposalMu.Lock()
	defer w.proposalMu.Unlock()
	receipt, err := w.receipts.CommitEventProposal(ctx, w.sessionID, proposalID)
	if err != nil {
		return EventProposalReceipt{}, fmt.Errorf("commit durable event proposal: %w", err)
	}
	if receipt.ProposalID != proposalID || receipt.Seq < 1 || receipt.Status != EventProposalAccepted {
		return EventProposalReceipt{}, fmt.Errorf("%w: event proposal receipt", ErrInvalidDurableReceipt)
	}
	if err := publish(receipt.Seq); err != nil {
		return receipt, fmt.Errorf("publish committed event sequence %d: %w", receipt.Seq, err)
	}
	return receipt, nil
}

func (w *SessionWorker) finalizeCommandUnknown(command SessionWorkerCommand, operation LedgerOperationReceipt, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), durableReceiptRecoveryTimeout)
	defer cancel()
	if err := w.receipts.FinalizeCommand(ctx, w.sessionID, command, operation, CommandOutcomeOutcomeUnknown); err != nil {
		return fmt.Errorf("%w: %v; finalize outcome_unknown: %v", ErrUnsafeCommandReplay, cause, err)
	}
	return fmt.Errorf("%w: %v", ErrUnsafeCommandReplay, cause)
}

func supportedSessionWorkerCommand(commandType string) bool {
	switch commandType {
	case "session.send", "permission.respond", "session.interrupt", "session.stop":
		return true
	default:
		return false
	}
}

func validCommandRoutingStatus(status CommandRoutingStatus) bool {
	switch status {
	case CommandRoutingAccepted, CommandRoutingRejected, CommandRoutingDuplicate:
		return true
	default:
		return false
	}
}

type durableReceiptStartAdmission struct {
	sessionID string
	receipts  SessionWorkerDurableReceipts
	delegate  ProcessStartAdmission
}

func (a *durableReceiptStartAdmission) PrepareProcessStart(ctx context.Context, attempt int) error {
	if a == nil || a.receipts == nil || attempt < 1 {
		return ErrDurableReceiptRequired
	}
	if err := a.receipts.PrepareProviderStart(ctx, a.sessionID, attempt); err != nil {
		return fmt.Errorf("prepare durable start receipt: %w", err)
	}
	if a.delegate != nil {
		if err := a.delegate.PrepareProcessStart(ctx, attempt); err != nil {
			return err
		}
	}
	return nil
}

func (a *durableReceiptStartAdmission) ConfirmProcessStarted(ctx context.Context, attempt int) error {
	if a == nil || a.receipts == nil || attempt < 1 {
		return ErrDurableReceiptRequired
	}
	if a.delegate != nil {
		if err := a.delegate.ConfirmProcessStarted(ctx, attempt); err != nil {
			return err
		}
	}
	if err := a.receipts.ConfirmProviderStarted(ctx, a.sessionID, attempt); err != nil {
		return fmt.Errorf("confirm durable starting receipt: %w", err)
	}
	return nil
}
