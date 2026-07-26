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
	// SessionID is an optional trusted routing assertion. Provider-controlled
	// values are never used to select a Worker and must match the bound Worker.
	SessionID string
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
	sessionID         string
	provider          *ProcessSupervisor
	receipts          SessionWorkerDurableReceipts
	credential        *SessionCredential
	rotation          *CredentialRotation
	ownership         ProcessTreeOwnership
	lease             QuiescenceLease
	metrics           *AdapterMetrics
	ownerObservations *sync.WaitGroup
	events            chan ProcessEvent
	ownerErrs         chan error
	stopMu            sync.Mutex
	stopped           bool
	stopErr           error
	commandMu         sync.Mutex
	proposalMu        sync.Mutex
}

type SessionWorkerConfig struct {
	SessionID                 string
	Provider                  ProcessConfig
	RecoveryStartHandleSource RecoveryStartHandleSource
	RecoveryStartHandle       *RecoveryStartHandle
	DurableReceipts           SessionWorkerDurableReceipts
	Credential                *SessionCredential
	ProcessOwnership          ProcessTreeOwnership
	QuiescenceLease           QuiescenceLease
	Metrics                   *AdapterMetrics
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
	if cfg.Credential != nil {
		if err := cfg.Credential.validate(cfg.SessionID, time.Now()); err != nil {
			return nil, err
		}
	}
	var ownerObservations sync.WaitGroup
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
	if cfg.ProcessOwnership != nil {
		cfg.Provider.StartAdmission = &ownedProcessStartAdmission{
			owner:       cfg.ProcessOwnership,
			delegate:    cfg.Provider.StartAdmission,
			onConfirmed: func() { ownerObservations.Add(1) },
		}
	}
	var rotation *CredentialRotation
	var err error
	if cfg.Credential != nil {
		rotation, err = NewCredentialRotation(cfg.SessionID, cfg.Credential, 1)
		if err != nil {
			return nil, err
		}
		rotationAdmission := &credentialRotationStartAdmission{
			rotation: rotation,
			delegate: cfg.Provider.StartAdmission,
		}
		cfg.Provider.StartAdmission = rotationAdmission
		if runner != nil {
			runner = &credentialRotationProcessRunner{delegate: runner, admission: rotationAdmission}
		}
	}
	provider, err := newProcessSupervisor(cfg.Provider, runner)
	if err != nil {
		return nil, err
	}
	worker := &SessionWorker{
		sessionID: cfg.SessionID, provider: provider, receipts: cfg.DurableReceipts,
		credential: cfg.Credential, rotation: rotation, ownership: cfg.ProcessOwnership, lease: cfg.QuiescenceLease,
		metrics:           cfg.Metrics,
		ownerObservations: &ownerObservations,
	}
	if cfg.ProcessOwnership != nil {
		worker.events = make(chan ProcessEvent, 128)
		worker.ownerErrs = make(chan error, 1)
		go worker.forwardEvents()
	}
	return worker, nil
}

func (w *SessionWorker) recordReceiptFailure() {
	if w != nil && w.metrics != nil {
		w.metrics.IncReceiptFailure()
	}
}

func (w *SessionWorker) SessionID() string {
	if w == nil {
		return ""
	}
	return w.sessionID
}

func (w *SessionWorker) PrepareCredentialRotation(rotationID string, credential *SessionCredential) error {
	if w == nil || w.rotation == nil {
		return ErrCredentialRotationUnavailable
	}
	return w.rotation.Prepare(rotationID, credential)
}

func (w *SessionWorker) AcknowledgeCredentialPossession(rotationID string, acceptedEpoch int64) (CredentialRotationReceipt, error) {
	if w == nil || w.rotation == nil {
		return CredentialRotationReceipt{}, ErrCredentialRotationUnavailable
	}
	return w.rotation.PossessionAck(rotationID, acceptedEpoch)
}

func (w *SessionWorker) ActivateCredentialRotation(receipt CredentialRotationReceipt) error {
	if w == nil || w.rotation == nil {
		return ErrCredentialRotationUnavailable
	}
	return w.rotation.Activate(receipt)
}

func (w *SessionWorker) RetryCredentialActivation(rotationID string) (CredentialRotationReceipt, error) {
	if w == nil || w.rotation == nil {
		return CredentialRotationReceipt{}, ErrCredentialRotationUnavailable
	}
	return w.rotation.RetryActivation(rotationID)
}

func (w *SessionWorker) ReconnectCredential(epoch, generation int64) error {
	if w == nil || w.rotation == nil {
		return ErrCredentialRotationUnavailable
	}
	return w.rotation.Reconnect(epoch, generation)
}

func (w *SessionWorker) CredentialRecoveryPermit() (CredentialRecoveryPermit, error) {
	if w == nil || w.rotation == nil {
		return CredentialRecoveryPermit{}, ErrCredentialRotationUnavailable
	}
	return w.rotation.RecoveryPermit()
}

func (w *SessionWorker) MarkCredentialAuthorityLost() {
	if w != nil && w.rotation != nil {
		w.rotation.MarkAuthorityLost()
	}
}

func (w *SessionWorker) RevokeCredential() {
	if w != nil && w.rotation != nil {
		w.rotation.Revoke()
	}
}

func (w *SessionWorker) TerminalCredential() {
	if w != nil && w.rotation != nil {
		w.rotation.Terminal()
	}
}

func (w *SessionWorker) validateActiveCredential() error {
	if w == nil {
		return ErrSessionCredentialRequired
	}
	if w.rotation != nil {
		return w.rotation.Authorize(time.Now())
	}
	if w.credential == nil {
		return ErrSessionCredentialRequired
	}
	return w.credential.validate(w.sessionID, time.Now())
}

func (w *SessionWorker) Events() <-chan ProcessEvent {
	if w == nil || w.provider == nil {
		return nil
	}
	if w.ownership == nil {
		return w.provider.Events()
	}
	return w.events
}

func (w *SessionWorker) Run(ctx context.Context) error {
	if w == nil || w.provider == nil {
		return ErrInvalidSessionWorkerConfig
	}
	if w.rotation != nil {
		if err := w.validateActiveCredential(); err != nil {
			return err
		}
	}
	if w.ownership == nil {
		return w.provider.Run(ctx)
	}
	if held, ok := w.lease.(interface{ Held() bool }); ok && !held.Held() {
		return ErrRecoveryAuthorityLost
	}
	w.stopMu.Lock()
	w.stopped, w.stopErr = false, nil
	w.stopMu.Unlock()
	runDone := make(chan error, 1)
	go func() { runDone <- w.provider.Run(ctx) }()
	cleanup := func(err error, label string) error {
		alreadyStopped := w.wasStopped()
		cleanupErr := w.stopOwned()
		if alreadyStopped {
			return err
		}
		if cleanupErr != nil && err != nil {
			return fmt.Errorf("%s: %v; ownership cleanup: %w", label, err, cleanupErr)
		}
		if cleanupErr != nil {
			return fmt.Errorf("ownership cleanup: %w", cleanupErr)
		}
		return err
	}
	select {
	case err := <-runDone:
		return cleanup(err, "provider run")
	case err := <-w.ownerErrs:
		return cleanup(err, "ownership event")
	case <-ctx.Done():
		return cleanup(ctx.Err(), "worker context")
	}
}
func (w *SessionWorker) stopOwned() error {
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return w.stopOnce(stopCtx)
}
func (w *SessionWorker) Stop(ctx context.Context) error {
	if w != nil && w.ownership != nil {
		return w.stopOnce(ctx)
	}
	return w.stopNow(ctx)
}
func (w *SessionWorker) wasStopped() bool {
	w.stopMu.Lock()
	defer w.stopMu.Unlock()
	return w.stopped
}
func (w *SessionWorker) stopOnce(ctx context.Context) error {
	w.stopMu.Lock()
	defer w.stopMu.Unlock()
	if w.stopped {
		return w.stopErr
	}
	w.stopped = true
	w.stopErr = w.stopNow(ctx)
	return w.stopErr
}
func (w *SessionWorker) stopNow(ctx context.Context) error {
	if w == nil || w.provider == nil {
		return ErrInvalidSessionWorkerConfig
	}
	if err := w.provider.Stop(ctx); err != nil {
		w.quarantine()
		return err
	}
	done := make(chan struct{})
	go func() { w.ownerObservations.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		w.quarantine()
		return ctx.Err()
	}
	if w.ownership != nil {
		if err := w.ownership.Quiesce(ctx); err != nil {
			w.quarantine()
			return err
		}
	}
	if w.lease != nil {
		if err := w.lease.Release(ctx); err != nil {
			w.quarantine()
			return err
		}
	}
	return nil
}
func (w *SessionWorker) quarantine() {
	if w.lease != nil {
		_ = w.lease.Quarantine(context.Background())
	}
}
func (w *SessionWorker) forwardEvents() {
	for event := range w.provider.Events() {
		if w.ownership != nil && event.Type == ProcessEventStarted {
			if err := w.ownership.ObserveStarted(context.Background(), event); err != nil {
				select {
				case w.ownerErrs <- err:
				default:
				}
			}
			w.ownerObservations.Done()
		}
		w.events <- event
	}
}

// RouteCommand verifies the private credential binding before entering the
// durable receipt path. A missing credential is deny-by-default for routed
// work; legacy callers that use DeliverCommand directly retain T42C behavior.
func (w *SessionWorker) RouteCommand(ctx context.Context, command SessionWorkerCommand, apply func(context.Context) error) (CommandRoutingReceipt, error) {
	if w == nil || (w.credential == nil && w.rotation == nil) {
		return CommandRoutingReceipt{}, ErrSessionCredentialRequired
	}
	if w.credential != nil || w.rotation != nil {
		if err := w.validateActiveCredential(); err != nil {
			return CommandRoutingReceipt{}, err
		}
	}
	if command.SessionID != "" && command.SessionID != w.sessionID {
		return CommandRoutingReceipt{}, ErrSessionCredentialMismatch
	}
	command.SessionID = w.sessionID
	return w.DeliverCommand(ctx, command, apply)
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
	if command.SessionID != "" && command.SessionID != w.sessionID {
		return CommandRoutingReceipt{}, ErrSessionCredentialMismatch
	}
	if w.credential != nil || w.rotation != nil {
		if err := w.validateActiveCredential(); err != nil {
			return CommandRoutingReceipt{}, err
		}
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
		w.recordReceiptFailure()
		return CommandRoutingReceipt{}, fmt.Errorf("prepare durable command: %w", err)
	}
	if routing.CommandID != command.CommandID || !validCommandRoutingStatus(routing.Status) {
		w.recordReceiptFailure()
		return CommandRoutingReceipt{}, fmt.Errorf("%w: command routing receipt", ErrInvalidDurableReceipt)
	}
	if routing.Status != CommandRoutingAccepted {
		return routing, nil
	}
	if operation.OperationID == "" || operation.Version < 1 || operation.Status != LedgerOperationPending {
		w.recordReceiptFailure()
		return CommandRoutingReceipt{}, fmt.Errorf("%w: ledger operation receipt", ErrInvalidDurableReceipt)
	}
	execute := func() error {
		if err := apply(ctx); err != nil {
			return w.finalizeCommandUnknown(command, operation, err)
		}
		if err := w.receipts.FinalizeCommand(ctx, w.sessionID, command, operation, CommandOutcomeCompleted); err != nil {
			w.recordReceiptFailure()
			return w.finalizeCommandUnknown(command, operation, err)
		}
		return nil
	}
	if w.rotation != nil {
		if err := w.rotation.withAuthorized(execute); err != nil {
			if isCredentialFenceError(err) {
				return routing, w.finalizeCommandUnknown(command, operation, err)
			}
			return routing, err
		}
		return routing, nil
	}
	if err := execute(); err != nil {
		return routing, err
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
	if w.credential != nil || w.rotation != nil {
		if err := w.validateActiveCredential(); err != nil {
			return EventProposalReceipt{}, err
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.proposalMu.Lock()
	defer w.proposalMu.Unlock()
	var receipt EventProposalReceipt
	commitAndPublish := func() error {
		var err error
		receipt, err = w.receipts.CommitEventProposal(ctx, w.sessionID, proposalID)
		if err != nil {
			w.recordReceiptFailure()
			return fmt.Errorf("commit durable event proposal: %w", err)
		}
		if receipt.ProposalID != proposalID || receipt.Seq < 1 || receipt.Status != EventProposalAccepted {
			w.recordReceiptFailure()
			return fmt.Errorf("%w: event proposal receipt", ErrInvalidDurableReceipt)
		}
		if err := publish(receipt.Seq); err != nil {
			return fmt.Errorf("publish committed event sequence %d: %w", receipt.Seq, err)
		}
		return nil
	}
	if w.rotation != nil {
		if err := w.rotation.withAuthorized(commitAndPublish); err != nil {
			return receipt, err
		}
		return receipt, nil
	}
	if err := commitAndPublish(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func isCredentialFenceError(err error) bool {
	return errors.Is(err, ErrCredentialAuthorityLost) || errors.Is(err, ErrCredentialTerminal) ||
		errors.Is(err, ErrSessionCredentialExpired) || errors.Is(err, ErrCredentialRotationUnavailable) ||
		errors.Is(err, ErrCredentialRecoveryRequired)
}

func (w *SessionWorker) finalizeCommandUnknown(command SessionWorkerCommand, operation LedgerOperationReceipt, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), durableReceiptRecoveryTimeout)
	defer cancel()
	if err := w.receipts.FinalizeCommand(ctx, w.sessionID, command, operation, CommandOutcomeOutcomeUnknown); err != nil {
		w.recordReceiptFailure()
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
type credentialRotationStartAdmission struct {
	rotation *CredentialRotation
	delegate ProcessStartAdmission
	startMu  sync.Mutex
	started  bool
}

type credentialRotationProcessRunner struct {
	delegate  processRunner
	admission *credentialRotationStartAdmission
}

func (r *credentialRotationProcessRunner) Start(command ProcessCommand) (processHandle, error) {
	if r == nil || r.delegate == nil {
		return nil, ErrInvalidProcessConfig
	}
	handle, err := r.delegate.Start(command)
	if err != nil && r.admission != nil {
		r.admission.AbortProcessStart()
	}
	return handle, err
}

type ownedProcessStartAdmission struct {
	owner       ProcessTreeOwnership
	delegate    ProcessStartAdmission
	onConfirmed func()
}

func (a *credentialRotationStartAdmission) PrepareProcessStart(ctx context.Context, attempt int) error {
	if a == nil || a.rotation == nil || attempt < 1 {
		return ErrCredentialAuthorityLost
	}
	a.startMu.Lock()
	a.rotation.mu.Lock()
	a.started = true
	if err := a.rotation.authorizeLocked(time.Now()); err != nil {
		a.releaseStartFence()
		return err
	}
	if a.delegate != nil {
		if err := a.delegate.PrepareProcessStart(ctx, attempt); err != nil {
			a.releaseStartFence()
			return err
		}
	}
	return nil
}

func (a *credentialRotationStartAdmission) ConfirmProcessStarted(ctx context.Context, attempt int) error {
	if a == nil || a.rotation == nil || attempt < 1 {
		return ErrCredentialAuthorityLost
	}
	if !a.started {
		return ErrCredentialAuthorityLost
	}
	var err error
	if a.delegate != nil {
		err = a.delegate.ConfirmProcessStarted(ctx, attempt)
	}
	if err == nil {
		err = a.rotation.authorizeLocked(time.Now())
	}
	a.releaseStartFence()
	return err
}

func (a *credentialRotationStartAdmission) AbortProcessStart() {
	if a == nil || a.rotation == nil || !a.started {
		return
	}
	a.releaseStartFence()
}

func (a *credentialRotationStartAdmission) releaseStartFence() {
	a.started = false
	a.rotation.mu.Unlock()
	a.startMu.Unlock()
}

func (a *ownedProcessStartAdmission) PrepareProcessStart(ctx context.Context, attempt int) error {
	if a == nil || a.owner == nil {
		return ErrRecoveryAuthorityLost
	}
	if err := a.owner.PrepareStart(ctx, attempt); err != nil {
		return err
	}
	if a.delegate != nil {
		if err := a.delegate.PrepareProcessStart(ctx, attempt); err != nil {
			_ = a.owner.AbortStart(context.Background(), attempt)
			return err
		}
	}
	return nil
}
func (a *ownedProcessStartAdmission) ConfirmProcessStarted(ctx context.Context, attempt int) error {
	if a == nil || a.owner == nil {
		return ErrRecoveryAuthorityLost
	}
	if a.delegate != nil {
		if err := a.delegate.ConfirmProcessStarted(ctx, attempt); err != nil {
			_ = a.owner.AbortStart(context.Background(), attempt)
			return err
		}
	}
	if a.onConfirmed != nil {
		a.onConfirmed()
	}
	return nil
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
