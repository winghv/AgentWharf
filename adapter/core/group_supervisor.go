package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/winghv/agentwharf/store"
)

var (
	ErrInvalidGroupSupervisorConfig = errors.New("invalid group supervisor config")
	ErrInvalidGroupWorkerAdmission  = errors.New("invalid group worker admission")
	ErrMultiWorkerDisabled          = errors.New("multi-worker admission is disabled")
	ErrGroupCapacity                = errors.New("group worker capacity reached")
	ErrSessionWorkerExists          = errors.New("session worker already exists")
	ErrSessionWorkerNotFound        = errors.New("session worker not found")
	ErrWorkspaceWriterExists        = errors.New("workspace already has a writer")
	ErrRecoveryAuthorityLost        = errors.New("recovery authority is unavailable")
)

// SessionWorkerRunner retains only the process lifecycle needed by this pre-activation
// foundation. Credential delivery, command routing and cleanup are owned by
// their dedicated tasks.
type SessionWorkerRunner interface {
	Run(context.Context) error
	Stop(context.Context) error
}

type workspaceLeaseReserver interface {
	ReserveWorkspaceLease(context.Context, store.WorkspaceLeaseReserve) (store.WorkspaceLease, error)
}

// MultiWorkerActivation is a trusted, runtime-only gate. Until T42F and T42H
// provide a concrete implementation, a nil gate rejects every second Worker.
type MultiWorkerActivation interface {
	AllowMultiWorker(context.Context) error
}

type recoveryTuple struct {
	SessionID            string
	WorkerID             string
	WorkspaceKey         store.WorkspaceLeaseKey
	ConnectionEpoch      int64
	CredentialGeneration int64
	LeaseID              string
	AcceptedFence        int64
	ExpiresAt            time.Time
	Revoked              bool
	Terminal             bool
	Quarantined          bool
}

// ConnectionAuthorityLifecycle is the Adapter connection lifecycle that owns the
// current T42B0 receipt. A RecoveryAuthority is usable only while this lifecycle
// still proves the same Store-issued, non-secret connection tuple.
type ConnectionAuthorityLifecycle interface {
	VerifyConnectionAuthority(context.Context, store.ConnectionAuthorityReceipt) error
	RunWithConnectionAuthority(context.Context, store.ConnectionAuthorityReceipt, func(context.Context) error) error
}

type RecoveryAuthority struct {
	receipt   store.ConnectionAuthorityReceipt
	lifecycle ConnectionAuthorityLifecycle
}

func NewRecoveryAuthority(receipt store.ConnectionAuthorityReceipt, lifecycle ConnectionAuthorityLifecycle) (RecoveryAuthority, error) {
	authority := RecoveryAuthority{receipt: receipt, lifecycle: lifecycle}
	if err := validateRecoveryAuthority(authority); err != nil {
		return RecoveryAuthority{}, err
	}
	return authority, nil
}

func (a RecoveryAuthority) verify(ctx context.Context) error {
	if err := validateRecoveryAuthority(a); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return a.lifecycle.VerifyConnectionAuthority(ctx, a.receipt)
}

func (a RecoveryAuthority) run(ctx context.Context, run func(context.Context) error) error {
	if err := validateRecoveryAuthority(a); err != nil {
		return err
	}
	if run == nil {
		return ErrRecoveryAuthorityLost
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return a.lifecycle.RunWithConnectionAuthority(ctx, a.receipt, run)
}

type GroupSupervisorConfig struct {
	MaxWorkers int
	Leases     workspaceLeaseReserver
	Activation MultiWorkerActivation
	NewWorker  func(SessionWorkerConfig) (SessionWorkerRunner, error)
}

// GroupWorkerAdmission contains the Store-derived opaque workspace tuple for
// one prospective writer. It is never copied into protocol frames or Provider
// process configuration.
type GroupWorkerAdmission struct {
	WorkerID  string
	SessionID string
	Worker    SessionWorkerConfig
	Lease     store.WorkspaceLeaseReserve
}

// GroupWorkerRecovery keeps ephemeral process setup separate from the tuple
// that a trusted lifecycle validates against durable authority.
type GroupWorkerRecovery struct {
	Admission GroupWorkerAdmission
	Authority RecoveryAuthority
}

type supervisedWorker struct {
	worker       SessionWorkerRunner
	workspaceKey store.WorkspaceLeaseKey
	recovery     *RecoveryAuthority
	runMu        *sync.Mutex
}

// GroupSupervisor owns bounded in-process membership. Durable lease release
// remains exclusively with the fixed-entry cleanup path after quiescence.
type GroupSupervisor struct {
	maxWorkers int
	leases     workspaceLeaseReserver
	activation MultiWorkerActivation
	newWorker  func(SessionWorkerConfig) (SessionWorkerRunner, error)

	mu          sync.Mutex
	bySession   map[string]supervisedWorker
	byWorkspace map[store.WorkspaceLeaseKey]string
}

func NewGroupSupervisor(cfg GroupSupervisorConfig) (*GroupSupervisor, error) {
	if cfg.MaxWorkers < 1 || cfg.Leases == nil {
		return nil, ErrInvalidGroupSupervisorConfig
	}
	if cfg.NewWorker == nil {
		cfg.NewWorker = func(workerCfg SessionWorkerConfig) (SessionWorkerRunner, error) {
			return NewSessionWorker(workerCfg)
		}
	}
	return &GroupSupervisor{
		maxWorkers:  cfg.MaxWorkers,
		leases:      cfg.Leases,
		activation:  cfg.Activation,
		newWorker:   cfg.NewWorker,
		bySession:   make(map[string]supervisedWorker),
		byWorkspace: make(map[store.WorkspaceLeaseKey]string),
	}, nil
}

// Admit reserves durable writer authority before constructing a Worker. A
// reservation failure therefore proves that no Provider spawn can follow.
func (s *GroupSupervisor) Admit(ctx context.Context, admission GroupWorkerAdmission) error {
	if s == nil {
		return ErrInvalidGroupSupervisorConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateGroupWorkerAdmission(admission); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.bySession[admission.SessionID]; exists {
		return ErrSessionWorkerExists
	}
	if _, exists := s.byWorkspace[admission.Lease.Key]; exists {
		return ErrWorkspaceWriterExists
	}
	if len(s.bySession) >= s.maxWorkers {
		return ErrGroupCapacity
	}
	if len(s.bySession) > 0 {
		if s.activation == nil {
			return ErrMultiWorkerDisabled
		}
		if err := s.activation.AllowMultiWorker(ctx); err != nil {
			return fmt.Errorf("%w: %v", ErrMultiWorkerDisabled, err)
		}
	}
	if _, err := s.leases.ReserveWorkspaceLease(ctx, admission.Lease); err != nil {
		return fmt.Errorf("reserve workspace writer: %w", err)
	}
	worker, err := s.newWorker(admission.Worker)
	if err != nil {
		return fmt.Errorf("construct session worker: %w", err)
	}
	if worker == nil {
		return fmt.Errorf("construct session worker: %w", ErrInvalidGroupWorkerAdmission)
	}
	s.bySession[admission.SessionID] = supervisedWorker{worker: worker, runMu: &sync.Mutex{}}
	s.byWorkspace[admission.Lease.Key] = admission.SessionID
	return nil
}

// Recover reconstructs membership only after trusted live authority verifies
// the durable tuple. It deliberately does not reserve or release a workspace.
func (s *GroupSupervisor) Recover(ctx context.Context, recovery GroupWorkerRecovery) error {
	if s == nil || validateGroupWorkerRecovery(recovery) != nil {
		return ErrRecoveryAuthorityLost
	}
	if ctx == nil {
		ctx = context.Background()
	}
	needsActivation, err := s.recoveryNeedsActivation(recovery.Admission)
	if err != nil {
		return err
	}
	if needsActivation {
		if s.activation == nil {
			return ErrMultiWorkerDisabled
		}
		if err := s.activation.AllowMultiWorker(ctx); err != nil {
			return fmt.Errorf("%w: %v", ErrMultiWorkerDisabled, err)
		}
	}
	if err := recovery.Authority.verify(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrRecoveryAuthorityLost, err)
	}
	worker, err := s.newWorker(recovery.Admission.Worker)
	if err != nil || worker == nil {
		return ErrRecoveryAuthorityLost
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.bySession[recovery.Admission.SessionID]; exists {
		return ErrSessionWorkerExists
	}
	if _, exists := s.byWorkspace[recovery.Admission.Lease.Key]; exists {
		return ErrWorkspaceWriterExists
	}
	if len(s.bySession) >= s.maxWorkers {
		return ErrGroupCapacity
	}
	if len(s.bySession) > 0 {
		if !needsActivation || s.activation == nil {
			return ErrMultiWorkerDisabled
		}
	}
	authority := recovery.Authority
	s.bySession[recovery.Admission.SessionID] = supervisedWorker{worker: worker, workspaceKey: recovery.Admission.Lease.Key, recovery: &authority, runMu: &sync.Mutex{}}
	s.byWorkspace[recovery.Admission.Lease.Key] = recovery.Admission.SessionID
	return nil
}

func (s *GroupSupervisor) Run(ctx context.Context, sessionID string) error {
	member, err := s.worker(sessionID)
	if err != nil {
		return err
	}
	if member.runMu != nil {
		member.runMu.Lock()
		defer member.runMu.Unlock()
	}
	if member.recovery != nil {
		if err := member.recovery.run(ctx, member.worker.Run); err != nil {
			s.fenceRecoveredWorker(sessionID, member)
			return fmt.Errorf("%w: %v", ErrRecoveryAuthorityLost, err)
		}
		return nil
	}
	return member.worker.Run(ctx)
}

func (s *GroupSupervisor) Stop(ctx context.Context, sessionID string) error {
	member, err := s.worker(sessionID)
	if err != nil {
		return err
	}
	return member.worker.Stop(ctx)
}

func (s *GroupSupervisor) WorkerCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bySession)
}

func (s *GroupSupervisor) recoveryNeedsActivation(admission GroupWorkerAdmission) (bool, error) {
	if s == nil {
		return false, ErrInvalidGroupSupervisorConfig
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.bySession[admission.SessionID]; exists {
		return false, ErrSessionWorkerExists
	}
	if _, exists := s.byWorkspace[admission.Lease.Key]; exists {
		return false, ErrWorkspaceWriterExists
	}
	if len(s.bySession) >= s.maxWorkers {
		return false, ErrGroupCapacity
	}
	return len(s.bySession) > 0, nil
}

func (s *GroupSupervisor) worker(sessionID string) (supervisedWorker, error) {
	if s == nil || sessionID == "" {
		return supervisedWorker{}, ErrSessionWorkerNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.bySession[sessionID]
	if !ok {
		return supervisedWorker{}, ErrSessionWorkerNotFound
	}
	return worker, nil
}

func (s *GroupSupervisor) fenceRecoveredWorker(sessionID string, member supervisedWorker) {
	if s == nil || member.recovery == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.bySession[sessionID]
	if !ok || current.recovery != member.recovery {
		return
	}
	delete(s.bySession, sessionID)
	delete(s.byWorkspace, member.workspaceKey)
}

func validateGroupWorkerAdmission(admission GroupWorkerAdmission) error {
	if admission.WorkerID == "" || admission.SessionID == "" || admission.Worker.SessionID != admission.SessionID {
		return ErrInvalidGroupWorkerAdmission
	}
	if admission.Lease.Owner.WorkerID != admission.WorkerID || admission.Lease.Owner.SessionID != admission.SessionID {
		return ErrInvalidGroupWorkerAdmission
	}
	if admission.Lease.Owner.ConnectionEpoch < 1 || admission.Lease.Owner.CredentialGeneration < 1 || admission.Lease.Owner.LeaseID == "" {
		return ErrInvalidGroupWorkerAdmission
	}
	if admission.Lease.ExpiresAt.IsZero() || !admission.Lease.ExpiresAt.After(time.Now()) || isZeroWorkspaceLeaseKey(admission.Lease.Key) {
		return ErrInvalidGroupWorkerAdmission
	}
	return nil
}

func validateGroupWorkerRecovery(recovery GroupWorkerRecovery) error {
	if validateGroupWorkerAdmission(recovery.Admission) != nil {
		return ErrRecoveryAuthorityLost
	}
	if err := validateRecoveryAuthority(recovery.Authority); err != nil {
		return ErrRecoveryAuthorityLost
	}
	tuple := recoveryTuple{
		SessionID:            recovery.Authority.receipt.SessionID,
		WorkerID:             recovery.Admission.WorkerID,
		WorkspaceKey:         recovery.Admission.Lease.Key,
		ConnectionEpoch:      recovery.Authority.receipt.ConnectionEpoch,
		CredentialGeneration: recovery.Authority.receipt.CredentialGeneration,
		LeaseID:              recovery.Authority.receipt.WriterLeaseID,
		AcceptedFence:        recovery.Authority.receipt.AcceptedFence,
		ExpiresAt:            recovery.Authority.receipt.ExpiresAt,
	}
	if tuple.SessionID != recovery.Admission.SessionID ||
		tuple.WorkerID != recovery.Admission.WorkerID ||
		tuple.WorkspaceKey != recovery.Admission.Lease.Key ||
		tuple.ConnectionEpoch != recovery.Admission.Lease.Owner.ConnectionEpoch ||
		tuple.CredentialGeneration != recovery.Admission.Lease.Owner.CredentialGeneration ||
		tuple.LeaseID != recovery.Admission.Lease.Owner.LeaseID ||
		tuple.AcceptedFence < 1 || tuple.ExpiresAt.IsZero() || !tuple.ExpiresAt.After(time.Now()) ||
		tuple.Revoked || tuple.Terminal || tuple.Quarantined {
		return ErrRecoveryAuthorityLost
	}
	return nil
}

func validateRecoveryAuthority(authority RecoveryAuthority) error {
	if authority.lifecycle == nil {
		return ErrRecoveryAuthorityLost
	}
	receipt := authority.receipt
	if receipt.SessionID == "" || receipt.ConnectionEpoch < 1 || receipt.CredentialGeneration < 1 ||
		receipt.AcceptedFence < 1 || receipt.WriterLeaseID == "" || receipt.ExpiresAt.IsZero() || !receipt.ExpiresAt.After(time.Now()) {
		return ErrRecoveryAuthorityLost
	}
	return nil
}

func isZeroWorkspaceLeaseKey(key store.WorkspaceLeaseKey) bool {
	var zero store.WorkspaceLeaseKey
	return key == zero
}
