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

// GroupSupervisor owns bounded in-process membership. Durable lease release
// remains exclusively with the fixed-entry cleanup path after quiescence.
type GroupSupervisor struct {
	maxWorkers int
	leases     workspaceLeaseReserver
	activation MultiWorkerActivation
	newWorker  func(SessionWorkerConfig) (SessionWorkerRunner, error)

	mu          sync.Mutex
	bySession   map[string]SessionWorkerRunner
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
		bySession:   make(map[string]SessionWorkerRunner),
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
	s.bySession[admission.SessionID] = worker
	s.byWorkspace[admission.Lease.Key] = admission.SessionID
	return nil
}

func (s *GroupSupervisor) Run(ctx context.Context, sessionID string) error {
	worker, err := s.worker(sessionID)
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}

func (s *GroupSupervisor) Stop(ctx context.Context, sessionID string) error {
	worker, err := s.worker(sessionID)
	if err != nil {
		return err
	}
	return worker.Stop(ctx)
}

func (s *GroupSupervisor) WorkerCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bySession)
}

func (s *GroupSupervisor) worker(sessionID string) (SessionWorkerRunner, error) {
	if s == nil || sessionID == "" {
		return nil, ErrSessionWorkerNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.bySession[sessionID]
	if !ok {
		return nil, ErrSessionWorkerNotFound
	}
	return worker, nil
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

func isZeroWorkspaceLeaseKey(key store.WorkspaceLeaseKey) bool {
	var zero store.WorkspaceLeaseKey
	return key == zero
}
