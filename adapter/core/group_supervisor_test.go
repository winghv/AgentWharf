package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestGroupSupervisorDeniesMultiWorkerWithoutActivation(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	created := 0
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		NewWorker: func(SessionWorkerConfig) (SessionWorkerRunner, error) {
			created++
			return &fakeGroupWorker{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_1", "ses_1", 1)); err != nil {
		t.Fatalf("Admit(first) error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_2", "ses_2", 2)); !errors.Is(err, ErrMultiWorkerDisabled) {
		t.Fatalf("Admit(second) error = %v, want ErrMultiWorkerDisabled", err)
	}
	if leases.reserveCount() != 1 || created != 1 {
		t.Fatalf("second worker reserved/spawned despite disabled activation: reserves=%d created=%d", leases.reserveCount(), created)
	}
}

func TestGroupSupervisorReservesBeforeCreatingWorker(t *testing.T) {
	reserveErr := errors.New("durable reserve rejected")
	leases := &recordingWorkspaceLeaseReserver{err: reserveErr}
	created := 0
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 1,
		Leases:     leases,
		NewWorker: func(SessionWorkerConfig) (SessionWorkerRunner, error) {
			created++
			return &fakeGroupWorker{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_1", "ses_1", 1)); !errors.Is(err, reserveErr) {
		t.Fatalf("Admit() error = %v, want reserve error", err)
	}
	if created != 0 {
		t.Fatalf("worker was created before durable reserve failure: %d", created)
	}
}

func TestGroupSupervisorEnforcesSessionWorkspaceAndCapacity(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		Activation: alwaysActiveMultiWorkerGate{},
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return &fakeGroupWorker{}, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_1", "ses_1", 1)); err != nil {
		t.Fatalf("Admit(first) error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_same_workspace", "ses_same_workspace", 1)); !errors.Is(err, ErrWorkspaceWriterExists) {
		t.Fatalf("Admit(duplicate workspace) error = %v, want ErrWorkspaceWriterExists", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_2", "ses_2", 2)); err != nil {
		t.Fatalf("Admit(second) error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_3", "ses_3", 3)); !errors.Is(err, ErrGroupCapacity) {
		t.Fatalf("Admit(capacity) error = %v, want ErrGroupCapacity", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_4", "ses_1", 4)); !errors.Is(err, ErrSessionWorkerExists) {
		t.Fatalf("Admit(duplicate session) error = %v, want ErrSessionWorkerExists", err)
	}
	if leases.reserveCount() != 2 {
		t.Fatalf("rejected admissions reached lease reservation: %d", leases.reserveCount())
	}
}

func TestGroupSupervisorRejectsInvalidTupleAndFailedActivationBeforeReserve(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		Activation: failingMultiWorkerGate{},
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return &fakeGroupWorker{}, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	invalid := validGroupWorkerAdmission("worker_invalid", "ses_invalid", 3)
	invalid.Lease.Owner.CredentialGeneration = 0
	if err := group.Admit(context.Background(), invalid); !errors.Is(err, ErrInvalidGroupWorkerAdmission) {
		t.Fatalf("Admit(invalid) error = %v, want ErrInvalidGroupWorkerAdmission", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_1", "ses_1", 1)); err != nil {
		t.Fatalf("Admit(first) error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_2", "ses_2", 2)); !errors.Is(err, ErrMultiWorkerDisabled) {
		t.Fatalf("Admit(activation failure) error = %v, want ErrMultiWorkerDisabled", err)
	}
	if leases.reserveCount() != 1 {
		t.Fatalf("invalid or disabled admission reached durable reservation: %d", leases.reserveCount())
	}
}

func TestGroupSupervisorSerializesConcurrentWorkspaceAdmission(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		Activation: alwaysActiveMultiWorkerGate{},
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return &fakeGroupWorker{}, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	admissions := []GroupWorkerAdmission{
		validGroupWorkerAdmission("worker_1", "ses_1", 1),
		validGroupWorkerAdmission("worker_2", "ses_2", 1),
	}
	errs := make(chan error, len(admissions))
	var start sync.WaitGroup
	start.Add(1)
	for _, admission := range admissions {
		admission := admission
		go func() {
			start.Wait()
			errs <- group.Admit(context.Background(), admission)
		}()
	}
	start.Done()
	var accepted, duplicate int
	for range admissions {
		switch err := <-errs; {
		case err == nil:
			accepted++
		case errors.Is(err, ErrWorkspaceWriterExists):
			duplicate++
		default:
			t.Fatalf("concurrent Admit() error = %v", err)
		}
	}
	if accepted != 1 || duplicate != 1 || leases.reserveCount() != 1 || group.WorkerCount() != 1 {
		t.Fatalf("concurrent admission accepted=%d duplicate=%d reserves=%d workers=%d", accepted, duplicate, leases.reserveCount(), group.WorkerCount())
	}
}

func TestGroupSupervisorOwnsWorkerLifecycle(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 1,
		Leases:     leases,
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_1", "ses_1", 1)); err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	if err := group.Run(context.Background(), "ses_1"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := group.Stop(context.Background(), "ses_1"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if worker.runs != 1 || worker.stops != 1 {
		t.Fatalf("worker lifecycle calls = run:%d stop:%d, want 1/1", worker.runs, worker.stops)
	}
	if err := group.Run(context.Background(), "ses_missing"); !errors.Is(err, ErrSessionWorkerNotFound) {
		t.Fatalf("Run(missing) error = %v, want ErrSessionWorkerNotFound", err)
	}
}

type fakeGroupWorker struct{ runs, stops int }

func (w *fakeGroupWorker) Run(context.Context) error {
	w.runs++
	return nil
}

func (w *fakeGroupWorker) Stop(context.Context) error {
	w.stops++
	return nil
}

type recordingWorkspaceLeaseReserver struct {
	mu       sync.Mutex
	reserves int
	err      error
}

func (r *recordingWorkspaceLeaseReserver) ReserveWorkspaceLease(_ context.Context, reserve store.WorkspaceLeaseReserve) (store.WorkspaceLease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reserves++
	if r.err != nil {
		return store.WorkspaceLease{}, r.err
	}
	return store.WorkspaceLease{Key: reserve.Key, Owner: reserve.Owner, Status: store.WorkspaceLeaseReserved, Version: 1, ExpiresAt: reserve.ExpiresAt}, nil
}

func (r *recordingWorkspaceLeaseReserver) reserveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reserves
}

type alwaysActiveMultiWorkerGate struct{}

func (alwaysActiveMultiWorkerGate) AllowMultiWorker(context.Context) error { return nil }

type failingMultiWorkerGate struct{}

func (failingMultiWorkerGate) AllowMultiWorker(context.Context) error {
	return errors.New("T42F/T42H receipts unavailable")
}

func validGroupWorkerAdmission(workerID, sessionID string, keyByte byte) GroupWorkerAdmission {
	var key store.WorkspaceLeaseKey
	key[0] = keyByte
	return GroupWorkerAdmission{
		WorkerID:  workerID,
		SessionID: sessionID,
		Worker: SessionWorkerConfig{
			SessionID: sessionID,
			Provider:  ProcessConfig{Command: ProcessCommand{Path: "provider"}},
		},
		Lease: store.WorkspaceLeaseReserve{
			Key: key,
			Owner: store.WorkspaceLeaseOwner{
				WorkerID: workerID, SessionID: sessionID, ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_" + workerID,
			},
			ExpiresAt: time.Now().Add(time.Minute),
		},
	}
}
