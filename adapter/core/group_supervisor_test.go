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

func TestGroupSupervisorRecoversOnlyVerifiedCurrentTuple(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	verifier := &recordingRecoveryVerifier{}
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if group.WorkerCount() != 1 || leases.reserveCount() != 0 || verifier.calls != 1 {
		t.Fatalf("recovery membership/reserve/verify = %d/%d/%d, want 1/0/1", group.WorkerCount(), leases.reserveCount(), verifier.calls)
	}
	if err := group.Run(context.Background(), "ses_1"); err != nil {
		t.Fatalf("Run(recovered) error = %v", err)
	}
	if worker.runs != 1 || verifier.calls != 2 {
		t.Fatalf("recovered run/verify = %d/%d, want 1/2", worker.runs, verifier.calls)
	}
}

func TestGroupSupervisorFailsClosedForRecoveryLossAndQueuedRevocation(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	verifier := &recordingRecoveryVerifier{}
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	verifier.err = errors.New("revoked after recovery")
	if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Run(revoked queued recovery) error = %v, want ErrRecoveryAuthorityLost", err)
	}
	if worker.runs != 0 || leases.reserveCount() != 0 {
		t.Fatalf("queued authority loss ran/reserved provider: runs=%d reserves=%d", worker.runs, leases.reserveCount())
	}
	verifier.err = nil
	if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrSessionWorkerNotFound) {
		t.Fatalf("Run(after fenced recovery) error = %v, want ErrSessionWorkerNotFound", err)
	}
	if group.WorkerCount() != 0 || worker.runs != 0 {
		t.Fatalf("fenced recovery remained runnable: workers=%d runs=%d", group.WorkerCount(), worker.runs)
	}
	missing, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 1, Leases: leases, NewWorker: func(SessionWorkerConfig) (SessionWorkerRunner, error) { return &fakeGroupWorker{}, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor(missing recovery verifier) error = %v", err)
	}
	missingAuthority := validGroupWorkerRecovery("worker_2", "ses_2", 2, verifier)
	missingAuthority.Authority = RecoveryAuthority{}
	if err := missing.Recover(context.Background(), missingAuthority); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Recover(without verifier) error = %v, want ErrRecoveryAuthorityLost", err)
	}
}

func TestGroupSupervisorRejectsRecoveryWhenLifecycleDeniesBeforeWorkerConstruction(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	verifier := &recordingRecoveryVerifier{err: errors.New("receipt no longer current")}
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
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Recover(lifecycle denied) error = %v, want ErrRecoveryAuthorityLost", err)
	}
	if verifier.calls != 1 || created != 0 || leases.reserveCount() != 0 || group.WorkerCount() != 0 {
		t.Fatalf("denied recovery verified/created/reserved/retained=%d/%d/%d/%d, want 1/0/0/0", verifier.calls, created, leases.reserveCount(), group.WorkerCount())
	}
}

func TestGroupSupervisorRunFencesRecoveredWorkerForLifecycleInvalidation(t *testing.T) {
	for _, name := range []string{"revoked", "terminal", "quarantined"} {
		t.Run(name, func(t *testing.T) {
			leases := &recordingWorkspaceLeaseReserver{}
			verifier := &recordingRecoveryVerifier{}
			worker := &fakeGroupWorker{}
			group, err := NewGroupSupervisor(GroupSupervisorConfig{
				MaxWorkers: 1,
				Leases:     leases,
				NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
			})
			if err != nil {
				t.Fatalf("NewGroupSupervisor() error = %v", err)
			}
			if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)); err != nil {
				t.Fatalf("Recover() error = %v", err)
			}
			verifier.err = errors.New(name)
			if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrRecoveryAuthorityLost) {
				t.Fatalf("Run(%s) error = %v, want ErrRecoveryAuthorityLost", name, err)
			}
			verifier.err = nil
			if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrSessionWorkerNotFound) {
				t.Fatalf("Run(%s after fence) error = %v, want ErrSessionWorkerNotFound", name, err)
			}
			if worker.runs != 0 || group.WorkerCount() != 0 || leases.reserveCount() != 0 {
				t.Fatalf("%s invalidation remained runnable/reserved: runs=%d workers=%d reserves=%d", name, worker.runs, group.WorkerCount(), leases.reserveCount())
			}
		})
	}
}

func TestGroupSupervisorAtomicLifecycleFencePreventsProviderStartAfterRevocation(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	verifier := &recordingRecoveryVerifier{revokeBeforeRun: true}
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 1,
		Leases:     leases,
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Run(revoked before atomic start) error = %v, want ErrRecoveryAuthorityLost", err)
	}
	if worker.runs != 0 || group.WorkerCount() != 0 {
		t.Fatalf("atomic lifecycle fence started or retained Provider: runs=%d workers=%d", worker.runs, group.WorkerCount())
	}
}

func TestGroupSupervisorRecoveryDeniesSecondWorkerWithoutActivation(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	verifier := &recordingRecoveryVerifier{}
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
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)); err != nil {
		t.Fatalf("Recover(first) error = %v", err)
	}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_2", "ses_2", 2, verifier)); !errors.Is(err, ErrMultiWorkerDisabled) {
		t.Fatalf("Recover(second) error = %v, want ErrMultiWorkerDisabled", err)
	}
	if group.WorkerCount() != 1 || verifier.calls != 1 || created != 1 || leases.reserveCount() != 0 {
		t.Fatalf("second recovery retained/verified/created/reserved=%d/%d/%d/%d, want 1/1/1/0", group.WorkerCount(), verifier.calls, created, leases.reserveCount())
	}
}

func TestGroupSupervisorRejectsInvalidRecoveryTupleBeforeVerifier(t *testing.T) {
	for _, mutate := range []func(*GroupWorkerRecovery){
		func(recovery *GroupWorkerRecovery) { recovery.Authority.receipt.ConnectionEpoch++ },
		func(recovery *GroupWorkerRecovery) { recovery.Authority.receipt.CredentialGeneration++ },
		func(recovery *GroupWorkerRecovery) { recovery.Authority.receipt.WriterLeaseID = "other" },
		func(recovery *GroupWorkerRecovery) { recovery.Authority.receipt.AcceptedFence = 0 },
		func(recovery *GroupWorkerRecovery) {
			recovery.Authority.receipt.ExpiresAt = time.Now().Add(-time.Second)
		},
		func(recovery *GroupWorkerRecovery) { recovery.Authority = RecoveryAuthority{} },
	} {
		leases := &recordingWorkspaceLeaseReserver{}
		verifier := &recordingRecoveryVerifier{}
		created := 0
		group, err := NewGroupSupervisor(GroupSupervisorConfig{
			MaxWorkers: 1, Leases: leases,
			NewWorker: func(SessionWorkerConfig) (SessionWorkerRunner, error) {
				created++
				return &fakeGroupWorker{}, nil
			},
		})
		if err != nil {
			t.Fatalf("NewGroupSupervisor() error = %v", err)
		}
		recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)
		mutate(&recovery)
		if err := group.Recover(context.Background(), recovery); !errors.Is(err, ErrRecoveryAuthorityLost) {
			t.Fatalf("Recover(invalid tuple) error = %v, want ErrRecoveryAuthorityLost", err)
		}
		if verifier.calls != 0 || created != 0 || leases.reserveCount() != 0 || group.WorkerCount() != 0 {
			t.Fatalf("invalid recovery verified/created/reserved/retained=%d/%d/%d/%d, want 0/0/0/0", verifier.calls, created, leases.reserveCount(), group.WorkerCount())
		}
	}
}

func TestGroupSupervisorRecoveryDependenciesRunOutsideSupervisorLock(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	var group *GroupSupervisor
	verifier := callbackRecoveryLifecycle{verify: func(context.Context, store.ConnectionAuthorityReceipt) error {
		if !supervisorLockAvailable(group) {
			return errors.New("recovery lifecycle ran under supervisor lock")
		}
		return nil
	}}
	created := 0
	var err error
	group, err = NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		NewWorker: func(SessionWorkerConfig) (SessionWorkerRunner, error) {
			created++
			if !supervisorLockAvailable(group) {
				return nil, errors.New("worker factory ran under supervisor lock")
			}
			return &fakeGroupWorker{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1, verifier)); err != nil {
		t.Fatalf("Recover(first) error = %v", err)
	}
	group.activation = callbackMultiWorkerGate{allow: func(context.Context) error {
		if !supervisorLockAvailable(group) {
			return errors.New("activation ran under supervisor lock")
		}
		return nil
	}}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_2", "ses_2", 2, verifier)); err != nil {
		t.Fatalf("Recover(second) error = %v", err)
	}
	if group.WorkerCount() != 2 || created != 2 || leases.reserveCount() != 0 {
		t.Fatalf("recovery dependencies retained/reserved wrong state: workers=%d created=%d reserves=%d", group.WorkerCount(), created, leases.reserveCount())
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

type callbackMultiWorkerGate struct {
	allow func(context.Context) error
}

func (g callbackMultiWorkerGate) AllowMultiWorker(ctx context.Context) error {
	return g.allow(ctx)
}

type recordingRecoveryVerifier struct {
	calls           int
	err             error
	revokeBeforeRun bool
}

func (v *recordingRecoveryVerifier) VerifyConnectionAuthority(_ context.Context, _ store.ConnectionAuthorityReceipt) error {
	v.calls++
	return v.err
}

func (v *recordingRecoveryVerifier) RunWithConnectionAuthority(ctx context.Context, _ store.ConnectionAuthorityReceipt, run func(context.Context) error) error {
	v.calls++
	if v.err != nil {
		return v.err
	}
	if v.revokeBeforeRun {
		return errors.New("connection authority revoked before Provider start")
	}
	return run(ctx)
}

type callbackRecoveryLifecycle struct {
	verify func(context.Context, store.ConnectionAuthorityReceipt) error
}

func (v callbackRecoveryLifecycle) VerifyConnectionAuthority(ctx context.Context, receipt store.ConnectionAuthorityReceipt) error {
	return v.verify(ctx, receipt)
}

func (v callbackRecoveryLifecycle) RunWithConnectionAuthority(ctx context.Context, receipt store.ConnectionAuthorityReceipt, run func(context.Context) error) error {
	if err := v.verify(ctx, receipt); err != nil {
		return err
	}
	return run(ctx)
}

func supervisorLockAvailable(group *GroupSupervisor) bool {
	if group == nil {
		return false
	}
	done := make(chan struct{})
	go func() {
		_ = group.WorkerCount()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(200 * time.Millisecond):
		return false
	}
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

func validGroupWorkerRecovery(workerID, sessionID string, keyByte byte, lifecycle ConnectionAuthorityLifecycle) GroupWorkerRecovery {
	admission := validGroupWorkerAdmission(workerID, sessionID, keyByte)
	authority, err := NewRecoveryAuthority(store.ConnectionAuthorityReceipt{
		SessionID:            sessionID,
		ConnectionEpoch:      admission.Lease.Owner.ConnectionEpoch,
		CredentialGeneration: admission.Lease.Owner.CredentialGeneration,
		AcceptedFence:        1,
		WriterLeaseID:        admission.Lease.Owner.LeaseID,
		ExpiresAt:            admission.Lease.ExpiresAt,
	}, lifecycle)
	if err != nil {
		panic(err)
	}
	return GroupWorkerRecovery{
		Admission: admission,
		Authority: authority,
	}
}
