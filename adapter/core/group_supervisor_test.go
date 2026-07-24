package core

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	storesqlite "github.com/winghv/agentwharf/store/sqlite"
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
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1)
	if err := group.Recover(context.Background(), recovery); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if group.WorkerCount() != 1 || leases.reserveCount() != 0 || recovery.Authority.lifecycle.verificationCount() != 1 {
		t.Fatalf("recovery membership/reserve/verify = %d/%d/%d, want 1/0/1", group.WorkerCount(), leases.reserveCount(), recovery.Authority.lifecycle.verificationCount())
	}
	if err := group.Run(context.Background(), "ses_1"); err != nil {
		t.Fatalf("Run(recovered) error = %v", err)
	}
	if worker.runs != 1 || recovery.Authority.lifecycle.verificationCount() != 1 {
		t.Fatalf("recovered run/verify = %d/%d, want 1/1 atomic start", worker.runs, recovery.Authority.lifecycle.verificationCount())
	}
}

func TestGroupSupervisorFailsClosedForRecoveryLossAndQueuedRevocation(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 2,
		Leases:     leases,
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1)
	if err := group.Recover(context.Background(), recovery); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	recovery.Authority.lifecycle.Revoke()
	if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Run(revoked queued recovery) error = %v, want ErrRecoveryAuthorityLost", err)
	}
	if worker.runs != 0 || leases.reserveCount() != 0 {
		t.Fatalf("queued authority loss ran/reserved provider: runs=%d reserves=%d", worker.runs, leases.reserveCount())
	}
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
	missingAuthority := validGroupWorkerRecovery("worker_2", "ses_2", 2)
	missingAuthority.Authority = RecoveryAuthority{}
	if err := missing.Recover(context.Background(), missingAuthority); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Recover(without verifier) error = %v, want ErrRecoveryAuthorityLost", err)
	}
}

func TestGroupSupervisorRejectsRecoveryWhenLifecycleDeniesBeforeWorkerConstruction(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
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
	recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1)
	recovery.Authority.lifecycle.Revoke()
	if err := group.Recover(context.Background(), recovery); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Recover(lifecycle denied) error = %v, want ErrRecoveryAuthorityLost", err)
	}
	if created != 0 || leases.reserveCount() != 0 || group.WorkerCount() != 0 {
		t.Fatalf("denied recovery created/reserved/retained=%d/%d/%d, want 0/0/0", created, leases.reserveCount(), group.WorkerCount())
	}
}

func TestGroupSupervisorRunFencesRecoveredWorkerForLifecycleInvalidation(t *testing.T) {
	for _, name := range []string{"revoked", "terminal", "quarantined"} {
		t.Run(name, func(t *testing.T) {
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
			recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1)
			if err := group.Recover(context.Background(), recovery); err != nil {
				t.Fatalf("Recover() error = %v", err)
			}
			recovery.Authority.lifecycle.Revoke()
			if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrRecoveryAuthorityLost) {
				t.Fatalf("Run(%s) error = %v, want ErrRecoveryAuthorityLost", name, err)
			}
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
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 1,
		Leases:     leases,
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1)
	if err := group.Recover(context.Background(), recovery); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	recovery.Authority.lifecycle.Revoke()
	if err := group.Run(context.Background(), "ses_1"); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Run(revoked before atomic start) error = %v, want ErrRecoveryAuthorityLost", err)
	}
	if worker.runs != 0 || group.WorkerCount() != 0 {
		t.Fatalf("atomic lifecycle fence started or retained Provider: runs=%d workers=%d", worker.runs, group.WorkerCount())
	}
}

func TestGroupSupervisorStoreAuthorityFailsClosedBeforeConstructionAndRun(t *testing.T) {
	t.Run("rejected before construction", func(t *testing.T) {
		leases := &recordingWorkspaceLeaseReserver{}
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
		recovery, authorityStore := validGroupWorkerRecoveryWithStore("worker_store", "ses_store", 1)
		now := time.Now()
		authorityStore.connection.RevokedAt = &now
		if err := group.Recover(context.Background(), recovery); !errors.Is(err, ErrRecoveryAuthorityLost) {
			t.Fatalf("Recover(revoked Store tuple) error = %v, want ErrRecoveryAuthorityLost", err)
		}
		if created != 0 || group.WorkerCount() != 0 {
			t.Fatalf("revoked Store tuple constructed/retained worker: created=%d count=%d", created, group.WorkerCount())
		}
	})

	t.Run("rejected before run", func(t *testing.T) {
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
		recovery, authorityStore := validGroupWorkerRecoveryWithStore("worker_store", "ses_store", 1)
		if err := group.Recover(context.Background(), recovery); err != nil {
			t.Fatalf("Recover() error = %v", err)
		}
		authorityStore.connection.ConnectionEpoch++
		if err := group.Run(context.Background(), "ses_store"); !errors.Is(err, ErrRecoveryAuthorityLost) {
			t.Fatalf("Run(replaced Store tuple) error = %v, want ErrRecoveryAuthorityLost", err)
		}
		if worker.runs != 0 {
			t.Fatalf("provider started with replaced Store tuple: runs=%d", worker.runs)
		}
		if err := group.Run(context.Background(), "ses_store"); !errors.Is(err, ErrSessionWorkerNotFound) {
			t.Fatalf("Run(after Store fence) error = %v, want ErrSessionWorkerNotFound", err)
		}
	})
}

func TestGroupSupervisorStoreLifecycleRejectsSQLiteReplacementBeforeProviderStart(t *testing.T) {
	ctx := context.Background()
	authorityStore, err := storesqlite.Open(ctx, filepath.Join(t.TempDir(), "connection.db"))
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := authorityStore.Close(); err != nil {
			t.Errorf("Store.Close() error = %v", err)
		}
	})

	expiresAt := time.Now().Add(time.Minute)
	if _, err := authorityStore.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_sqlite_recovery", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: expiresAt,
	}); err != nil {
		t.Fatalf("InitializeAdapterConnection() error = %v", err)
	}
	writer := store.SettingsWriter{LeaseID: "lease_sqlite_recovery"}
	connection, err := authorityStore.AcceptAdapterHello(ctx, "ses_sqlite_recovery", store.AdapterHello{CredentialGeneration: 1, WriterLeaseID: writer.LeaseID})
	if err != nil {
		t.Fatalf("AcceptAdapterHello(initial) error = %v", err)
	}
	writer.ConnectionEpoch = connection.ConnectionEpoch
	writer.CredentialGeneration = connection.ActiveCredentialGeneration
	grantFence, err := authorityStore.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("AllocateAdapterGrantFence() error = %v", err)
	}
	receipt, err := authorityStore.IssueAdapterConnectionAuthorityReceipt(ctx, "ses_sqlite_recovery", store.AdapterConnectionAdmission{
		CredentialGeneration: connection.ActiveCredentialGeneration,
		ConnectionEpoch:      connection.ConnectionEpoch,
		AcceptedFence:        connection.AcceptedFence,
		GrantFence:           grantFence,
	}, writer)
	if err != nil {
		t.Fatalf("IssueAdapterConnectionAuthorityReceipt() error = %v", err)
	}
	lifecycle, err := NewConnectionAuthorityLifecycle(receipt, authorityStore)
	if err != nil {
		t.Fatalf("NewConnectionAuthorityLifecycle() error = %v", err)
	}

	admission := validGroupWorkerAdmission("worker_sqlite_recovery", "ses_sqlite_recovery", 1)
	admission.Lease.Owner.ConnectionEpoch = receipt.ConnectionEpoch
	admission.Lease.Owner.CredentialGeneration = receipt.CredentialGeneration
	admission.Lease.Owner.LeaseID = receipt.WriterLeaseID
	admission.Lease.ExpiresAt = receipt.ExpiresAt
	handle, err := NewRecoveryStartHandle("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6")
	if err != nil {
		t.Fatalf("NewRecoveryStartHandle() error = %v", err)
	}
	authority, err := NewRecoveryAuthorityWithStartHandle(receipt, lifecycle, handle)
	if err != nil {
		t.Fatalf("NewRecoveryAuthorityWithStartHandle() error = %v", err)
	}
	worker := &fakeGroupWorker{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 1,
		Leases:     &recordingWorkspaceLeaseReserver{},
		NewWorker:  func(SessionWorkerConfig) (SessionWorkerRunner, error) { return worker, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Recover(ctx, GroupWorkerRecovery{Admission: admission, Authority: authority, StartHandle: handle}); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if _, err := authorityStore.AcceptAdapterHello(ctx, "ses_sqlite_recovery", store.AdapterHello{CredentialGeneration: 1, WriterLeaseID: "lease_sqlite_replaced"}); err != nil {
		t.Fatalf("AcceptAdapterHello(replacement) error = %v", err)
	}
	if err := group.Run(ctx, "ses_sqlite_recovery"); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Run(replaced Store connection) error = %v, want ErrRecoveryAuthorityLost", err)
	}
	if worker.runs != 0 || group.WorkerCount() != 0 {
		t.Fatalf("replaced Store connection started or retained worker: runs=%d workers=%d", worker.runs, group.WorkerCount())
	}
}

func TestConnectionAuthorityLifecycleRevokeWaitsForProviderStartCallback(t *testing.T) {
	recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1)
	lifecycle := recovery.Authority.lifecycle
	started := make(chan struct{})
	release := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- lifecycle.RunWithConnectionAuthority(context.Background(), recovery.Authority.receipt, func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Provider-start callback did not begin")
	}
	revokeDone := make(chan struct{})
	go func() {
		lifecycle.Revoke()
		close(revokeDone)
	}()
	select {
	case <-revokeDone:
		t.Fatal("Revoke completed while Provider-start callback was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("RunWithConnectionAuthority() error = %v", err)
	}
	select {
	case <-revokeDone:
	case <-time.After(time.Second):
		t.Fatal("Revoke did not complete after Provider-start callback")
	}
	if err := lifecycle.VerifyConnectionAuthority(context.Background(), recovery.Authority.receipt); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("VerifyConnectionAuthority() after Revoke error = %v, want ErrRecoveryAuthorityLost", err)
	}
}

func TestGroupSupervisorRecoveryDeniesSecondWorkerWithoutActivation(t *testing.T) {
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
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1)); err != nil {
		t.Fatalf("Recover(first) error = %v", err)
	}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_2", "ses_2", 2)); !errors.Is(err, ErrMultiWorkerDisabled) {
		t.Fatalf("Recover(second) error = %v, want ErrMultiWorkerDisabled", err)
	}
	if group.WorkerCount() != 1 || created != 1 || leases.reserveCount() != 0 {
		t.Fatalf("second recovery retained/created/reserved=%d/%d/%d, want 1/1/0", group.WorkerCount(), created, leases.reserveCount())
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
		recovery := validGroupWorkerRecovery("worker_1", "ses_1", 1)
		mutate(&recovery)
		if err := group.Recover(context.Background(), recovery); !errors.Is(err, ErrRecoveryAuthorityLost) {
			t.Fatalf("Recover(invalid tuple) error = %v, want ErrRecoveryAuthorityLost", err)
		}
		if created != 0 || leases.reserveCount() != 0 || group.WorkerCount() != 0 {
			t.Fatalf("invalid recovery created/reserved/retained=%d/%d/%d, want 0/0/0", created, leases.reserveCount(), group.WorkerCount())
		}
	}
}

func TestGroupSupervisorRecoveryFencesMismatchedOpaqueStartHandle(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers: 1, Leases: leases,
		NewWorker: func(SessionWorkerConfig) (SessionWorkerRunner, error) { return &fakeGroupWorker{}, nil },
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() = %v", err)
	}
	recovery := validGroupWorkerRecovery("worker_handle", "ses_handle", 9)
	handle, err := NewRecoveryStartHandle("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6")
	if err != nil {
		t.Fatalf("NewRecoveryStartHandle() = %v", err)
	}
	authority, err := NewRecoveryAuthorityWithStartHandle(recovery.Authority.receipt, recovery.Authority.lifecycle, handle)
	if err != nil {
		t.Fatalf("NewRecoveryAuthorityWithStartHandle() = %v", err)
	}
	recovery.Authority = authority
	recovery.StartHandle = RecoveryStartHandle{}
	if err := group.Recover(context.Background(), recovery); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Recover() mismatched opaque start handle = %v, want authority loss", err)
	}
	if got := group.WorkerCount(); got != 0 {
		t.Fatalf("rejected recovery worker count = %d, want 0", got)
	}
}

func TestGroupSupervisorRecoveryDependenciesRunOutsideSupervisorLock(t *testing.T) {
	leases := &recordingWorkspaceLeaseReserver{}
	var group *GroupSupervisor
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
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_1", "ses_1", 1)); err != nil {
		t.Fatalf("Recover(first) error = %v", err)
	}
	group.activation = callbackMultiWorkerGate{allow: func(context.Context) error {
		if !supervisorLockAvailable(group) {
			return errors.New("activation ran under supervisor lock")
		}
		return nil
	}}
	if err := group.Recover(context.Background(), validGroupWorkerRecovery("worker_2", "ses_2", 2)); err != nil {
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

func validGroupWorkerRecovery(workerID, sessionID string, keyByte byte) GroupWorkerRecovery {
	recovery, _ := validGroupWorkerRecoveryWithStore(workerID, sessionID, keyByte)
	return recovery
}

func validGroupWorkerRecoveryWithStore(workerID, sessionID string, keyByte byte) (GroupWorkerRecovery, *recordingConnectionAuthorityStore) {
	admission := validGroupWorkerAdmission(workerID, sessionID, keyByte)
	receipt := store.ConnectionAuthorityReceipt{
		SessionID:            sessionID,
		ConnectionEpoch:      admission.Lease.Owner.ConnectionEpoch,
		CredentialGeneration: admission.Lease.Owner.CredentialGeneration,
		AcceptedFence:        1,
		WriterLeaseID:        admission.Lease.Owner.LeaseID,
		ExpiresAt:            admission.Lease.ExpiresAt,
	}
	authorityStore := &recordingConnectionAuthorityStore{connection: store.AdapterConnection{
		SessionID:                  sessionID,
		ConnectionEpoch:            receipt.ConnectionEpoch,
		AcceptedFence:              receipt.AcceptedFence,
		ActiveCredentialGeneration: receipt.CredentialGeneration,
		ActiveCredentialExpiresAt:  receipt.ExpiresAt,
	}}
	lifecycle, err := NewConnectionAuthorityLifecycle(receipt, authorityStore)
	if err != nil {
		panic(err)
	}
	handle, err := NewRecoveryStartHandle("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6")
	if err != nil {
		panic(err)
	}
	authority, err := NewRecoveryAuthorityWithStartHandle(receipt, lifecycle, handle)
	if err != nil {
		panic(err)
	}
	return GroupWorkerRecovery{
		Admission:   admission,
		Authority:   authority,
		StartHandle: handle,
	}, authorityStore
}

type recordingConnectionAuthorityStore struct {
	store.AdapterConnectionStore
	connection store.AdapterConnection
	err        error
}

func (s *recordingConnectionAuthorityStore) AdapterConnection(context.Context, string) (store.AdapterConnection, error) {
	if s.err != nil {
		return store.AdapterConnection{}, s.err
	}
	return s.connection, nil
}
