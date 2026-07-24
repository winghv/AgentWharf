package core

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type recordingProcessOwnership struct {
	mu         sync.Mutex
	calls      []string
	err        error
	quiesceErr error
}

type blockingProcessOwnership struct {
	entered  chan struct{}
	release  chan struct{}
	quiesced chan struct{}
}

func (o *blockingProcessOwnership) PrepareStart(context.Context, int) error { return nil }
func (o *blockingProcessOwnership) AbortStart(context.Context, int) error   { return nil }
func (o *blockingProcessOwnership) ObserveStarted(context.Context, ProcessEvent) error {
	close(o.entered)
	<-o.release
	return nil
}
func (o *blockingProcessOwnership) Quiesce(context.Context) error { close(o.quiesced); return nil }

func (o *recordingProcessOwnership) PrepareStart(context.Context, int) error {
	o.mu.Lock()
	o.calls = append(o.calls, "owner:prepare")
	err := o.err
	o.mu.Unlock()
	return err
}

func (o *recordingProcessOwnership) AbortStart(context.Context, int) error {
	o.mu.Lock()
	o.calls = append(o.calls, "owner:abort")
	o.mu.Unlock()
	return nil
}

func (o *recordingProcessOwnership) ObserveStarted(context.Context, ProcessEvent) error {
	o.mu.Lock()
	o.calls = append(o.calls, "owner:started")
	o.mu.Unlock()
	return nil
}

func (o *recordingProcessOwnership) Quiesce(context.Context) error {
	o.mu.Lock()
	o.calls = append(o.calls, "owner:quiesce")
	err := o.quiesceErr
	o.mu.Unlock()
	return err
}

func (o *recordingProcessOwnership) Calls() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.calls...)
}

type recordingQuiescenceLease struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (l *recordingQuiescenceLease) Release(context.Context) error {
	l.mu.Lock()
	l.calls = append(l.calls, "lease:release")
	err := l.err
	l.mu.Unlock()
	return err
}

func (l *recordingQuiescenceLease) Quarantine(context.Context) error {
	l.mu.Lock()
	l.calls = append(l.calls, "lease:quarantine")
	l.mu.Unlock()
	return nil
}

func (l *recordingQuiescenceLease) Calls() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

func TestSessionWorkerOwnershipPrecedesDurableStartAndReleasesAfterQuiescence(t *testing.T) {
	t.Parallel()
	owner := &recordingProcessOwnership{}
	lease := &recordingQuiescenceLease{}
	receipts := &testDurableReceiptGate{}
	runner := newFakeProcessRunner()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID: "ses_owner",
		Provider: ProcessConfig{
			Command:     ProcessCommand{Path: "provider"},
			MaxRestarts: 0,
			Backoff:     time.Millisecond,
			GracePeriod: time.Millisecond,
		},
		ProcessOwnership: owner, QuiescenceLease: lease, DurableReceipts: receipts,
	}, runner)
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	owned, ok := worker.provider.cfg.StartAdmission.(*ownedProcessStartAdmission)
	if !ok {
		t.Fatalf("start admission type = %T, want ownership fence", worker.provider.cfg.StartAdmission)
	}
	if _, ok := owned.delegate.(*durableReceiptStartAdmission); !ok {
		t.Fatalf("ownership delegate type = %T, want durable receipt fence", owned.delegate)
	}
	if err := worker.provider.cfg.StartAdmission.PrepareProcessStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareProcessStart() error = %v", err)
	}
	if err := worker.provider.cfg.StartAdmission.ConfirmProcessStarted(context.Background(), 1); err != nil {
		t.Fatalf("ConfirmProcessStarted() error = %v", err)
	}
	if got, want := receipts.calls(), []string{"start:prepare", "start:confirmed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("durable calls = %v, want %v", got, want)
	}
	if got, want := owner.Calls(), []string{"owner:prepare"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ownership calls before start = %v, want %v", got, want)
	}

	// The direct admission calls above leave the fake supervisor with no live
	// process. Abort clears the prepared ownership record before the lifecycle
	// stop path is exercised by the worker itself.
	if err := owner.AbortStart(context.Background(), 1); err != nil {
		t.Fatalf("AbortStart() error = %v", err)
	}
	if got, want := owner.Calls(), []string{"owner:prepare", "owner:abort"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ownership calls after abort = %v, want %v", got, want)
	}
}

func TestOwnedProcessStartAdmissionAbortsDelegateFailure(t *testing.T) {
	t.Parallel()
	owner := &recordingProcessOwnership{}
	delegate := &recordingProcessStartAdmission{rejectAttempt: 1}
	admission := &ownedProcessStartAdmission{owner: owner, delegate: delegate}
	if err := admission.PrepareProcessStart(context.Background(), 1); err == nil {
		t.Fatal("PrepareProcessStart() error = nil, want delegate failure")
	}
	if got, want := owner.Calls(), []string{"owner:prepare", "owner:abort"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ownership calls = %v, want %v", got, want)
	}
}

func TestSessionWorkerRejectsOwnershipBeforeStart(t *testing.T) {
	t.Parallel()
	owner := &recordingProcessOwnership{err: errors.New("ownership prepare failed")}
	lease := &recordingQuiescenceLease{}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:        "ses_quarantine",
		Provider:         ProcessConfig{Command: ProcessCommand{Path: "provider"}},
		ProcessOwnership: owner, QuiescenceLease: lease,
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	if err := worker.provider.cfg.StartAdmission.PrepareProcessStart(context.Background(), 1); err == nil {
		t.Fatal("PrepareProcessStart() error = nil, want ownership failure")
	}
	if got := lease.Calls(); len(got) != 0 {
		t.Fatalf("lease calls after rejected prepare = %v, want none", got)
	}
}

func TestSessionWorkerQuarantinesWhenQuiescenceFails(t *testing.T) {
	t.Parallel()
	owner := &recordingProcessOwnership{quiesceErr: errors.New("quiescence uncertain")}
	lease := &recordingQuiescenceLease{}
	runner := newFakeProcessRunner()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID: "ses_quiesce_failure",
		Provider: ProcessConfig{
			Command:     ProcessCommand{Path: "provider"},
			GracePeriod: time.Millisecond,
		},
		ProcessOwnership: owner, QuiescenceLease: lease,
	}, runner)
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(context.Background()) }()
	_ = waitEvent(t, worker.Events(), ProcessEventStarted)
	if err := worker.Stop(context.Background()); err == nil {
		t.Fatal("Stop() error = nil, want quiescence failure")
	}
	if got, want := lease.Calls(), []string{"lease:quarantine"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lease calls = %v, want %v", got, want)
	}
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() after Stop() error = %v", err)
	}
}

func TestSessionWorkerRunCleansOwnershipAfterProviderExit(t *testing.T) {
	t.Parallel()
	owner := &recordingProcessOwnership{}
	lease := &recordingQuiescenceLease{}
	runner := newFakeProcessRunner()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:        "ses_owner_exit",
		Provider:         ProcessConfig{Command: ProcessCommand{Path: "provider"}},
		ProcessOwnership: owner, QuiescenceLease: lease,
	}, runner)
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(context.Background()) }()
	_ = waitEvent(t, worker.Events(), ProcessEventStarted)
	runner.handle(0).finish(nil)
	if err := <-runDone; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := owner.Calls(); !reflect.DeepEqual(got, []string{"owner:prepare", "owner:started", "owner:quiesce"}) {
		t.Fatalf("ownership calls = %v", got)
	}
	if got := lease.Calls(); !reflect.DeepEqual(got, []string{"lease:release"}) {
		t.Fatalf("lease calls = %v", got)
	}
}

func TestSessionWorkerWaitsForStartedObservationBeforeRelease(t *testing.T) {
	t.Parallel()
	owner := &blockingProcessOwnership{entered: make(chan struct{}), release: make(chan struct{}), quiesced: make(chan struct{})}
	lease := &recordingQuiescenceLease{}
	runner := newFakeProcessRunner()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:        "ses_owner_order",
		Provider:         ProcessConfig{Command: ProcessCommand{Path: "provider"}},
		ProcessOwnership: owner, QuiescenceLease: lease,
	}, runner)
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(context.Background()) }()
	select {
	case <-owner.entered:
	case <-time.After(time.Second):
		t.Fatal("ownership observer did not receive started event")
	}
	runner.handle(0).finish(nil)
	select {
	case err := <-runDone:
		t.Fatalf("Run() returned before started observation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(owner.release)
	if err := <-runDone; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case <-owner.quiesced:
	case <-time.After(time.Second):
		t.Fatal("ownership was not quiesced")
	}
	if got, want := lease.Calls(), []string{"lease:release"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lease calls = %v, want %v", got, want)
	}
}

func TestSessionWorkerReleasesOwnershipOnRepeatedRun(t *testing.T) {
	t.Parallel()
	owner := &recordingProcessOwnership{}
	lease := &recordingQuiescenceLease{}
	runner := newFakeProcessRunner()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:        "ses_owner_repeat",
		Provider:         ProcessConfig{Command: ProcessCommand{Path: "provider"}},
		ProcessOwnership: owner, QuiescenceLease: lease,
	}, runner)
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	for i := 0; i < 2; i++ {
		runDone := make(chan error, 1)
		go func() { runDone <- worker.Run(context.Background()) }()
		_ = waitEvent(t, worker.Events(), ProcessEventStarted)
		runner.handle(i).finish(nil)
		if err := <-runDone; err != nil {
			t.Fatalf("Run(%d) error = %v", i, err)
		}
	}
	if got, want := owner.Calls(), []string{"owner:prepare", "owner:started", "owner:quiesce", "owner:prepare", "owner:started", "owner:quiesce"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ownership calls = %v, want %v", got, want)
	}
	if got, want := lease.Calls(), []string{"lease:release", "lease:release"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lease calls = %v, want %v", got, want)
	}
}

func TestGroupSupervisorPassesOwnershipAndLeaseToWorker(t *testing.T) {
	t.Parallel()
	owner := &recordingProcessOwnership{}
	lease := &recordingQuiescenceLease{}
	leases := &recordingWorkspaceLeaseReserver{}
	var got SessionWorkerConfig
	group, err := NewGroupSupervisor(GroupSupervisorConfig{
		MaxWorkers:       1,
		Leases:           leases,
		ProcessOwnership: owner,
		QuiescenceLease:  lease,
		NewWorker: func(cfg SessionWorkerConfig) (SessionWorkerRunner, error) {
			got = cfg
			return &fakeGroupWorker{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewGroupSupervisor() error = %v", err)
	}
	if err := group.Admit(context.Background(), validGroupWorkerAdmission("worker_owner", "ses_owner", 1)); err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	if got.ProcessOwnership != owner || got.QuiescenceLease != lease {
		t.Fatalf("worker ownership wiring = %p/%p, want %p/%p", got.ProcessOwnership, got.QuiescenceLease, owner, lease)
	}
}
