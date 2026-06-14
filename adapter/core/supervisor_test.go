package core

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"
)

func TestProcessSupervisorRestartsCrashedProviderUpToLimit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	supervisor, err := NewProcessSupervisor(ProcessConfig{
		Command:     helperCommand("crash"),
		MaxRestarts: 2,
		Backoff:     time.Millisecond,
		GracePeriod: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewProcessSupervisor() error = %v", err)
	}

	err = supervisor.Run(ctx)
	if !errors.Is(err, ErrRestartLimitExceeded) {
		t.Fatalf("Run() error = %v, want ErrRestartLimitExceeded", err)
	}
	started := collectEvents(supervisor.Events(), ProcessEventStarted)
	if len(started) != 3 {
		t.Fatalf("started events = %d, want 3", len(started))
	}
	if started[0].Attempt != 1 || started[1].Attempt != 2 || started[2].Attempt != 3 {
		t.Fatalf("started attempts = %+v", started)
	}
}

func TestProcessSupervisorRestartsKilledProviderChild(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	supervisor, err := NewProcessSupervisor(ProcessConfig{
		Command:     helperCommand("wait"),
		MaxRestarts: 1,
		Backoff:     time.Millisecond,
		GracePeriod: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewProcessSupervisor() error = %v", err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- supervisor.Run(ctx)
	}()
	first := waitEvent(t, supervisor.Events(), ProcessEventStarted)
	if first.PID <= 0 {
		t.Fatalf("first start event = %+v", first)
	}
	process, err := os.FindProcess(first.PID)
	if err != nil {
		t.Fatalf("find helper process: %v", err)
	}
	if err := process.Kill(); err != nil {
		t.Fatalf("kill helper process: %v", err)
	}
	second := waitEvent(t, supervisor.Events(), ProcessEventStarted)
	if second.PID <= 0 || second.PID == first.PID || second.Attempt != 2 {
		t.Fatalf("second start event = %+v after %+v", second, first)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := supervisor.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("Run() error after stop = %v", err)
	}
}

func TestProcessSupervisorStopsProviderWithInterrupt(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	supervisor, err := NewProcessSupervisor(ProcessConfig{
		Command:     helperCommand("wait"),
		MaxRestarts: 1,
		Backoff:     time.Millisecond,
		GracePeriod: time.Second,
	})
	if err != nil {
		t.Fatalf("NewProcessSupervisor() error = %v", err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- supervisor.Run(ctx)
	}()
	_ = waitEvent(t, supervisor.Events(), ProcessEventStarted)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := supervisor.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("Run() error after graceful stop = %v", err)
	}
}

func TestProcessSupervisorKillsProviderAfterGracePeriod(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runner := newFakeProcessRunner()
	supervisor, err := newProcessSupervisor(ProcessConfig{
		Command:     ProcessCommand{Path: "provider"},
		MaxRestarts: 1,
		Backoff:     time.Millisecond,
		GracePeriod: 20 * time.Millisecond,
	}, runner)
	if err != nil {
		t.Fatalf("NewProcessSupervisor() error = %v", err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- supervisor.Run(ctx)
	}()
	_ = waitEvent(t, supervisor.Events(), ProcessEventStarted)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := supervisor.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopped := waitEvent(t, supervisor.Events(), ProcessEventStopped)
	if !stopped.Killed {
		t.Fatalf("stopped event = %+v, want killed", stopped)
	}
	handle := runner.handle(0)
	interrupts, kills := handle.counts()
	if interrupts != 1 || kills != 1 {
		t.Fatalf("process signals = interrupt:%d kill:%d, want 1/1", interrupts, kills)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("Run() error after kill stop = %v", err)
	}
}

func TestProcessSupervisorRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	if _, err := NewProcessSupervisor(ProcessConfig{}); !errors.Is(err, ErrInvalidProcessConfig) {
		t.Fatalf("NewProcessSupervisor(empty) error = %v, want ErrInvalidProcessConfig", err)
	}
}

func helperCommand(mode string) ProcessCommand {
	return ProcessCommand{
		Path: os.Args[0],
		Args: []string{"-test.run=TestProcessSupervisorHelperProcess"},
		Env:  []string{"AGENTWHARF_HELPER_PROCESS=" + mode},
	}
}

func waitEvent(t *testing.T, events <-chan ProcessEvent, eventType ProcessEventType) ProcessEvent {
	t.Helper()

	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Type == eventType {
				return ev
			}
		case <-timeout:
			t.Fatalf("timed out waiting for process event %s", eventType)
		}
	}
}

func collectEvents(events <-chan ProcessEvent, eventType ProcessEventType) []ProcessEvent {
	var out []ProcessEvent
	for {
		select {
		case ev := <-events:
			if ev.Type == eventType {
				out = append(out, ev)
			}
		default:
			return out
		}
	}
}

type fakeProcessRunner struct {
	mu      sync.Mutex
	nextPID int
	started []*fakeProcessHandle
}

type fakeProcessHandle struct {
	pid  int
	done chan struct{}
	once sync.Once

	mu         sync.Mutex
	err        error
	interrupts int
	kills      int
}

func newFakeProcessRunner() *fakeProcessRunner {
	return &fakeProcessRunner{nextPID: 1000}
}

func (r *fakeProcessRunner) Start(ProcessCommand) (processHandle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextPID++
	handle := &fakeProcessHandle{
		pid:  r.nextPID,
		done: make(chan struct{}),
	}
	r.started = append(r.started, handle)
	return handle, nil
}

func (r *fakeProcessRunner) handle(index int) *fakeProcessHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started[index]
}

func (h *fakeProcessHandle) PID() int {
	return h.pid
}

func (h *fakeProcessHandle) Wait() error {
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *fakeProcessHandle) Interrupt() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.interrupts++
	return nil
}

func (h *fakeProcessHandle) Kill() error {
	h.mu.Lock()
	h.kills++
	h.err = errors.New("killed by test")
	h.mu.Unlock()

	h.once.Do(func() {
		close(h.done)
	})
	return nil
}

func (h *fakeProcessHandle) counts() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.interrupts, h.kills
}

func TestMain(m *testing.M) {
	mode := os.Getenv("AGENTWHARF_HELPER_PROCESS")
	if mode == "" {
		os.Exit(m.Run())
	}
	runHelperProcess(mode)
}

func runHelperProcess(mode string) {
	switch mode {
	case "crash":
		os.Exit(2)
	case "wait":
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		<-signals
		os.Exit(0)
	case "ignore":
		signal.Reset(os.Interrupt)
		signal.Ignore(os.Interrupt)
		select {}
	default:
		os.Exit(3)
	}
}
