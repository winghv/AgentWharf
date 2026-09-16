package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Stop must never fail merely because the platform cannot deliver a graceful
// interrupt. Windows has no os.Interrupt implementation, so before the fallback
// existed every Console-driven stop of a machine-hosted Session returned
// "interrupt provider process: not supported", the Adapter exited without
// publishing an outcome, the Hub recovered the reservation as outcome_unknown,
// and the Session could never reach the terminal state its archive requires.
// This test pins the contract at the supervisor level: an unsupported interrupt
// escalates to Kill instead of failing, and it must not wait out the grace
// period first, because no graceful shutdown can possibly happen.
func TestProcessSupervisorStopEscalatesToKillWhenInterruptIsUnsupported(t *testing.T) {
	t.Parallel()

	runner := newFakeProcessRunner()
	// A GracePeriod far longer than the test timeout proves the stop does not
	// wait for a graceful exit that can never happen.
	supervisor, err := newProcessSupervisor(ProcessConfig{
		Command:     ProcessCommand{Path: "provider"},
		MaxRestarts: 1,
		Backoff:     time.Millisecond,
		GracePeriod: 30 * time.Second,
	}, runner)
	if err != nil {
		t.Fatalf("newProcessSupervisor() error = %v", err)
	}

	runDone := make(chan error, 1)
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() {
		runDone <- supervisor.Run(runCtx)
	}()
	if event := waitEvent(t, supervisor.Events(), ProcessEventStarted); event.PID <= 0 {
		t.Fatalf("start event = %+v, want a started process", event)
	}
	handle := runner.handle(0)
	handle.mu.Lock()
	handle.interruptErr = fmt.Errorf("%w: not supported", ErrInterruptUnsupported)
	handle.mu.Unlock()

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStop()
	if err := supervisor.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v, want nil: an unsupported interrupt must escalate to Kill, not fail the stop", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("Run() error after stop = %v, want nil", err)
	}

	interrupts, kills := handle.counts()
	if interrupts != 1 {
		t.Fatalf("interrupts = %d, want exactly one graceful attempt before the escalation", interrupts)
	}
	if kills != 1 {
		t.Fatalf("kills = %d, want the unsupported interrupt to escalate to exactly one Kill", kills)
	}
	if event := waitEvent(t, supervisor.Events(), ProcessEventStopped); !event.Killed {
		t.Fatalf("stop event = %+v, want Killed=true", event)
	}
}

// A graceful interrupt that genuinely fails (not "unsupported") must keep
// failing the stop: the escalation is only for platforms with no signal path,
// not a blanket excuse to kill a Provider whose interrupt errored for another
// reason.
func TestProcessSupervisorStopStillFailsOnARealInterruptError(t *testing.T) {
	t.Parallel()

	runner := newFakeProcessRunner()
	supervisor, err := newProcessSupervisor(ProcessConfig{
		Command:     ProcessCommand{Path: "provider"},
		MaxRestarts: 1,
		Backoff:     time.Millisecond,
		GracePeriod: 20 * time.Millisecond,
	}, runner)
	if err != nil {
		t.Fatalf("newProcessSupervisor() error = %v", err)
	}

	runDone := make(chan error, 1)
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() {
		runDone <- supervisor.Run(runCtx)
	}()
	if event := waitEvent(t, supervisor.Events(), ProcessEventStarted); event.PID <= 0 {
		t.Fatalf("start event = %+v, want a started process", event)
	}
	handle := runner.handle(0)
	handle.mu.Lock()
	handle.interruptErr = errors.New("permission denied")
	handle.mu.Unlock()

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStop()
	err = supervisor.Stop(stopCtx)
	if err == nil {
		t.Fatal("Stop() error = nil, want the interrupt delivery error to surface")
	}
	if !strings.Contains(err.Error(), "interrupt provider process") {
		t.Fatalf("Stop() error = %v, want the wrapped interrupt failure", err)
	}
	interrupts, kills := handle.counts()
	if kills != 0 {
		t.Fatalf("kills = %d, want 0: a genuine interrupt error must not escalate to Kill", kills)
	}
	if interrupts != 1 {
		t.Fatalf("interrupts = %d, want 1", interrupts)
	}
	cancelRun()
}
