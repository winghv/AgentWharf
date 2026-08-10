//go:build unix

package core

import (
	"errors"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// Kill is the escalation path the supervisor takes when Interrupt does not stop
// a Provider child, so it is the last thing standing between a stuck Provider
// and a leaked process. It had no test. It is exercised here against a real
// process rather than a fake handle, because the property under test is that the
// OS process actually dies -- a mock would only prove that a method was called.
func TestExecProcessHandleKillTerminatesARunningChild(t *testing.T) {
	t.Parallel()

	// A shell that sleeps far longer than the test: if Kill does not work, the
	// Wait below blocks and the test fails by timeout rather than passing
	// silently.
	handle, err := execProcessRunner{}.Start(ProcessCommand{
		Path: shellPathForKillTest(t),
		Args: []string{"-c", "sleep 300"},
	})
	if err != nil {
		t.Fatalf("Start: unexpected error: %v", err)
	}

	pid := handle.PID()
	if pid <= 0 {
		t.Fatalf("PID() = %d, want a positive pid for a started process", pid)
	}

	if err := handle.Kill(); err != nil {
		t.Fatalf("Kill: unexpected error: %v", err)
	}

	// Wait must return, and it must report the abnormal termination rather than
	// a nil error -- a caller that saw nil would record a clean Provider exit
	// for a process it had just killed.
	waitErr := make(chan error, 1)
	go func() { waitErr <- handle.Wait() }()

	select {
	case err := <-waitErr:
		if err == nil {
			t.Fatal("Wait returned nil after Kill; a killed child must not be reported as a clean exit")
		}
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("Wait error = %v (%T), want *exec.ExitError carrying the signal", err, err)
		}
		if exitErr.Success() {
			t.Fatalf("exit state reports success after Kill: %v", exitErr)
		}

		// Pin the signal, not merely "died abnormally". Kill exists for a child
		// that ignored Interrupt, so a downgrade to any catchable signal is the
		// exact regression this test has to catch -- and an earlier version of
		// it did not: replacing SIGKILL with SIGINT left the test passing,
		// because a `sleep` that takes the default SIGINT disposition still
		// terminates by signal.
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok {
			t.Fatalf("exit status = %T, want syscall.WaitStatus so the terminating signal can be read", exitErr.Sys())
		}
		if !status.Signaled() {
			t.Fatalf("child exited with code %d rather than being signalled; Kill must terminate the child, not wait for it to leave on its own", status.ExitStatus())
		}
		if sig := status.Signal(); sig != syscall.SIGKILL {
			t.Fatalf("child was terminated by %v, want SIGKILL; a catchable signal can be ignored by exactly the stuck child Kill is the escalation path for", sig)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Wait did not return within 30s after Kill; the child was not terminated")
	}

	// Confirm against the OS rather than trusting Wait alone. Signal 0 runs the
	// existence and permission checks without delivering anything, so ESRCH here
	// is independent evidence that the pid is gone.
	//
	// This must be syscall.Kill and not os.Process.Signal(os.Signal(nil)): the
	// latter type-asserts its argument to syscall.Signal and rejects nil before
	// issuing any syscall, so it returns "unsupported signal type" for live and
	// dead pids alike. An earlier version of this test asserted on that call and
	// therefore could never have failed; an independent reviewer measured the
	// difference against both a live and a reaped pid rather than reading past
	// it.
	//
	// Reaped-then-recycled would defeat this, but the pid was reaped microseconds
	// earlier by the Wait above, so recycling it that fast is not a case worth
	// defending against here.
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatalf("pid %d still exists after Kill and Wait; the process was not reaped", pid)
	} else if !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("probing pid %d after Kill returned %v, want ESRCH; anything else means the probe did not establish that the process is gone", pid, err)
	}
}
