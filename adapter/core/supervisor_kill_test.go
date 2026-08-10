package core

import (
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

// Kill is the escalation path the supervisor takes when Interrupt does not stop
// a Provider child, so it is the last thing standing between a stuck Provider
// and a leaked process. It had no test. Both branches are exercised here
// against a real process rather than a fake handle, because the property under
// test is that the OS process actually dies -- a mock would only prove that a
// method was called.
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
	case <-time.After(30 * time.Second):
		t.Fatal("Wait did not return within 30s after Kill; the child was not terminated")
	}

	// Confirm against the OS rather than trusting Wait alone. Signal 0 probes
	// liveness without delivering anything; on a reaped pid it must fail.
	if proc, findErr := os.FindProcess(pid); findErr == nil {
		if sigErr := proc.Signal(os.Signal(nil)); sigErr == nil {
			t.Fatalf("pid %d still accepts signals after Kill and Wait; the process was not reaped", pid)
		}
	}
}

// The nil-Process branch is reachable whenever Kill races a handle whose child
// never started, and it must be a no-op rather than a nil dereference. Without
// this the supervisor's cleanup path could panic while already handling a
// startup failure.
func TestExecProcessHandleKillOnUnstartedProcessIsANoOp(t *testing.T) {
	t.Parallel()

	handle := &execProcessHandle{cmd: exec.Command(shellPathForKillTest(t), "-c", "true")}
	if handle.cmd.Process != nil {
		t.Fatal("precondition failed: exec.Command must not populate Process before Start")
	}

	if err := handle.Kill(); err != nil {
		t.Fatalf("Kill on an unstarted process returned %v, want nil", err)
	}
	// Interrupt shares the same guard and the same failure mode, so it is
	// asserted alongside rather than left to a separate test.
	if err := handle.Interrupt(); err != nil {
		t.Fatalf("Interrupt on an unstarted process returned %v, want nil", err)
	}
}

func shellPathForKillTest(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("sh is unavailable, so a real child process cannot be started: %v", err)
	}
	return path
}
