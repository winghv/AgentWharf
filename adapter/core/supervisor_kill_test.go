package core

import (
	"os/exec"
	"testing"
)

// The real-child half of Kill's coverage lives in supervisor_kill_unix_test.go.
// It asserts which signal terminated the child and probes the pid afterwards,
// both of which need syscall.Kill and syscall.WaitStatus. Those do not exist on
// Windows, and `GOOS=windows go vet ./...` is green today, so keeping them in
// this untagged file would have regressed a working cross-compile invariant to
// buy an assertion that only means anything on unix anyway.
//
// What stays here is the part that is genuinely portable: the nil-Process
// guard, which is reachable on every platform.

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
