//go:build !windows

package core

import (
	"os"
	"os/exec"
)

// bindProviderProcessTree has no extra tree-kill machinery on unix: the entry
// process is the provider itself, and Process.Kill delivers SIGKILL directly.
// Grandchildren spawned by a provider that daemonizes its own children remain
// outside this handle on unix, exactly as before.
func bindProviderProcessTree(*exec.Cmd) (func() error, error) {
	return nil, nil
}

// interruptProcess delivers the graceful stop signal. On unix os.Interrupt is
// a real signal, so failures are genuine delivery errors and the supervisor
// must not skip its grace period.
func interruptProcess(p *os.Process) error {
	return p.Signal(os.Interrupt)
}
