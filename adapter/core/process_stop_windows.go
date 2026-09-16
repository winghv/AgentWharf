//go:build windows

package core

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

// bindProviderProcessTree puts a just-started Provider child into a
// kill-on-close Job Object so the Kill escalation ends the whole tree
// (cmd.exe shim -> Node ACP bridge -> provider CLI), not just the entry
// process. Without the job, terminating the shim orphans the bridge and the
// provider, which keep running with their session-scoped credentials.
//
// The returned closure is retained for the child's whole lifetime, so the
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE flag also cleans up the tree whenever the
// adapter process that owns the handle exits -- a daemon restart or a terminal
// session teardown can no longer leave providers behind.
func bindProviderProcessTree(cmd *exec.Cmd) (func() error, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create provider job object: %w", err)
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		windows.CloseHandle(job)
		return nil, fmt.Errorf("configure provider job object: %w", err)
	}
	// os.Process does not expose the child handle, so reopen it with exactly
	// the rights job assignment needs.
	child, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		windows.CloseHandle(job)
		return nil, fmt.Errorf("open provider process for job assignment: %w", err)
	}
	defer windows.CloseHandle(child)
	if err := windows.AssignProcessToJobObject(job, child); err != nil {
		windows.CloseHandle(job)
		return nil, fmt.Errorf("assign provider process to job object: %w", err)
	}
	return func() error {
		terminateErr := windows.TerminateJobObject(job, 1)
		closeErr := windows.CloseHandle(job)
		if terminateErr != nil {
			return terminateErr
		}
		return closeErr
	}, nil
}

// interruptProcess reports ErrInterruptUnsupported for a live child: Windows
// deliberately has no os.Interrupt implementation, so Signal(os.Interrupt)
// fails with EWINDOWS for every process that has not already exited. The
// supervisor treats that as "no graceful path exists" and escalates to the
// Job Object kill instead of failing the stop.
func interruptProcess(p *os.Process) error {
	err := p.Signal(os.Interrupt)
	if err == nil || errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrInterruptUnsupported, err)
}
