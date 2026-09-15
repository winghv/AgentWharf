//go:build windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"unsafe"
)

const (
	detachedProcess       = 0x00000008
	createNewProcessGroup = 0x00000200
	processQueryLimited   = 0x1000
	stillActive           = 259
)

// detachBackgroundCommand launches the child without inheriting the console so
// wharf serve survives closing the terminal window.
func detachBackgroundCommand(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: detachedProcess | createNewProcessGroup,
		HideWindow:    true,
	}
	return nil
}

var (
	kernel32DLL     = syscall.NewLazyDLL("kernel32.dll")
	procOpenProcess = kernel32DLL.NewProc("OpenProcess")
	procGetExitCode = kernel32DLL.NewProc("GetExitCodeProcess")
	procCloseHandle = kernel32DLL.NewProc("CloseHandle")
	procLockFileEx  = kernel32DLL.NewProc("LockFileEx")
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
)

// serveLockOverlapped backs every LockFileEx call. Windows requires a valid
// OVERLAPPED (a NULL pointer faults inside kernel32), and the structure must stay
// reachable while the byte-range lock is held, so it lives for the process.
var serveLockOverlapped syscall.Overlapped

// processAlive reports whether a Windows process is still running via
// OpenProcess + GetExitCodeProcess, which is the reliable liveness probe.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, _, _ := procOpenProcess.Call(processQueryLimited, 0, uintptr(pid))
	if handle == 0 {
		return false
	}
	defer procCloseHandle.Call(handle)
	var exitCode uint32
	ret, _, _ := procGetExitCode.Call(handle, uintptr(unsafe.Pointer(&exitCode)))
	if ret == 0 {
		return false
	}
	return exitCode == stillActive
}

func terminateProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return process.Kill()
}

// lockFileExclusive takes a non-blocking exclusive byte-range lock on the
// file. The lock is released when the handle is closed or the process exits.
// LockFileEx requires an OVERLAPPED describing the locked range and cannot take
// us a NULL pointer, so the call passes serveLockOverlapped.
func lockFileExclusive(file *os.File) error {
	ret, _, callErr := procLockFileEx.Call(
		file.Fd(),
		lockfileFailImmediately|lockfileExclusiveLock,
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&serveLockOverlapped)),
	)
	if ret == 0 {
		if callErr == nil || callErr == syscall.Errno(0) {
			return errors.New("lock wharf serve daemon file")
		}
		return callErr
	}
	return nil
}
