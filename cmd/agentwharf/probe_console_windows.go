//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// createNoWindow keeps a console-subsystem child from allocating a console.
// Same flag the interactive provider path uses: a .cmd shim and its Node
// bridge are console apps, so without it they flash a black window.
const probeCreateNoWindow = 0x08000000

// hideProbeProcessWindow runs the settings probe without a console window.
// Without this, refreshing model settings on Windows flashes a black console
// window for every provider, because the probe launches the provider CLI the
// same way the Session path does but without that path's console policy.
func hideProbeProcessWindow(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= probeCreateNoWindow
	cmd.SysProcAttr.HideWindow = true
}
