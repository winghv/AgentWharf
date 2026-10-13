//go:build !windows

package main

import "os/exec"

// hideProbeProcessWindow is a no-op off Windows: only Windows console apps can
// allocate a visible window for a child process.
func hideProbeProcessWindow(cmd *exec.Cmd) {}
