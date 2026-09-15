//go:build !windows

package core

import "os/exec"

// applyProviderShellShim is a no-op where the OS executes provider wrappers
// directly. It reports whether the start command was rewritten.
func applyProviderShellShim(*exec.Cmd) bool { return false }
