//go:build windows

package core

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// createNoWindow keeps a console-subsystem child from allocating a new console.
// The wharf serve daemon runs detached, so without this every provider child
// (the cmd.exe shim and its Node bridge) opens a visible console window, and
// closing that window kills the provider and the whole Session.
const (
	createNoWindow = 0x08000000
	// detachedProcess cannot be combined with CREATE_NO_WINDOW.
	detachedProcess = 0x00000008
)

// applyProviderConsolePolicy runs provider children without a console window.
// CREATE_NO_WINDOW cannot be combined with DETACHED_PROCESS or
// CREATE_NEW_CONSOLE, so only this flag and STARTF_USESHOWWINDOW are set.
func applyProviderConsolePolicy(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= createNoWindow
	cmd.SysProcAttr.HideWindow = true
}

// applyProviderShellShim routes an npm/batch provider wrapper through cmd.exe.
// CreateProcess cannot execute a .cmd or .bat image, and the ACP provider
// bridges are installed as npm shims on Windows, so without this every
// dispatched Session fails to start its provider. This is what Windows shells
// and cross-spawn do for the same reason.
func applyProviderShellShim(cmd *exec.Cmd) bool {
	switch strings.ToLower(filepath.Ext(cmd.Path)) {
	case ".cmd", ".bat":
	default:
		return false
	}
	line := quoteWindowsBatchLine(append([]string{cmd.Path}, cmd.Args[1:]...))
	shell := windowsCommandInterpreter()
	cmd.Path = shell
	cmd.Args = []string{shell}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	// CmdLine is used verbatim, so cmd's documented "/s with an outer quoted
	// command line" rule survives instead of Go re-escaping the inner quotes.
	cmd.SysProcAttr.CmdLine = `/d /s /c "` + line + `"`
	return true
}

// windowsCommandInterpreter prefers the system copy over a PATH lookup, so a
// directory earlier on PATH cannot substitute the shell.
func windowsCommandInterpreter() string {
	if root := os.Getenv("SystemRoot"); root != "" {
		candidate := filepath.Join(root, "System32", "cmd.exe")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return "cmd.exe"
}

// quoteWindowsBatchLine quotes every element. /s makes cmd strip the surrounding
// pair, and the provider re-parses ordinary quoted arguments.
func quoteWindowsBatchLine(elements []string) string {
	quoted := make([]string, 0, len(elements))
	for _, element := range elements {
		quoted = append(quoted, `"`+strings.ReplaceAll(element, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, " ")
}
