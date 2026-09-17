//go:build windows

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	ptylib "github.com/aymanbagabas/go-pty"
)

// applyOfficialProviderShellShim routes npm batch shims through the system
// command interpreter. ConPTY still owns the resulting cmd.exe process and its
// child TUI, so terminal rendering and remote prompt injection share one PTY.
func applyOfficialProviderShellShim(cmd *ptylib.Cmd) error {
	resolved, err := exec.LookPath(cmd.Path)
	if err != nil {
		return err
	}
	switch strings.ToLower(filepath.Ext(resolved)) {
	case ".cmd", ".bat":
	default:
		return nil
	}

	line, err := quoteOfficialWindowsBatchLine(append([]string{resolved}, cmd.Args[1:]...))
	if err != nil {
		return err
	}
	shell := officialWindowsCommandInterpreter()
	cmd.Path = shell
	cmd.Args = []string{shell}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CmdLine = `/d /s /c "` + line + `"`
	return nil
}

func officialWindowsCommandInterpreter() string {
	if root := os.Getenv("SystemRoot"); root != "" {
		candidate := filepath.Join(root, "System32", "cmd.exe")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return "cmd.exe"
}
