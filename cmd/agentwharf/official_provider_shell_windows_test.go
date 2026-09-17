//go:build windows

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	ptylib "github.com/aymanbagabas/go-pty"
)

func TestApplyOfficialProviderShellShimWrapsBatchCLI(t *testing.T) {
	batch := filepath.Join(t.TempDir(), "Claude Code", "claude.cmd")
	if err := os.MkdirAll(filepath.Dir(batch), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(batch, []byte("@echo off\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := &ptylib.Cmd{Path: batch, Args: []string{batch, "--model", "Claude Sonnet"}}
	if err := applyOfficialProviderShellShim(cmd); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(filepath.Base(cmd.Path), "cmd.exe") {
		t.Fatalf("path = %q, want cmd.exe", cmd.Path)
	}
	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	want := `/d /s /c ""` + batch + `" "--model" "Claude Sonnet""`
	if cmd.SysProcAttr.CmdLine != want {
		t.Fatalf("CmdLine = %q, want %q", cmd.SysProcAttr.CmdLine, want)
	}
}

func TestApplyOfficialProviderShellShimLeavesNativeCLI(t *testing.T) {
	native := filepath.Join(t.TempDir(), "codex.exe")
	if err := os.WriteFile(native, []byte("not executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := &ptylib.Cmd{Path: native, Args: []string{native}}
	if err := applyOfficialProviderShellShim(cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.Path != native || cmd.SysProcAttr != nil {
		t.Fatalf("native command changed: path=%q attr=%+v", cmd.Path, cmd.SysProcAttr)
	}
}
