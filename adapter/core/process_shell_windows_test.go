//go:build windows

package core

import (
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

// The ACP provider bridges are npm shims (.cmd), and CreateProcess cannot start
// a batch image, so a dispatched Session on Windows must be launched through the
// command interpreter. A plain executable must be left alone.
func TestApplyProviderShellShimWrapsBatchProvidersOnly(t *testing.T) {
	batch := exec.Command(`C:\Users\First Last\.agentwharf\bin\claude-agent-acp.cmd`)
	if !applyProviderShellShim(batch) {
		t.Fatal("a .cmd provider was not routed through the command interpreter")
	}
	if !strings.EqualFold(batch.Path, windowsCommandInterpreter()) {
		t.Fatalf("provider path = %q, want the command interpreter", batch.Path)
	}
	if len(batch.Args) != 1 || !strings.EqualFold(batch.Args[0], windowsCommandInterpreter()) {
		t.Fatalf("provider args = %q, want only the command interpreter", batch.Args)
	}
	if batch.SysProcAttr == nil || batch.SysProcAttr.CmdLine == "" {
		t.Fatal("provider command line is not pinned")
	}
	want := `/d /s /c ""C:\Users\First Last\.agentwharf\bin\claude-agent-acp.cmd""`
	if batch.SysProcAttr.CmdLine != want {
		t.Fatalf("provider command line = %q, want %q", batch.SysProcAttr.CmdLine, want)
	}

	exe := exec.Command(`C:\Program Files\nodejs\node.exe`, "script.js")
	exe.SysProcAttr = &syscall.SysProcAttr{}
	if applyProviderShellShim(exe) {
		t.Fatal("a native executable was routed through the command interpreter")
	}
	if exe.SysProcAttr.CmdLine != "" {
		t.Fatalf("native executable command line was rewritten: %q", exe.SysProcAttr.CmdLine)
	}
}
