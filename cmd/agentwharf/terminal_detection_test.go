package main

import (
	"os"
	"testing"
)

// A sandbox runs the adapter with its stdin closed onto /dev/null. /dev/null
// is a character device, so a "is it a character device" check made every cloud
// VM Session take the interactive official-CLI path instead of the ACP
// bridge: Claude Code's first-run onboarding then blocked the VM and the
// health marker never appeared.
func TestNonTerminalStdinDoesNotSelectTheInteractivePath(t *testing.T) {
	t.Parallel()

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	defer devNull.Close()

	if stdinIsTerminal(devNull) {
		t.Fatalf("%s must not count as a terminal", os.DevNull)
	}
}

// The interactive path is for a real user terminal only.
func TestPipeStdinIsNotATerminal(t *testing.T) {
	t.Parallel()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer reader.Close()
	defer writer.Close()

	if stdinIsTerminal(reader) {
		t.Fatal("a pipe must not count as a terminal")
	}
}

// The `wrap` entry point is the adapter path: the platform, the own-machine
// daemon, and the sandbox entry point launch it, and none of them wants an
// interactive TUI. Only `wharf <agent>` in a real terminal may take it.
func TestWrapEntryPointNeverRunsInteractive(t *testing.T) {
	t.Parallel()

	cfg, err := parseWrapConfig([]string{"--provider", "claude-code", "--acp"}, os.Stderr)
	if err != nil {
		t.Fatalf("parseWrapConfig() error = %v", err)
	}
	if !cfg.ForceHeadless {
		t.Fatal("wrap must force the headless path")
	}
}
