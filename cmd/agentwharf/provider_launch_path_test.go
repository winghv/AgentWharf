package main

import "testing"

// Windows has no PTY backend, so every provider must take the headless ACP
// bridge path there instead of the official terminal CLI path.
func TestProviderBridgeRequiredForPlatform(t *testing.T) {
	for _, tc := range []struct {
		provider string
		goos     string
		want     bool
	}{
		{provider: "claude-code", goos: "darwin", want: false},
		{provider: "claude-code", goos: "linux", want: false},
		{provider: "claude-code", goos: "windows", want: true},
		{provider: "codex", goos: "darwin", want: false},
		{provider: "codex", goos: "windows", want: true},
		{provider: "deepseek-harness", goos: "darwin", want: true},
		{provider: "pi", goos: "linux", want: true},
	} {
		if got := providerBridgeRequiredForPlatform(tc.provider, tc.goos); got != tc.want {
			t.Fatalf("providerBridgeRequiredForPlatform(%q, %q) = %v, want %v", tc.provider, tc.goos, got, tc.want)
		}
	}
}
