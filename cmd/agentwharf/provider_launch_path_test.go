package main

import "testing"

// Claude and Codex have official terminal CLIs on every supported platform;
// Windows uses ConPTY. Providers without one remain on the ACP bridge.
func TestProviderBridgeRequired(t *testing.T) {
	for _, tc := range []struct {
		provider string
		want     bool
	}{
		{provider: "claude-code", want: false},
		{provider: "codex", want: false},
		{provider: "deepseek-harness", want: true},
		{provider: "pi", want: true},
	} {
		if got := providerBridgeRequired(tc.provider); got != tc.want {
			t.Fatalf("providerBridgeRequired(%q) = %v, want %v", tc.provider, got, tc.want)
		}
	}
}
