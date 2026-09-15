//go:build !windows

package core

import "testing"

// POSIX environments keep their exact allowlist: no Windows-only name may leak
// in, and names stay case-sensitive.
func TestPlatformInheritedProviderEnvNameIsEmptyOffWindows(t *testing.T) {
	for _, name := range []string{"PATHEXT", "SystemRoot", "ComSpec", "LOCALAPPDATA"} {
		if platformInheritedProviderEnvName(name) {
			t.Fatalf("platformInheritedProviderEnvName(%q) = true off Windows", name)
		}
	}
	if canonicalProviderEnvName("PATH") != "PATH" {
		t.Fatal("POSIX environment names must not be folded")
	}
	if canonicalProviderEnvName("path") == canonicalProviderEnvName("PATH") {
		t.Fatal("POSIX environment names are case-sensitive")
	}
}
