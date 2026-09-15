//go:build windows

package core

import "testing"

// The provider child runs an npm .cmd shim, which needs cmd.exe's own variables
// and the per-user profile paths. Windows names are case-insensitive, so a
// parent spelling of `Path` must behave exactly like `PATH`.
func TestPlatformInheritedProviderEnvNameCoversWindowsShimVariables(t *testing.T) {
	for _, name := range []string{"Path", "PATHEXT", "SystemRoot", "windir", "ComSpec", "SystemDrive", "USERPROFILE", "LOCALAPPDATA", "APPDATA", "TEMP", "TMP", "ProgramData", "PROGRAMFILES(X86)"} {
		if !platformInheritedProviderEnvName(name) {
			t.Fatalf("platformInheritedProviderEnvName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"SESSIONNAME", "LOGONSERVER", "SOME_TOKEN", "ELSEWHERE"} {
		if platformInheritedProviderEnvName(name) {
			t.Fatalf("platformInheritedProviderEnvName(%q) = true, want false", name)
		}
	}
	if canonicalProviderEnvName("Path") != canonicalProviderEnvName("PATH") {
		t.Fatal("Windows environment names are not folded for duplicate detection")
	}
}
