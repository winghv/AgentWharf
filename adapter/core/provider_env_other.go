//go:build !windows

package core

// platformInheritedProviderEnvName adds no platform-specific names where the
// POSIX environment already matches the shared allowlist exactly.
func platformInheritedProviderEnvName(string) bool { return false }

// canonicalProviderEnvName leaves POSIX environment names untouched because they
// are case-sensitive.
func canonicalProviderEnvName(name string) string { return name }
