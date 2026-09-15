//go:build windows

package core

import "strings"

// platformInheritedProviderEnvName allows the Windows variables a provider child
// needs to run an npm shim: cmd.exe resolves `node` through PATHEXT and
// SystemRoot, and Node/Claude Code read their per-user profile paths. None of
// these carry credentials, and names matching TOKEN/API_KEY/SECRET/... are still
// refused by safeProviderEnvName before they reach the child.
//
// Windows environment names are case-insensitive, so this must compare folded:
// a parent that spells the variable `Path` must still reach the child.
func platformInheritedProviderEnvName(name string) bool {
	switch strings.ToUpper(name) {
	case "PATH", "HOME", "LANG", "TERM", "TMPDIR", "TZ", "USER", "LOGNAME", "SHELL", "PWD",
		"PATHEXT", "SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "COMSPEC", "OS",
		"USERPROFILE", "HOMEDRIVE", "HOMEPATH", "APPDATA", "LOCALAPPDATA", "USERDOMAIN", "USERNAME", "COMPUTERNAME",
		"TEMP", "TMP", "PROGRAMDATA", "ALLUSERSPROFILE", "PUBLIC",
		"PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432", "COMMONPROGRAMFILES", "COMMONPROGRAMFILES(X86)",
		"NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE", "PROCESSOR_IDENTIFIER", "PROCESSOR_LEVEL", "PROCESSOR_REVISION":
		return true
	default:
		return false
	}
}

// canonicalProviderEnvName folds the names used for duplicate detection so a
// parent block carrying both `Path` and `PATH` reaches the child once.
func canonicalProviderEnvName(name string) string { return strings.ToUpper(name) }
