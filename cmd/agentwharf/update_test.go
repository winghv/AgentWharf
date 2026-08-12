package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/internal/buildinfo"
)

func TestVersionCommand(t *testing.T) {
	oldVersion := buildinfo.Version
	buildinfo.Version = "v1.2.3"
	t.Cleanup(func() { buildinfo.Version = oldVersion })
	var stdout bytes.Buffer
	if err := runWithInput(context.Background(), []string{"version"}, nil, &stdout, os.Stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "wharf v1.2.3\n" {
		t.Fatalf("version output = %q", stdout.String())
	}
}

func TestIsNewerVersion(t *testing.T) {
	tests := []struct {
		current, candidate string
		want               bool
	}{
		{"v0.1.9", "v0.1.10", true},
		{"v0.2.0", "v1.0.0", true},
		{"v1.0.0", "v1.0.0", false},
		{"v1.1.0", "v1.0.9", false},
		{"dev", "v1.0.0", false},
		{"v1.0", "v1.0.1", false},
	}
	for _, test := range tests {
		t.Run(test.current+"_"+test.candidate, func(t *testing.T) {
			if got := isNewerVersion(test.current, test.candidate); got != test.want {
				t.Fatalf("isNewerVersion(%q, %q) = %t, want %t", test.current, test.candidate, got, test.want)
			}
		})
	}
}

func TestUpgradeCheckReportsAvailableVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"tag_name":"v0.2.0"}`)
	}))
	defer server.Close()
	t.Setenv("WHARF_UPDATE_URL", server.URL)
	oldVersion := buildinfo.Version
	buildinfo.Version = "v0.1.9"
	t.Cleanup(func() { buildinfo.Version = oldVersion })
	var stdout bytes.Buffer
	if err := runUpgradeCommand(context.Background(), []string{"--check"}, nil, &stdout, os.Stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "v0.2.0") || !strings.Contains(stdout.String(), "wharf upgrade") {
		t.Fatalf("upgrade check output = %q", stdout.String())
	}
}

func TestUpgradeRunsOfficialInstallerOnlyWhenExplicitlyRequested(t *testing.T) {
	var installerCalled bool
	installer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		installerCalled = true
		fmt.Fprint(w, "#!/bin/sh\nprintf 'upgraded successfully\\n'\n")
	}))
	defer installer.Close()
	release := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"tag_name":"v0.2.0"}`)
	}))
	defer release.Close()
	t.Setenv("WHARF_UPDATE_URL", release.URL)
	t.Setenv("WHARF_INSTALLER_URL", installer.URL)
	oldVersion := buildinfo.Version
	buildinfo.Version = "v0.1.9"
	t.Cleanup(func() { buildinfo.Version = oldVersion })
	var stdout bytes.Buffer
	if err := runUpgradeCommand(context.Background(), nil, nil, &stdout, os.Stderr); err != nil {
		t.Fatal(err)
	}
	if !installerCalled || stdout.String() != "upgraded successfully\n" {
		t.Fatalf("installer called = %t, output = %q", installerCalled, stdout.String())
	}
}

func TestUpdateReminderUsesFreshCacheWithoutNetwork(t *testing.T) {
	oldVersion := buildinfo.Version
	buildinfo.Version = "v0.1.0"
	t.Cleanup(func() { buildinfo.Version = oldVersion })
	cachePath := filepath.Join(t.TempDir(), "update.json")
	t.Setenv("WHARF_UPDATE_CACHE", cachePath)
	t.Setenv("WHARF_UPDATE_URL", "http://127.0.0.1:1/unreachable")
	if err := writeUpdateCache(cachePath, updateCache{CheckedAt: time.Now(), Latest: "v0.2.0"}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	maybePrintUpdateReminder(context.Background(), &output)
	if !strings.Contains(output.String(), "wharf upgrade") {
		t.Fatalf("reminder output = %q", output.String())
	}
}

func TestUpdateReminderFailsOpen(t *testing.T) {
	oldVersion := buildinfo.Version
	buildinfo.Version = "v0.1.0"
	t.Cleanup(func() { buildinfo.Version = oldVersion })
	t.Setenv("WHARF_UPDATE_CACHE", filepath.Join(t.TempDir(), "update.json"))
	t.Setenv("WHARF_UPDATE_URL", "http://127.0.0.1:1/unreachable")
	var output bytes.Buffer
	started := time.Now()
	maybePrintUpdateReminder(context.Background(), &output)
	if output.Len() != 0 {
		t.Fatalf("failed check output = %q", output.String())
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("failed update check delayed startup")
	}
}

func TestUpdateReminderCanBeDisabled(t *testing.T) {
	oldVersion := buildinfo.Version
	buildinfo.Version = "v0.1.0"
	t.Cleanup(func() { buildinfo.Version = oldVersion })
	t.Setenv("WHARF_NO_UPDATE_CHECK", "1")
	t.Setenv("WHARF_UPDATE_URL", "invalid://must-not-be-called")
	var output bytes.Buffer
	maybePrintUpdateReminder(context.Background(), &output)
	if output.Len() != 0 {
		t.Fatalf("disabled reminder output = %q", output.String())
	}
}
