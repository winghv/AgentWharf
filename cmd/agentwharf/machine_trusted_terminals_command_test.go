package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func trustedTerminalsCredentialFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", path)
	return path
}

func TestTrustedTerminalsCommandReportsPlatformSetting(t *testing.T) {
	t.Setenv("AGENTWHARF_TRUST_ACCOUNT_TERMINALS", "")
	trustedTerminalsCredentialFile(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/machines/machine_1/trusted-terminals/endpoint" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"enabled":true}}`))
	}))
	defer server.Close()
	if err := saveMachineCredential(machineCredential{MachineID: "machine_1", MachineToken: "machine-token", CloudAPIURL: server.URL}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	var out bytes.Buffer
	if err := runTrustedTerminalsCommand(context.Background(), nil, &out); err != nil {
		t.Fatalf("runTrustedTerminalsCommand() error = %v", err)
	}
	if got := out.String(); !strings.Contains(got, "trusted_account_terminals: true") || !strings.Contains(got, "platform_setting: true") {
		t.Fatalf("output = %q", got)
	}
}

func TestTrustedTerminalsCommandReportsLocalOverride(t *testing.T) {
	t.Setenv("AGENTWHARF_TRUST_ACCOUNT_TERMINALS", "1")
	trustedTerminalsCredentialFile(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"enabled":false}}`))
	}))
	defer server.Close()
	if err := saveMachineCredential(machineCredential{MachineID: "machine_1", MachineToken: "machine-token", CloudAPIURL: server.URL}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	var out bytes.Buffer
	if err := runTrustedTerminalsCommand(context.Background(), nil, &out); err != nil {
		t.Fatalf("runTrustedTerminalsCommand() error = %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "trusted_account_terminals: true") || !strings.Contains(got, "platform_setting: false") || !strings.Contains(got, "local_override: AGENTWHARF_TRUST_ACCOUNT_TERMINALS=1") {
		t.Fatalf("output = %q", got)
	}
}

func TestTrustedTerminalsCommandRejectsLocalWrite(t *testing.T) {
	trustedTerminalsCredentialFile(t)
	if err := runTrustedTerminalsCommand(context.Background(), []string{"on"}, io.Discard); err == nil || !strings.Contains(err.Error(), "owner-controlled") {
		t.Fatalf("on error = %v", err)
	}
	if err := runTrustedTerminalsCommand(context.Background(), []string{"off"}, io.Discard); err == nil || !strings.Contains(err.Error(), "owner-controlled") {
		t.Fatalf("off error = %v", err)
	}
}

func TestTrustedTerminalsCommandRequiresPairing(t *testing.T) {
	trustedTerminalsCredentialFile(t)
	if err := runTrustedTerminalsCommand(context.Background(), nil, io.Discard); err == nil || !strings.Contains(err.Error(), "no local machine pairing") {
		t.Fatalf("unpaired error = %v", err)
	}
}
