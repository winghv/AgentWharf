package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMachineLocalAccountBindingPrefersEnvironment(t *testing.T) {
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "env/account")
	if got := machineLocalAccountBinding(machineCredential{LocalAccountBinding: "stored/account"}); got != "env/account" {
		t.Fatalf("binding = %q, want env/account", got)
	}
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "")
	if got := machineLocalAccountBinding(machineCredential{LocalAccountBinding: "stored/account"}); got != "stored/account" {
		t.Fatalf("binding = %q, want stored/account", got)
	}
	if got := machineLocalAccountBinding(machineCredential{}); got != "" {
		t.Fatalf("binding = %q, want empty", got)
	}
}

func TestReuseMachineCredentialAcceptsAcceptedPairing(t *testing.T) {
	var requests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/machines/machine_1/trusted-terminals/endpoint" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"enabled":false,"org_id":"org_1","owner_user_id":"user_1"}}`))
	}))
	defer controlPlane.Close()

	credential := machineCredential{MachineID: "machine_1", MachineToken: "token", CloudAPIURL: controlPlane.URL}
	reused, ok := reuseMachineCredential(context.Background(), controlPlane.Client(), credential)
	if !ok || reused.MachineID != "machine_1" || requests != 1 {
		t.Fatalf("reuse = %+v ok=%t requests=%d", reused, ok, requests)
	}
}

func TestReuseMachineCredentialRejectsRevokedPairing(t *testing.T) {
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, `{"error":"revoked"}`, http.StatusUnauthorized)
	}))
	defer controlPlane.Close()

	credential := machineCredential{MachineID: "machine_1", MachineToken: "token", CloudAPIURL: controlPlane.URL}
	if _, ok := reuseMachineCredential(context.Background(), controlPlane.Client(), credential); ok {
		t.Fatal("reuse accepted a revoked credential without a refresh secret")
	}
}
