package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
func TestHydrateMachineOnboardingHonorsTrustScope(t *testing.T) {
	var putEnabled *bool
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, `{"data":{"enabled":false,"org_id":"org_1","owner_user_id":"user_1"}}`)
		case http.MethodPut:
			var body struct {
				Enabled bool `json:"enabled"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode put body: %v", err)
			}
			value := body.Enabled
			putEnabled = &value
			fmt.Fprint(w, `{"data":{"enabled":true,"org_id":"org_1","owner_user_id":"user_1"}}`)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer controlPlane.Close()

	accountScoped := machineCredential{MachineID: "machine_1", MachineToken: "token", CloudAPIURL: controlPlane.URL}
	hydrateMachineOnboarding(context.Background(), controlPlane.Client(), &accountScoped, true, io.Discard)
	if accountScoped.LocalAccountBinding != "org_1/user_1" {
		t.Fatalf("binding = %q", accountScoped.LocalAccountBinding)
	}
	if putEnabled == nil || !*putEnabled {
		t.Fatalf("account scope put = %v, want true", putEnabled)
	}

	putEnabled = nil
	machineScoped := machineCredential{MachineID: "machine_1", MachineToken: "token", CloudAPIURL: controlPlane.URL}
	hydrateMachineOnboarding(context.Background(), controlPlane.Client(), &machineScoped, false, io.Discard)
	if putEnabled != nil {
		t.Fatalf("machine scope wrote trust = %v", *putEnabled)
	}
}

func TestPromptTrustScopeDefaultsForNonTerminal(t *testing.T) {
	if got := promptTrustScope(strings.NewReader("2\n"), io.Discard); got != trustScopeAccount {
		t.Fatalf("non-terminal prompt = %q, want %q", got, trustScopeAccount)
	}
}

func TestPairCommandRejectsInvalidTrustScope(t *testing.T) {
	for _, args := range [][]string{{"--trust-scope"}, {"--trust-scope", "everyone"}, {"https://a.example/v1", "extra"}} {
		if err := runPairCommandWithInput(context.Background(), args, strings.NewReader(""), io.Discard, io.Discard); err == nil {
			t.Fatalf("args %v error = nil, want usage error", args)
		}
	}
}
