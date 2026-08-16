package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServeWrapConfigCarriesLaunchSettings(t *testing.T) {
	handoff := machineServeDispatch{
		ClaimID: "claim_1", SessionID: "session_1", Provider: "claude-code",
		HubWSURL: "wss://hub.example/ws", AdapterToken: "adapter", ClientToken: "client",
		FirstInstruction: "build it", WorkingDirectory: "/tmp/repo",
		ModelID: "reasoning", ReasoningEffortID: "high", PermissionModeID: "acceptEdits",
		AdapterExpiresAt: time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		ClientExpiresAt:  time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	}
	cfg := serveWrapConfig(handoff, false)
	if cfg.LaunchSettings.ModelID != "reasoning" || cfg.LaunchSettings.ReasoningEffortID != "high" || cfg.LaunchSettings.PermissionModeID != "acceptEdits" {
		t.Fatalf("serveWrapConfig launch settings = %+v", cfg.LaunchSettings)
	}
	if cfg.ProviderCommand[0] != "claude-agent-acp" {
		t.Fatalf("serveWrapConfig provider command = %v", cfg.ProviderCommand)
	}
}

func TestExchangeAutoMachineClaimParsesLaunchSettings(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/machine-task-claims/claim_1/exchange" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer machine-token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		writeTestJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
			"session_id": "session_1", "provider": "codex", "hub_ws_url": "wss://hub.example/ws",
			"adapter_token": "adapter", "client_token": "client",
			"first_instruction": "build it", "delivery": "auto",
			"working_directory": "/tmp/repo",
			"model_id":          "balanced", "reasoning_effort_id": "medium", "permission_mode_id": "default",
			"adapter_expires_at": now.Add(24 * time.Hour).Format(time.RFC3339Nano),
			"client_expires_at":  now.Add(15 * time.Minute).Format(time.RFC3339Nano),
		}})
	}))
	defer server.Close()

	credential := machineCredential{CloudAPIURL: server.URL, MachineToken: "machine-token", MachineID: "machine_1"}
	handoff, err := exchangeAutoMachineClaim(context.Background(), server.Client(), credential, machinePendingClaim{
		ClaimID: "claim_1", TaskID: "task_1", RunID: "run_1", SessionID: "session_1", Provider: "codex",
	})
	if err != nil {
		t.Fatalf("exchangeAutoMachineClaim() error = %v", err)
	}
	if handoff.ModelID != "balanced" || handoff.ReasoningEffortID != "medium" || handoff.PermissionModeID != "default" {
		t.Fatalf("handoff launch settings = %+v", handoff)
	}
	if handoff.WorkingDirectory != "/tmp/repo" {
		t.Fatalf("handoff working directory = %q", handoff.WorkingDirectory)
	}
}

func TestParseAgentEntrypointConfigParsesLaunchSettingFlags(t *testing.T) {
	cfg, err := parseAgentEntrypointConfig("claude", []string{"--model", "reasoning", "--reasoning-effort", "high", "--permission-mode", "acceptEdits"}, nil)
	if err != nil {
		t.Fatalf("parseAgentEntrypointConfig() error = %v", err)
	}
	if cfg.LaunchSettings.ModelID != "reasoning" || cfg.LaunchSettings.ReasoningEffortID != "high" || cfg.LaunchSettings.PermissionModeID != "acceptEdits" {
		t.Fatalf("launch settings = %+v", cfg.LaunchSettings)
	}
}

func TestParseWrapConfigParsesLaunchSettingFlags(t *testing.T) {
	cfg, err := parseWrapConfig([]string{"--model", "balanced", "--permission-mode", "default"}, nil)
	if err != nil {
		t.Fatalf("parseWrapConfig() error = %v", err)
	}
	if cfg.LaunchSettings.ModelID != "balanced" || cfg.LaunchSettings.PermissionModeID != "default" || cfg.LaunchSettings.ReasoningEffortID != "" {
		t.Fatalf("launch settings = %+v", cfg.LaunchSettings)
	}
}
