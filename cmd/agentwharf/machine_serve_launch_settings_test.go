package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServeWrapConfigCarriesLaunchSettings(t *testing.T) {
	handoff := machineServeDispatch{
		ClaimID: "claim_1", SessionID: "session_1", Provider: "claude-code",
		HubWSURL: "wss://hub.example/ws", AdapterToken: "adapter", ClientToken: "client",
		EncryptedFirstInstruction: `{"version":1,"scope":"command","key_id":"key","sender":"sender","message_id":"claim_1:command","type":"session.send","packet":{"version":1,"public":{},"encrypted":{"nonce":"AAAAAAAAAAAAAAAA","ciphertext":"AAAAAAAAAAAAAAAAAAAAAA","signature":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}}`, WorkingDirectory: "/tmp/repo",
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

func TestRememberProviderSessionUpdatesWrapConfigForRestart(t *testing.T) {
	setupServeTestEnv(t)
	handoff := machineServeDispatch{
		ClaimID: "claim_1", SessionID: "session_1", Provider: "claude-code",
		HubWSURL: "wss://hub.example/ws", AdapterToken: "adapter", ClientToken: "client",
		AdapterExpiresAt: time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		ClientExpiresAt:  time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	}
	cfg := serveWrapConfig(handoff, false)
	remember := rememberProviderSession(&handoff, &cfg, nil, &sync.Mutex{})
	remember("acp_ses_1")
	if cfg.ProviderSessionID != "acp_ses_1" {
		t.Fatalf("wrap config provider session = %q, want acp_ses_1 so the next restart uses session/load", cfg.ProviderSessionID)
	}
	if handoff.ProviderSessionID != "acp_ses_1" {
		t.Fatalf("handoff provider session = %q, want acp_ses_1", handoff.ProviderSessionID)
	}
	loaded, err := loadMachineDispatches()
	if err != nil {
		t.Fatalf("load persisted dispatch: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ProviderSessionID != "acp_ses_1" {
		t.Fatalf("persisted provider session = %+v, want acp_ses_1", loaded)
	}
	cfg.ProviderSessionID = ""
	remember("acp_ses_1")
	if cfg.ProviderSessionID != "acp_ses_1" {
		t.Fatalf("restart copy of wrap config provider session = %q, want acp_ses_1", cfg.ProviderSessionID)
	}
}

func TestExchangeAutoMachineClaimRejectsUnauthenticatedLaunchSettings(t *testing.T) {
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
			"encryption_mode": "required", "encrypted_first_instruction": `{"version":1,"scope":"command","key_id":"key","sender":"sender","message_id":"claim_1:command","type":"session.send","packet":{"version":1,"public":{},"encrypted":{"nonce":"AAAAAAAAAAAAAAAA","ciphertext":"AAAAAAAAAAAAAAAAAAAAAA","signature":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}}`, "delivery": "auto",
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
	if err == nil || !strings.Contains(err.Error(), "unauthenticated launch settings") || handoff != nil {
		t.Fatalf("untrusted launch settings produced a handoff: error=%v", err)
	}
}

func TestParseAgentEntrypointConfigForwardsAgentArguments(t *testing.T) {
	cfg, err := parseAgentEntrypointConfig("claude", []string{"--model", "sonnet", "--permission-mode", "acceptEdits"}, nil)
	if err != nil {
		t.Fatalf("parseAgentEntrypointConfig() error = %v", err)
	}
	if strings.Join(cfg.ProviderCommand, " ") != "claude-agent-acp --model sonnet --permission-mode acceptEdits" {
		t.Fatalf("provider command = %v", cfg.ProviderCommand)
	}
}

func TestParseAgentEntrypointConfigKeepsWharfFlagsBeforeAgentArgs(t *testing.T) {
	cfg, err := parseAgentEntrypointConfig("codex", []string{"--session", "--model", "gpt-5"}, nil)
	if err != nil {
		t.Fatalf("parseAgentEntrypointConfig() error = %v", err)
	}
	if cfg.PairOnly || !cfg.Session {
		t.Fatalf("config = %+v, want Session=true PairOnly=false", cfg)
	}
	if strings.Join(cfg.ProviderCommand, " ") != "codex-acp --model gpt-5" {
		t.Fatalf("provider command = %v", cfg.ProviderCommand)
	}
}
