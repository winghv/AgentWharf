package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMachineRecoveryRetainsNativeSessionAcrossCredentialExpiry(t *testing.T) {
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(t.TempDir(), "machine.json"))
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"session": map[string]any{"id": "session", "provider": "pi"}, "hub_ws_url": "wss://hub.example/hub", "adapter_token": "new-token", "expires_at": time.Now().Add(time.Hour).Format(time.RFC3339), "encryption_mode": "required"}})
	}))
	defer server.Close()
	credential := machineCredential{CloudAPIURL: server.URL, HubWSURL: "wss://hub.example/hub", MachineToken: "machine-token"}
	session := machineRecoverableSession{SessionID: "session", Provider: "pi"}
	if _, err := recoverMachineSession(context.Background(), server.Client(), credential, session); err == nil || !strings.Contains(err.Error(), "provider_session_resume_failed") {
		t.Fatalf("missing history should block: %v", err)
	}
	if calls != 0 {
		t.Fatal("missing history refreshed authority")
	}
	prior := machineServeDispatch{ClaimID: "recovery:session", SessionID: "session", Provider: "pi", HubWSURL: credential.HubWSURL, AdapterToken: "expired-token", AdapterExpiresAt: time.Now().Add(-time.Hour).Format(time.RFC3339), ProviderSessionID: "original-native-session"}
	if err := saveMachineDispatch(prior); err != nil {
		t.Fatal(err)
	}
	recovered, err := recoverMachineSession(context.Background(), server.Client(), credential, session)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ProviderSessionID != prior.ProviderSessionID || recovered.AdapterToken != "new-token" {
		t.Fatal("recovery lost native identity or reused expired authority")
	}
	if err := saveMachineDispatch(*recovered); err != nil {
		t.Fatal(err)
	}
	persisted, err := loadMachineDispatches()
	if err != nil || len(persisted) != 1 || persisted[0].ProviderSessionID != prior.ProviderSessionID {
		t.Fatalf("native identity did not survive persistence: %v", err)
	}
}

func TestRecoveryWithoutNativeIdentityNeverStartsProvider(t *testing.T) {
	var logs bytes.Buffer
	handoff := &machineServeDispatch{ClaimID: "recovery:session", SessionID: "session", Provider: "pi"}
	err := keepAdapterAlive(context.Background(), machineServeConfig{}, handoff, &logs, &logs, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "provider_session_resume_failed") {
		t.Fatalf("missing identity must block before loading credentials or starting provider: %v", err)
	}
}
