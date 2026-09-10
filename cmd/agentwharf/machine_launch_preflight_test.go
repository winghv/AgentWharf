package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMachineRejectsForgedLaunchBeforeHubDial(t *testing.T) {
	setupServeTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var dials atomic.Int32
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dials.Add(1); w.WriteHeader(http.StatusForbidden) }))
	defer hub.Close()
	hubURL := "ws" + strings.TrimPrefix(hub.URL, "http")
	var launch string
	cloud, _, _, _ := newServeTestControlPlane(t, hubURL, "session", 0, false, &launch)
	if err := saveMachineCredential(machineCredential{MachineID: "machine_serve", MachineToken: "machine-token", CloudAPIURL: cloud.URL, HubWSURL: hubURL, ExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	var carrier map[string]any
	if err := json.Unmarshal([]byte(launch), &carrier); err != nil {
		t.Fatal(err)
	}
	packet := carrier["packet"].(map[string]any)
	envelope := packet["encrypted"].(map[string]any)
	envelope["signature"] = strings.Repeat("A", 86)
	forged, err := json.Marshal(carrier)
	if err != nil {
		t.Fatal(err)
	}
	handoff := &machineServeDispatch{SessionID: "session", Provider: "claude-code", HubWSURL: hubURL, AdapterToken: "synthetic-adapter", EncryptedFirstInstruction: string(forged), AdapterExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339)}
	var output, diagnostics bytes.Buffer
	err = keepAdapterAlive(ctx, machineServeConfig{}, handoff, &output, &diagnostics, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "encrypted launch configuration rejected") {
		t.Fatalf("forged launch result: %v", err)
	}
	if dials.Load() != 0 || output.Len() != 0 {
		t.Fatal("forged launch reached runtime transport")
	}
}
