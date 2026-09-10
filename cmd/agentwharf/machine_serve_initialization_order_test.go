package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestServeInitializationContinuesWhenClaimsUnavailable(t *testing.T) {
	setupServeTestEnv(t)
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "synthetic-account")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var polls, claims atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/e2ee-sessions/pending") {
			polls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		if polls.Load() == 0 {
			t.Error("claims requested before initialization")
		}
		if claims.Add(1) >= 2 {
			cancel()
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if err := saveMachineCredential(machineCredential{MachineID: "machine_serve", MachineToken: "synthetic-token", CloudAPIURL: server.URL, HubWSURL: "ws://unused.invalid", ExpiresAt: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	err := runWithInput(ctx, []string{"serve", "--foreground", "--poll-interval", "1"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil && !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatal(err)
	}
	if claims.Load() < 2 || polls.Load() < 2 {
		t.Fatalf("initialization starved: polls=%d claims=%d", polls.Load(), claims.Load())
	}
}
