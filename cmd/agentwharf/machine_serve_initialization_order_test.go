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

// The initialization discovery poll must not be starved by claim traffic.
//
// This is the ordering the daemon guarantees: the pending-initializer
// discovery is polled before the claims are requested, so a Session whose
// initialization is pending is never held back behind a claim request.
//
// The assertion used to be `polls >= 2`, which was a proxy for "the
// initialization poll keeps running". That proxy stopped being true when
// claim pickup moved into its own faster loop (6542b8c): claims are now
// fetched every couple of seconds while the maintenance poll -- which
// carries the initialization discovery -- runs on the slower cadence. The
// count of initialization polls is therefore not a statement about ordering.
//
// The invariant is the ordering itself, and it is asserted directly: every
// claim request must arrive after an initialization poll, and the poll must
// keep happening while claims keep failing.
func TestServeInitializationContinuesWhenClaimsUnavailable(t *testing.T) {
	setupServeTestEnv(t)
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "synthetic-account")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var polls, claims atomic.Int32
	var claimsBeforeFirstPoll atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/e2ee-sessions/pending") {
			polls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		if polls.Load() == 0 {
			claimsBeforeFirstPoll.Add(1)
		}
		claims.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if err := saveMachineCredential(machineCredential{MachineID: "machine_serve", MachineToken: "synthetic-token", CloudAPIURL: server.URL, HubWSURL: "ws://unused.invalid", ExpiresAt: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- runWithInput(ctx, []string{"serve", "--foreground", "--poll-interval", "1"}, strings.NewReader(""), &stdout, &stderr)
	}()
	// Wait for the maintenance loop to poll the initialization endpoint at
	// least twice, so the assertion is about the poll keeping up rather than
	// about how fast the machine happened to answer the claims. The daemon
	// polls once at startup and then once per interval, and the claim pickup
	// runs on its own faster loop, so a fast machine reaches the claim count
	// long before the second poll.
	deadline := time.Now().Add(5 * time.Second)
	for polls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	err := <-serveDone
	if err != nil && !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatal(err)
	}
	// The ordering: no claim request may precede the initialization poll.
	if claimsBeforeFirstPoll.Load() != 0 {
		t.Fatalf("%d claim requests arrived before the initialization poll", claimsBeforeFirstPoll.Load())
	}
	// The claims must keep being attempted.
	if claims.Load() < 2 {
		t.Fatalf("the claim pickup stopped: polls=%d claims=%d", polls.Load(), claims.Load())
	}
	// And the initialization poll must have kept running alongside them, so a
	// pending initializer is never starved by claim traffic.
	if polls.Load() < 2 {
		t.Fatalf("initialization starved: polls=%d claims=%d", polls.Load(), claims.Load())
	}
}
