package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMachineDispatchCancelsSenderWhenRuntimeUnavailable(t *testing.T) {
	setupServeTestEnv(t)
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	var output, diagnostics bytes.Buffer
	var adapters sync.WaitGroup
	done := make(chan struct{})
	go func() {
		dispatchOutcome(ctx, machineServeConfig{}, &machineServeDispatch{Provider: "claude-code", SessionID: "session", HubWSURL: "ws" + strings.TrimPrefix(server.URL, "http")}, &machineServeLockedWriter{Writer: &output}, &machineServeLockedWriter{Writer: &diagnostics}, &adapters, nil, nil)
		adapters.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("instruction sender outlived failed adapter startup")
	}
	if output.Len() != 0 {
		t.Fatal("unavailable runtime reported successful dispatch")
	}
	if !bytes.Contains(diagnostics.Bytes(), []byte("machine credential")) {
		t.Fatalf("missing runtime failure: %s", diagnostics.String())
	}
}

func TestBackgroundRecoveryAdaptersDoNotConsumeDispatchSlots(t *testing.T) {
	const maxConcurrent = 2
	sem := make(chan struct{}, maxConcurrent)
	var workers sync.WaitGroup
	var adapters sync.WaitGroup
	adapterStop := make(chan struct{})
	adapterStarted := make(chan struct{}, maxConcurrent)

	for range maxConcurrent {
		workers.Add(1)
		go func() {
			defer workers.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			startBackgroundAdapter(&adapters, func() {
				adapterStarted <- struct{}{}
				<-adapterStop
			})
		}()
	}
	for range maxConcurrent {
		select {
		case <-adapterStarted:
		case <-time.After(time.Second):
			t.Fatal("recovery adapter did not start")
		}
	}

	workers.Wait()
	thirdDispatch := make(chan struct{})
	go func() {
		sem <- struct{}{}
		close(thirdDispatch)
		<-sem
	}()
	select {
	case <-thirdDispatch:
	case <-time.After(time.Second):
		t.Fatal("background recovery adapters consumed every dispatch slot")
	}

	close(adapterStop)
	adapters.Wait()
}
