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
	if !bytes.Contains(diagnostics.Bytes(), []byte("AGENTWHARF_LOCAL_ACCOUNT_BINDING")) {
		t.Fatalf("missing runtime failure: %s", diagnostics.String())
	}
}
