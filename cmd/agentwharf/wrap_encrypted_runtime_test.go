package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

func TestRequiredWrapRejectsUnprovisionedRuntimeBeforeHubDial(t *testing.T) {
	setupServeTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); http.Error(w, "unexpected", 500) }))
	defer server.Close()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	for _, local := range []*machineE2EERuntime{nil, runtime} {
		cfg := serveWrapConfig(machineServeDispatch{SessionID: "unprovisioned", Provider: "claude-code", HubWSURL: "ws" + strings.TrimPrefix(server.URL, "http"), AdapterToken: "test-token"}, false)
		cfg.ContentMode = protocol.ContentModeRequired
		cfg.e2eeRuntime = local
		_, err := runWrap(ctx, cfg, strings.NewReader(""), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "local encrypted session unavailable") {
			t.Fatalf("unexpected startup result: %v", err)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("unprovisioned runtime opened a Hub connection")
	}
}
