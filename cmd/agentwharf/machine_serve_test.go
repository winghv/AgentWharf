package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

// serveTestHub is a fake platform Hub that accepts one Adapter connection and
// one Client connection for a machine serve dispatch. It forwards the
// adapter's interactive session.state to the client socket and records the
// client command that delivers the first instruction.
type serveTestHub struct {
	Server *httptest.Server

	// silentClientHellos makes the first N client connections receive an
	// attach-only hello.ack with no event subscription, mirroring the real
	// Hub's admission while a Session is still `starting`.
	silentClientHellos int32

	mu          sync.Mutex
	clientConns int32
	readyCh     chan struct{}
	readyOnce   sync.Once
	commands    []recordedCommand
	clientHello []protocol.Hello
	adapterDone chan struct{}
}

type recordedCommand struct {
	CommandID string
	SessionID string
	Payload   string
}

func newServeTestHub(t *testing.T, ctx context.Context, sessionID string) *serveTestHub {
	t.Helper()
	hub := &serveTestHub{readyCh: make(chan struct{}), adapterDone: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		first, err := readFrameFromConn(ctx, conn)
		if err != nil {
			return
		}
		hello, ok := first.(*protocol.Hello)
		if !ok {
			return
		}
		switch hello.Role {
		case protocol.RoleAdapter:
			hub.serveAdapter(ctx, conn, sessionID)
		case protocol.RoleClient:
			hub.serveClient(ctx, conn, *hello, sessionID)
		}
	})
	hub.Server = httptest.NewServer(mux)
	return hub
}

func (h *serveTestHub) serveAdapter(ctx context.Context, conn *websocket.Conn, sessionID string) {
	defer close(h.adapterDone)
	if err := writeFrameToConn(ctx, conn, &protocol.HelloAck{ProtocolVersion: protocol.ProtocolVersionV2, Sessions: []protocol.SessionSummary{{SessionID: sessionID, Provider: "claude-code"}}, ConnectionAuthority: &protocol.ConnectionAuthorityReceipt{SessionID: sessionID, ConnectionEpoch: 1, CredentialGeneration: 1, AcceptedFence: 1, WriterLeaseID: "lease_serve", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}}); err != nil {
		return
	}
	frame, err := readProviderStartFrame(ctx, conn)
	if err != nil {
		return
	}
	start, ok := frame.(*protocol.ProviderStart)
	if !ok {
		return
	}
	if err := writeFrameToConn(ctx, conn, &protocol.ProviderStartPrepare{Attempt: start.Attempt}); err != nil {
		return
	}
	if _, err := readFrameFromConn(ctx, conn); err != nil {
		return
	}
	if err := writeFrameToConn(ctx, conn, &protocol.ProviderStartAck{Attempt: start.Attempt, Status: protocol.ProviderStartAdmitted, RecoveryHandle: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"}); err != nil {
		return
	}
	for {
		frame, err := readFrameFromConn(ctx, conn)
		if err != nil {
			return
		}
		event, ok := frame.(*protocol.Event)
		if !ok {
			continue
		}
		if event.Type == "session.state" && event.ProposalID != "" && strings.Contains(string(event.Payload), `"state":"ready"`) {
			if err := writeFrameToConn(ctx, conn, &protocol.EventReceipt{ProposalID: event.ProposalID, Seq: 2, Status: protocol.EventReceiptAccepted}); err != nil {
				return
			}
			h.readyOnce.Do(func() { close(h.readyCh) })
		}
		if event.Type == "session.state" && event.ProposalID != "" && strings.Contains(string(event.Payload), `"state":"ended"`) {
			_ = writeFrameToConn(ctx, conn, &protocol.EventReceipt{ProposalID: event.ProposalID, Seq: 3, Status: protocol.EventReceiptAccepted})
			return
		}
		if event.ProposalID != "" {
			_ = writeFrameToConn(ctx, conn, &protocol.EventReceipt{ProposalID: event.ProposalID, Seq: 1, Status: protocol.EventReceiptAccepted})
		}
	}
}

func (h *serveTestHub) serveClient(ctx context.Context, conn *websocket.Conn, hello protocol.Hello, sessionID string) {
	h.mu.Lock()
	h.clientHello = append(h.clientHello, hello)
	connection := h.clientConns
	h.clientConns++
	h.mu.Unlock()
	if connection < h.silentClientHellos {
		// Attach-only admission while the Session is `starting`: the hello.ack
		// carries the non-interactive state and no events are subscribed.
		if err := writeFrameToConn(ctx, conn, &protocol.HelloAck{ProtocolVersion: protocol.ProtocolVersionV2, Sessions: []protocol.SessionSummary{{SessionID: sessionID, State: "attach_only", Provider: "claude-code"}}}); err != nil {
			return
		}
		for {
			frame, err := readFrameFromConn(ctx, conn)
			if err != nil {
				return
			}
			if ping, ok := frame.(*protocol.Ping); ok {
				_ = writeFrameToConn(ctx, conn, &protocol.Pong{Nonce: ping.Nonce})
			}
		}
	}
	if err := writeFrameToConn(ctx, conn, &protocol.HelloAck{ProtocolVersion: protocol.ProtocolVersionV2, Sessions: []protocol.SessionSummary{}}); err != nil {
		return
	}
	select {
	case <-h.readyCh:
	case <-ctx.Done():
		return
	}
	readyPayload, _ := json.Marshal(map[string]string{"state": "ready"})
	if err := writeFrameToConn(ctx, conn, &protocol.Event{Type: "session.state", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: readyPayload}); err != nil {
		return
	}
	frame, err := readFrameFromConn(ctx, conn)
	if err != nil {
		return
	}
	command, ok := frame.(*protocol.Command)
	if !ok {
		return
	}
	h.mu.Lock()
	h.commands = append(h.commands, recordedCommand{CommandID: command.CommandID, SessionID: command.SessionID, Payload: string(command.Payload)})
	h.mu.Unlock()
	_ = writeFrameToConn(ctx, conn, &protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted})
	// Keep the socket open so the sender sees the ack before any close races.
	select {
	case <-ctx.Done():
	case <-h.adapterDone:
	}
}

func (h *serveTestHub) recordedCommands() []recordedCommand {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]recordedCommand(nil), h.commands...)
}

func newServeTestControlPlane(t *testing.T, hubURL, sessionID string, pending int32, refusePending bool) (*httptest.Server, *atomic.Int32, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var pendingPolls atomic.Int32
	var exchanges atomic.Int32
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/machine-task-claims/pending" && r.Method == http.MethodGet:
			pendingPolls.Add(1)
			if refusePending {
				writeTestJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"code": "unauthorized", "message": "invalid machine token"}})
				return
			}
			claims := []map[string]any{}
			if pendingPolls.Load() <= pending {
				claims = append(claims, map[string]any{
					"claim_id": "claim_auto", "task_id": "task_auto", "run_id": "run_auto", "session_id": sessionID,
					"provider":   "claude-code",
					"created_at": time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
					"expires_at": time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano),
				})
			}
			writeTestJSON(w, http.StatusOK, map[string]any{"data": claims})
		case r.URL.Path == "/machine-task-claims/claim_auto/exchange" && r.Method == http.MethodPost:
			exchanges.Add(1)
			if r.Header.Get("Authorization") != "Bearer machine-token" {
				t.Errorf("exchange authorization = %q", r.Header.Get("Authorization"))
			}
			writeTestJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
				"session_id": sessionID, "provider": "claude-code", "hub_ws_url": hubURL,
				"adapter_token": "adapter-token", "client_token": "client-token",
				"first_instruction": "build a login page", "delivery": "auto",
				"adapter_expires_at": time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339Nano),
				"client_expires_at":  time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano),
			}})
		case r.URL.Path == "/machine-token/refresh" && r.Method == http.MethodPost:
			refreshes.Add(1)
			if authorization := r.Header.Get("Authorization"); authorization != "Bearer machine-token" && authorization != "Bearer refreshed-machine-token" {
				t.Errorf("refresh authorization = %q", authorization)
			}
			writeTestJSON(w, http.StatusOK, map[string]any{"data": map[string]any{
				"machine":       map[string]any{"id": "machine_serve"},
				"machine_token": "refreshed-machine-token",
				"expires_at":    time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339Nano),
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, &pendingPolls, &exchanges, &refreshes
}

func writeTestJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func setupServeTestEnv(t *testing.T) string {
	t.Helper()
	providerDir := t.TempDir()
	providerPath := filepath.Join(providerDir, "claude-agent-acp")
	providerScript := fmt.Sprintf("#!/usr/bin/env bash\nexport AGENTWHARF_ACP_IDLE_HELPER=1\nexec %q\n", os.Args[0])
	if err := os.WriteFile(providerPath, []byte(providerScript), 0o755); err != nil {
		t.Fatalf("write provider helper: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	credentialDir := t.TempDir()
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(credentialDir, "machine.json"))
	return credentialDir
}

func TestMachineServeDispatchesAutoClaimEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	credentialDir := setupServeTestEnv(t)

	hub := newServeTestHub(t, ctx, "ses_auto")
	t.Cleanup(hub.Server.Close)
	controlPlane, _, exchanges, refreshes := newServeTestControlPlane(t, "ws"+strings.TrimPrefix(hub.Server.URL, "http"), "ses_auto", 1, false)

	if err := saveMachineCredential(machineCredential{MachineID: "machine_serve", MachineToken: "machine-token", CloudAPIURL: controlPlane.URL, HubWSURL: "ws://unused.invalid", ExpiresAt: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
		t.Fatalf("save machine credential: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithInput(ctx, []string{"serve", "--foreground", "--poll-interval", "1", "--startup-smoke"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("machine serve: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "auto_dispatch_ok: claim_id=claim_auto session_id=ses_auto") {
		t.Fatalf("serve output = %q stderr=%q", stdout.String(), stderr.String())
	}
	commands := hub.recordedCommands()
	if len(commands) != 1 {
		t.Fatalf("hub commands = %+v, want exactly one first instruction", commands)
	}
	if commands[0].CommandID != "claim_auto:command" || commands[0].SessionID != "ses_auto" {
		t.Fatalf("hub command = %+v", commands[0])
	}
	if !strings.Contains(commands[0].Payload, "build a login page") {
		t.Fatalf("hub command payload = %s", commands[0].Payload)
	}
	if exchanges.Load() != 1 {
		t.Fatalf("exchanges = %d, want 1", exchanges.Load())
	}
	if refreshes.Load() != 0 {
		t.Fatalf("refreshes = %d, want 0", refreshes.Load())
	}
	entries, err := os.ReadDir(filepath.Join(credentialDir, "dispatch"))
	if err == nil && len(entries) != 0 {
		t.Fatalf("dispatch directory not cleaned: %v", entries)
	}
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read dispatch directory: %v", err)
	}
}

func TestMachineServeRetriesWhileSessionIsStarting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	credentialDir := setupServeTestEnv(t)

	hub := newServeTestHub(t, ctx, "ses_auto")
	hub.silentClientHellos = 1
	t.Cleanup(hub.Server.Close)
	controlPlane, _, exchanges, _ := newServeTestControlPlane(t, "ws"+strings.TrimPrefix(hub.Server.URL, "http"), "ses_auto", 1, false)

	if err := saveMachineCredential(machineCredential{MachineID: "machine_serve", MachineToken: "machine-token", CloudAPIURL: controlPlane.URL, HubWSURL: "ws://unused.invalid", ExpiresAt: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
		t.Fatalf("save machine credential: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithInput(ctx, []string{"serve", "--foreground", "--poll-interval", "1", "--startup-smoke"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("machine serve starting-session retry: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "auto_dispatch_ok: claim_id=claim_auto session_id=ses_auto") {
		t.Fatalf("serve retry output = %q stderr=%q", stdout.String(), stderr.String())
	}
	commands := hub.recordedCommands()
	if len(commands) != 1 || commands[0].CommandID != "claim_auto:command" {
		t.Fatalf("hub commands = %+v, want exactly one first instruction", commands)
	}
	if exchanges.Load() != 1 {
		t.Fatalf("exchanges = %d, want 1", exchanges.Load())
	}
	entries, err := os.ReadDir(filepath.Join(credentialDir, "dispatch"))
	if err == nil && len(entries) != 0 {
		t.Fatalf("dispatch directory not cleaned: %v", entries)
	}
}

func TestMachineServeResumesPersistedHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	credentialDir := setupServeTestEnv(t)

	hub := newServeTestHub(t, ctx, "ses_auto")
	t.Cleanup(hub.Server.Close)
	controlPlane, _, exchanges, _ := newServeTestControlPlane(t, "ws"+strings.TrimPrefix(hub.Server.URL, "http"), "ses_auto", 0, false)

	if err := saveMachineCredential(machineCredential{MachineID: "machine_serve", MachineToken: "machine-token", CloudAPIURL: controlPlane.URL, HubWSURL: "ws://unused.invalid", ExpiresAt: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
		t.Fatalf("save machine credential: %v", err)
	}
	dispatchDir := filepath.Join(credentialDir, "dispatch")
	handoff := machineServeDispatch{
		ClaimID: "claim_auto", TaskID: "task_auto", RunID: "run_auto", SessionID: "ses_auto",
		Provider: "claude-code", HubWSURL: "ws" + strings.TrimPrefix(hub.Server.URL, "http"),
		AdapterToken: "adapter-token", ClientToken: "client-token", FirstInstruction: "build a login page",
		AdapterExpiresAt: time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano),
		ClientExpiresAt:  time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano),
	}
	if err := saveMachineDispatch(handoff); err != nil {
		t.Fatalf("save handoff: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithInput(ctx, []string{"serve", "--foreground", "--poll-interval", "1", "--startup-smoke"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("machine serve resume: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "auto_dispatch_ok: claim_id=claim_auto session_id=ses_auto") {
		t.Fatalf("serve resume output = %q stderr=%q", stdout.String(), stderr.String())
	}
	commands := hub.recordedCommands()
	if len(commands) != 1 || commands[0].CommandID != "claim_auto:command" {
		t.Fatalf("hub commands = %+v", commands)
	}
	if exchanges.Load() != 0 {
		t.Fatalf("exchanges = %d, want 0 for a persisted handoff", exchanges.Load())
	}
	if _, err := os.Stat(filepath.Join(dispatchDir, "claim_auto.json")); !os.IsNotExist(err) {
		t.Fatalf("handoff file must be removed after dispatch, stat err=%v", err)
	}
}

func TestMachineServeLoadsPersistedRecoveryHandoff(t *testing.T) {
	credentialDir := setupServeTestEnv(t)
	if err := saveMachineCredential(machineCredential{
		MachineID: "machine_serve", MachineToken: "machine-token", CloudAPIURL: "https://cloud.example",
	}); err != nil {
		t.Fatalf("save machine credential: %v", err)
	}
	handoff := machineServeDispatch{
		ClaimID: "recovery:ses_recover", SessionID: "ses_recover", Provider: "claude-code",
		HubWSURL: "wss://hub.example/ws", AdapterToken: "adapter-token",
		AdapterExpiresAt: time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
	}
	if !isMachineRecoveryDispatch(handoff) {
		t.Fatal("recovery handoff was not recognized")
	}
	if err := saveMachineDispatch(handoff); err != nil {
		t.Fatalf("save recovery handoff: %v", err)
	}
	loaded, err := loadMachineDispatches()
	if err != nil {
		t.Fatalf("load recovery handoff: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ClaimID != handoff.ClaimID {
		t.Fatalf("loaded handoffs = %+v", loaded)
	}
	if _, err := os.Stat(filepath.Join(credentialDir, "dispatch", "recovery:ses_recover.json")); err != nil {
		t.Fatalf("recovery handoff was not persisted: %v", err)
	}
}

func TestMachineRecoveryGuardReleasesAfterWorkerExit(t *testing.T) {
	guard := newMachineRecoveryGuard()
	if !guard.claim("ses_recovery") {
		t.Fatal("first recovery claim was rejected")
	}
	if guard.claim("ses_recovery") {
		t.Fatal("duplicate recovery claim was accepted")
	}
	guard.release("ses_recovery")
	if !guard.claim("ses_recovery") {
		t.Fatal("recovery claim was not reusable after worker exit")
	}
}

func TestMachineServeRestartsOnlyFailedAdapters(t *testing.T) {
	if machineServeAdapterShouldRestart(nil, true, 0) {
		t.Fatal("clean adapter exit must remain terminal")
	}
	if !machineServeAdapterShouldRestart(errors.New("transport failed"), true, 0) {
		t.Fatal("failed adapter with live credential should restart")
	}
	if machineServeAdapterShouldRestart(errors.New("transport failed"), false, 0) {
		t.Fatal("expired adapter credential must not restart")
	}
	if machineServeAdapterShouldRestart(errors.New("transport failed"), true, machineServeAdapterRestartLimit) {
		t.Fatal("adapter at restart limit must not restart")
	}
}

func TestMachineServeRefreshesRejectedMachineCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	setupServeTestEnv(t)

	hub := newServeTestHub(t, ctx, "ses_auto")
	t.Cleanup(hub.Server.Close)
	controlPlane, _, _, refreshes := newServeTestControlPlane(t, "ws"+strings.TrimPrefix(hub.Server.URL, "http"), "ses_auto", 0, true)

	if err := saveMachineCredential(machineCredential{MachineID: "machine_serve", MachineToken: "machine-token", CloudAPIURL: controlPlane.URL, HubWSURL: "ws://unused.invalid", ExpiresAt: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)}); err != nil {
		t.Fatalf("save machine credential: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithInput(ctx, []string{"serve", "--foreground", "--poll-interval", "1"}, strings.NewReader(""), &stdout, &stderr); err != nil && !strings.Contains(err.Error(), context.Canceled.Error()) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("machine serve refresh: %v stderr=%q", err, stderr.String())
	}
	if refreshes.Load() < 1 {
		t.Fatalf("refreshes = %d, want at least one", refreshes.Load())
	}
	credential, err := loadMachineCredential()
	if err != nil {
		t.Fatalf("reload refreshed credential: %v", err)
	}
	if credential.MachineToken != "refreshed-machine-token" {
		t.Fatalf("credential was not refreshed: %+v", credential)
	}
	if !strings.Contains(stderr.String(), "machine credential rejected") {
		t.Fatalf("serve stderr must surface the rejected credential: %q", stderr.String())
	}
}

func TestParseMachineServeConfigRejectsInvalidArguments(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"--poll-interval", "0"},
		{"--poll-interval", "x"},
		{"--max-concurrent", "0"},
		{"--bogus"},
		{"extra"},
	} {
		if _, err := parseMachineServeConfig(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("parseMachineServeConfig(%v) succeeded, want error", args)
		}
	}
	cfg, err := parseMachineServeConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseMachineServeConfig(nil) error = %v", err)
	}
	if cfg.PollInterval != machineServeDefaultPollInterval || cfg.MaxConcurrent != machineServeDefaultMaxConcurrent || cfg.StartupSmoke {
		t.Fatalf("default config = %+v", cfg)
	}
	cfg, err = parseMachineServeConfig([]string{"--poll-interval", "2", "--max-concurrent", "3", "--startup-smoke"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseMachineServeConfig error = %v", err)
	}
	if cfg.PollInterval != 2*time.Second || cfg.MaxConcurrent != 3 || !cfg.StartupSmoke {
		t.Fatalf("parsed config = %+v", cfg)
	}
}
