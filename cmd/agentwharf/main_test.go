package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

func TestServeStartsLocalHubWithSQLiteAndStaticAuth(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "events.db")
	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       dbPath,
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	if running.addr == "" || running.wsURL == "" {
		t.Fatalf("running server missing addresses: %+v", running)
	}
	parsed, err := url.Parse(running.wsURL)
	if err != nil {
		t.Fatalf("parse ws url: %v", err)
	}
	if parsed.Scheme != "ws" || parsed.Host == "" {
		t.Fatalf("ws url = %q", running.wsURL)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat sqlite db: %v", err)
	}

	conn, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial hub: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	ack := readFrame(t, conn).(*protocol.HelloAck)
	if ack.ProtocolVersion != protocol.ProtocolVersion || len(ack.Sessions) != 1 {
		t.Fatalf("hello ack = %+v", ack)
	}
	if got := ack.Sessions[0]; got.SessionID != "ses_local" || got.Provider != "claude-code" ||
		got.State != "ready" || got.LatestSeq != 0 || got.ReplayFrom != 1 {
		t.Fatalf("session summary = %+v", got)
	}
}

func TestRunServeRejectsNonLocalDefaultToken(t *testing.T) {
	t.Parallel()

	err := run(context.Background(), []string{
		"serve",
		"--addr", "0.0.0.0:0",
		"--db", filepath.Join(t.TempDir(), "events.db"),
	}, io.Discard, io.Discard)
	if err == nil || !errors.Is(err, errUnsafeDefaultToken) {
		t.Fatalf("run serve error = %v, want errUnsafeDefaultToken", err)
	}
}

func TestParseWrapConfigAcceptsClaudeACPFlag(t *testing.T) {
	t.Parallel()

	cfg, err := parseWrapConfig([]string{
		"--agent", "claude",
		"--acp",
		"--hub", "ws://127.0.0.1:8765",
		"--session-id", "ses_1",
		"--adapter-token", "adapter-token",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseWrapConfig() error = %v", err)
	}
	if cfg.Agent != "claude" || cfg.Provider != "claude-code" || cfg.Format != "acp" {
		t.Fatalf("wrap config = %+v", cfg)
	}
}

func TestParseWrapConfigUsesEnvironmentDefaults(t *testing.T) {
	t.Setenv("AGENTWHARF_HUB_URL", "wss://hub.superwhv.example/ws")
	t.Setenv("AGENTWHARF_SESSION_ID", "session_env")
	t.Setenv("AGENTWHARF_ADAPTER_TOKEN", "adapter-env-token")
	t.Setenv("AGENTWHARF_PROVIDER", "claude-code")

	cfg, err := parseWrapConfig([]string{"--acp"}, io.Discard)
	if err != nil {
		t.Fatalf("parseWrapConfig() error = %v", err)
	}
	if cfg.HubURL != "wss://hub.superwhv.example/ws" ||
		cfg.SessionID != "session_env" ||
		cfg.AdapterToken != "adapter-env-token" ||
		cfg.Provider != "claude-code" {
		t.Fatalf("wrap config = %+v", cfg)
	}
}

func TestParseWrapConfigAcceptsPairing(t *testing.T) {
	t.Parallel()

	cfg, err := parseWrapConfig([]string{
		"--pair",
		"--cloud", "https://cloud.superwhv.example/v1",
		"--agent", "claude",
		"--acp",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseWrapConfig() error = %v", err)
	}
	if !cfg.Pair || cfg.CloudAPIURL != "https://cloud.superwhv.example/v1" ||
		cfg.Provider != "claude-code" || cfg.Format != "acp" {
		t.Fatalf("wrap config = %+v", cfg)
	}
}

func TestParseAgentEntrypointDefaultsToManagedClaudePairing(t *testing.T) {
	cfg, err := parseAgentEntrypointConfig("claude", nil, io.Discard)
	if err != nil {
		t.Fatalf("parseAgentEntrypointConfig() error = %v", err)
	}
	if cfg.Agent != "claude" ||
		cfg.Provider != "claude-code" ||
		cfg.Format != "acp" ||
		!cfg.Pair ||
		cfg.CloudAPIURL != defaultManagedCloudAPIURL ||
		strings.Join(cfg.ProviderCommand, " ") != "claude-agent-acp" {
		t.Fatalf("agent entrypoint config = %+v", cfg)
	}
}

func TestParseAgentEntrypointUsesInjectedSessionWithoutPairing(t *testing.T) {
	t.Setenv("AGENTWHARF_HUB_URL", "wss://hub.superwhv.example/hub")
	t.Setenv("AGENTWHARF_SESSION_ID", "ses_vm")
	t.Setenv("AGENTWHARF_ADAPTER_TOKEN", "adapter-token")

	cfg, err := parseAgentEntrypointConfig("codex", nil, io.Discard)
	if err != nil {
		t.Fatalf("parseAgentEntrypointConfig() error = %v", err)
	}
	if cfg.Agent != "codex" ||
		cfg.Provider != "codex" ||
		cfg.Format != "acp" ||
		cfg.Pair ||
		cfg.CloudAPIURL != "" ||
		cfg.HubURL != "wss://hub.superwhv.example/hub" ||
		cfg.SessionID != "ses_vm" ||
		cfg.AdapterToken != "adapter-token" ||
		strings.Join(cfg.ProviderCommand, " ") != "codex-acp" {
		t.Fatalf("agent entrypoint config = %+v", cfg)
	}
}

func TestRunWrapChecksProviderCommandBeforePairing(t *testing.T) {
	t.Parallel()

	var requests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		http.Error(w, "unexpected pairing request", http.StatusInternalServerError)
	}))
	defer controlPlane.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := runWrap(ctx, wrapConfig{
		Agent:           "claude",
		Provider:        "claude-code",
		Format:          "acp",
		Pair:            true,
		CloudAPIURL:     controlPlane.URL,
		ProviderCommand: []string{"definitely-missing-agentwharf-provider-command"},
	}, strings.NewReader(""), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "provider command definitely-missing-agentwharf-provider-command not found") {
		t.Fatalf("runWrap() error = %v, want missing provider command", err)
	}
	if requests != 0 {
		t.Fatalf("pairing requests = %d, want 0 before provider command is available", requests)
	}
}

func TestParseAgentEntrypointTreatsEmptyInjectedSessionAsMissing(t *testing.T) {
	t.Setenv("AGENTWHARF_HUB_URL", "")
	t.Setenv("AGENTWHARF_SESSION_ID", "ses_vm")
	t.Setenv("AGENTWHARF_ADAPTER_TOKEN", "adapter-token")

	cfg, err := parseAgentEntrypointConfig("claude", nil, io.Discard)
	if err != nil {
		t.Fatalf("parseAgentEntrypointConfig() error = %v", err)
	}
	if !cfg.Pair || cfg.CloudAPIURL != defaultManagedCloudAPIURL {
		t.Fatalf("agent entrypoint config = %+v, want managed pairing", cfg)
	}
}

func TestRunWrapPairingCreatesMachineSessionAndPublishesEvents(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_machine",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "paired-adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/machine-pairing-codes":
			if r.Method != http.MethodPost {
				t.Fatalf("pairing method = %s", r.Method)
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"data":{"device_code":"device-code-1","user_code":"ABCD-E","verification_uri":"https://cloud.example/pair","expires_at":"2026-06-18T10:10:00Z","interval_seconds":1}}`)
		case "/machine-pairing-codes/token":
			if r.Method != http.MethodPost {
				t.Fatalf("exchange method = %s", r.Method)
			}
			var body struct {
				DeviceCode string `json:"device_code"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode exchange request: %v", err)
			}
			if body.DeviceCode != "device-code-1" {
				t.Fatalf("device code = %q", body.DeviceCode)
			}
			fmt.Fprint(w, `{"data":{"machine":{"id":"machine_1"},"machine_token":"machine-token","hub_ws_url":"wss://ignored.example/ws","expires_at":"2026-06-19T10:00:00Z"}}`)
		case "/machine-sessions":
			if r.Method != http.MethodPost {
				t.Fatalf("machine session method = %s", r.Method)
			}
			if r.Header.Get("Authorization") != "Bearer machine-token" {
				t.Fatalf("machine session authorization = %q", r.Header.Get("Authorization"))
			}
			var body struct {
				Provider string `json:"provider"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode machine session request: %v", err)
			}
			if body.Provider != "claude-code" {
				t.Fatalf("provider = %q", body.Provider)
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"data":{"session":{"id":"ses_machine","host_type":"machine","host_id":"machine_1","provider":"claude-code","status":"starting","started_at":"2026-06-18T10:00:00Z"},"hub_ws_url":%q,"adapter_token":"paired-adapter-token","expires_at":"2026-06-18T10:15:00Z"}}`, running.wsURL)
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_machine"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	stderr := new(strings.Builder)
	stdin := strings.NewReader(`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"paired pong"}]}}`)
	if err := runWithInput(ctx, []string{
		"wrap",
		"--pair",
		"--cloud", controlPlane.URL,
		"--agent", "claude",
		"--jsonstream",
	}, stdin, io.Discard, stderr); err != nil {
		t.Fatalf("run wrap pair error = %v", err)
	}
	if !strings.Contains(stderr.String(), "device-code-1") || !strings.Contains(stderr.String(), "ABCD-E") ||
		strings.Contains(stderr.String(), "machine-token") ||
		strings.Contains(stderr.String(), "paired-adapter-token") {
		t.Fatalf("pairing output leaked or missed data: %s", stderr.String())
	}
	event := readFrame(t, client).(*protocol.Event)
	if event.Type != "session.message" || !strings.Contains(string(event.Payload), "paired pong") {
		t.Fatalf("paired event = %+v payload=%s", event, string(event.Payload))
	}
}

func TestRunWrapJSONStreamPublishesEventsToHub(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "events.db")
	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       dbPath,
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	stdin := strings.NewReader(strings.Join([]string{
		`{"type":"system","subtype":"init","session_id":"provider_ses"}`,
		`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"pong"}]}}`,
	}, "\n"))
	if err := runWithInput(ctx, []string{
		"wrap",
		"--hub", running.wsURL,
		"--session-id", "ses_local",
		"--adapter-token", "adapter-token",
		"--agent", "claude",
		"--jsonstream",
	}, stdin, io.Discard, io.Discard); err != nil {
		t.Fatalf("run wrap error = %v", err)
	}

	first := readFrame(t, client).(*protocol.Event)
	second := readFrame(t, client).(*protocol.Event)
	if first.Type != "session.state" || first.Seq == nil || *first.Seq != 1 {
		t.Fatalf("first event = %+v", first)
	}
	if second.Type != "session.message" || second.Seq == nil || *second.Seq != 2 || string(second.Payload) == "" {
		t.Fatalf("second event = %+v", second)
	}
}

func TestRunWrapMasksEventsWithSecretDir(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secretDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(secretDir, "provider_api"), []byte("secret-token"), 0o400); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	stdin := strings.NewReader(`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"use secret-token carefully"}]}}`)
	if err := runWithInput(ctx, []string{
		"wrap",
		"--hub", running.wsURL,
		"--session-id", "ses_local",
		"--adapter-token", "adapter-token",
		"--agent", "claude",
		"--jsonstream",
		"--secret-dir", secretDir,
	}, stdin, io.Discard, io.Discard); err != nil {
		t.Fatalf("run wrap error = %v", err)
	}

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Type != "session.message" {
		t.Fatalf("event type = %s, want session.message", ev.Type)
	}
	if strings.Contains(string(ev.Payload), "secret-token") || !strings.Contains(string(ev.Payload), "[MASKED]") {
		t.Fatalf("masked payload = %s", string(ev.Payload))
	}
}

func TestRunWrapACPPublishesEventsToHub(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	stdin := strings.NewReader(`{"method":"session/update","params":{"session_id":"acp_ses","update":{"type":"agent_message_chunk","text":"hello"}}}`)
	if err := runWithInput(ctx, []string{
		"wrap",
		"--hub", running.wsURL,
		"--session-id", "ses_local",
		"--adapter-token", "adapter-token",
		"--agent", "claude",
		"--acp",
	}, stdin, io.Discard, io.Discard); err != nil {
		t.Fatalf("run wrap error = %v", err)
	}

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Type != "session.message" || ev.Seq == nil || *ev.Seq != 1 {
		t.Fatalf("event = %+v", ev)
	}
}

func TestRunWrapProviderCommandWritesHubCommandsToProviderStdin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Setenv("AGENTWHARF_WRAP_HELPER", "1")

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	runDone := make(chan error, 1)
	go func() {
		runDone <- runWithInput(ctx, []string{
			"wrap",
			"--hub", running.wsURL,
			"--session-id", "ses_local",
			"--adapter-token", "adapter-token",
			"--agent", "claude",
			"--jsonstream",
			"--", os.Args[0],
		}, nil, io.Discard, io.Discard)
	}()

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_provider",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_local",
		Payload:   []byte(`{"content":[{"kind":"text","text":"ping"}]}`),
	})

	ackSeen := false
	replySeen := false
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline) && (!ackSeen || !replySeen); {
		frame := readFrame(t, client)
		switch typed := frame.(type) {
		case *protocol.CommandAck:
			if typed.CommandID == "cmd_provider" && typed.Status == protocol.AckAccepted {
				ackSeen = true
			}
		case *protocol.Event:
			if typed.Type == "session.message" && strings.Contains(string(typed.Payload), "provider saw cmd_provider") {
				replySeen = true
			}
		}
	}
	if !ackSeen || !replySeen {
		t.Fatalf("ackSeen=%v replySeen=%v", ackSeen, replySeen)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run wrap error = %v", err)
	}
}

func TestRunWrapACPProviderCommandSendsSessionPrompt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Setenv("AGENTWHARF_ACP_HELPER", "1")

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	runDone := make(chan error, 1)
	go func() {
		runDone <- runWithInput(ctx, []string{
			"wrap",
			"--hub", running.wsURL,
			"--session-id", "ses_local",
			"--adapter-token", "adapter-token",
			"--agent", "claude",
			"--acp",
			"--", os.Args[0],
		}, nil, io.Discard, io.Discard)
	}()

	ready := readFrame(t, client).(*protocol.Event)
	if ready.Type != "session.state" {
		t.Fatalf("ready event type = %s", ready.Type)
	}
	readyPayload := payloadObject(t, ready.Payload)
	if readyPayload["state"] != "ready" || readyPayload["provider_session_id"] != "acp_ses_1" {
		t.Fatalf("ready payload = %+v", readyPayload)
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_acp_prompt",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_local",
		Payload:   []byte(`{"content":[{"kind":"text","text":"ping"}]}`),
	})

	ackSeen := false
	replySeen := false
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline) && (!ackSeen || !replySeen); {
		frame := readFrame(t, client)
		switch typed := frame.(type) {
		case *protocol.CommandAck:
			if typed.CommandID == "cmd_acp_prompt" && typed.Status == protocol.AckAccepted {
				ackSeen = true
			}
		case *protocol.Event:
			if typed.Type == "session.message" && strings.Contains(string(typed.Payload), "acp saw ping") {
				replySeen = true
			}
		}
	}
	if !ackSeen || !replySeen {
		t.Fatalf("ackSeen=%v replySeen=%v", ackSeen, replySeen)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run wrap error = %v", err)
	}
}

func TestRunWrapACPProviderRoutesPermissionDecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Setenv("AGENTWHARF_ACP_PERMISSION_HELPER", "1")

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	runDone := make(chan error, 1)
	go func() {
		runDone <- runWithInput(ctx, []string{
			"wrap",
			"--hub", running.wsURL,
			"--session-id", "ses_local",
			"--adapter-token", "adapter-token",
			"--agent", "claude",
			"--acp",
			"--", os.Args[0],
		}, nil, io.Discard, io.Discard)
	}()

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_permission_prompt",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_local",
		Payload:   []byte(`{"content":[{"kind":"text","text":"needs permission"}]}`),
	})

	var requestID string
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline) && requestID == ""; {
		frame := readFrame(t, client)
		if event, ok := frame.(*protocol.Event); ok && event.Type == "permission.request" {
			payload := payloadObject(t, event.Payload)
			requestID, _ = payload["request_id"].(string)
		}
	}
	if requestID == "" {
		t.Fatal("permission.request not received")
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_permission_decision",
		Type:      protocol.CommandPermissionRespond,
		SessionID: "ses_local",
		Payload:   []byte(`{"request_id":"` + requestID + `","decision":"deny","decided_by":"usr_1","note":""}`),
	})

	decisionAckSeen := false
	replySeen := false
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline) && (!decisionAckSeen || !replySeen); {
		frame := readFrame(t, client)
		switch typed := frame.(type) {
		case *protocol.CommandAck:
			if typed.CommandID == "cmd_permission_decision" && typed.Status == protocol.AckAccepted {
				decisionAckSeen = true
			}
		case *protocol.Event:
			if typed.Type == "session.message" && strings.Contains(string(typed.Payload), "permission denied") {
				replySeen = true
			}
		}
	}
	if !decisionAckSeen || !replySeen {
		t.Fatalf("decisionAckSeen=%v replySeen=%v", decisionAckSeen, replySeen)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run wrap error = %v", err)
	}
}

func writeFrame(t *testing.T, conn *websocket.Conn, frame protocol.Frame) {
	t.Helper()

	data, err := protocol.Encode(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) protocol.Frame {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("websocket message type = %v, want text", typ)
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		t.Fatalf("decode frame %s: %v", string(data), err)
	}
	return frame
}

func TestMain(m *testing.M) {
	if os.Getenv("AGENTWHARF_ACP_PERMISSION_HELPER") == "1" {
		runWrapACPPermissionProviderHelper()
		return
	}
	if os.Getenv("AGENTWHARF_ACP_HELPER") == "1" {
		runWrapACPProviderHelper()
		return
	}
	if os.Getenv("AGENTWHARF_WRAP_HELPER") == "1" {
		runWrapProviderHelper()
		return
	}
	os.Exit(m.Run())
}

func runWrapProviderHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		os.Exit(2)
	}
	frame, err := protocol.Decode(scanner.Bytes())
	if err != nil {
		os.Exit(3)
	}
	cmd, ok := frame.(*protocol.Command)
	if !ok {
		os.Exit(4)
	}
	_, _ = fmt.Fprintf(os.Stdout, `{"type":"assistant","message":{"id":"reply_1","content":[{"type":"text","text":"provider saw %s"}]}}`+"\n", cmd.CommandID)
	os.Exit(0)
}

func runWrapACPProviderHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	init := readACPRequest(scanner)
	if init["method"] != "initialize" {
		os.Exit(10)
	}
	writeACPResponse(init["id"], map[string]any{
		"protocolVersion": 1,
		"agentInfo": map[string]any{
			"name":    "fake-acp",
			"version": "0.0.0",
		},
	})

	sessionNew := readACPRequest(scanner)
	if sessionNew["method"] != "session/new" {
		os.Exit(11)
	}
	writeACPResponse(sessionNew["id"], map[string]any{"sessionId": "acp_ses_1"})

	prompt := readACPRequest(scanner)
	if prompt["method"] != "session/prompt" {
		os.Exit(12)
	}
	params, _ := prompt["params"].(map[string]any)
	if params["sessionId"] != "acp_ses_1" {
		os.Exit(13)
	}
	items, _ := params["prompt"].([]any)
	if len(items) != 1 {
		os.Exit(14)
	}
	textPart, _ := items[0].(map[string]any)
	if textPart["text"] != "ping" {
		os.Exit(15)
	}

	fmt.Fprintln(os.Stdout, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"agent_message_chunk","messageId":"resp_1","content":{"type":"text","text":"acp saw ping"}}}}`)
	writeACPResponse(prompt["id"], map[string]any{"stopReason": "end_turn"})
	os.Exit(0)
}

func readACPRequest(scanner *bufio.Scanner) map[string]any {
	if !scanner.Scan() {
		os.Exit(20)
	}
	var message map[string]any
	if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
		os.Exit(21)
	}
	return message
}

func writeACPResponse(id any, result map[string]any) {
	encoded, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
	if err != nil {
		os.Exit(22)
	}
	fmt.Fprintln(os.Stdout, string(encoded))
}

func runWrapACPPermissionProviderHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	init := readACPRequest(scanner)
	if init["method"] != "initialize" {
		os.Exit(30)
	}
	writeACPResponse(init["id"], map[string]any{"protocolVersion": 1})

	sessionNew := readACPRequest(scanner)
	if sessionNew["method"] != "session/new" {
		os.Exit(31)
	}
	writeACPResponse(sessionNew["id"], map[string]any{"sessionId": "acp_ses_1"})

	prompt := readACPRequest(scanner)
	if prompt["method"] != "session/prompt" {
		os.Exit(32)
	}
	fmt.Fprintln(os.Stdout, `{"jsonrpc":"2.0","id":99,"method":"session/request_permission","params":{"sessionId":"acp_ses_1","action":"fs.write","riskLevel":"medium","summary":"Write a file","options":[{"kind":"reject","optionId":"reject_1"},{"kind":"allow","optionId":"allow_1"}]}}`)

	permissionResponse := readACPRequest(scanner)
	if fmt.Sprint(permissionResponse["id"]) != "99" {
		os.Exit(33)
	}
	result, _ := permissionResponse["result"].(map[string]any)
	outcome, _ := result["outcome"].(map[string]any)
	if outcome["outcome"] != "selected" || outcome["optionId"] != "reject_1" {
		os.Exit(34)
	}

	fmt.Fprintln(os.Stdout, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"agent_message_chunk","messageId":"resp_1","content":{"type":"text","text":"permission denied"}}}}`)
	writeACPResponse(prompt["id"], map[string]any{"stopReason": "end_turn"})
	os.Exit(0)
}

func payloadObject(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("decode payload %s: %v", string(payload), err)
	}
	return out
}
