package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/auth/static"
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
	metricsResponse, err := http.Get("http://" + running.addr + "/adapter/metrics")
	if err != nil {
		t.Fatalf("public adapter metrics request: %v", err)
	}
	defer metricsResponse.Body.Close()
	if metricsResponse.StatusCode == http.StatusOK {
		t.Fatalf("public adapter metrics unexpectedly reachable")
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

func TestStartWrapDiagnosticsRequiresFixedEntryProviderProof(t *testing.T) {
	if server := startWrapDiagnostics(context.Background(), wrapConfig{}, core.NewAdapterMetrics()); server != nil {
		t.Fatal("diagnostics enabled without fixed-entry proof")
	}
	root, err := os.MkdirTemp("/tmp", "aw-wrap-diag-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	marker := filepath.Join(root, "health")
	if err := os.WriteFile(marker, []byte("healthy"), 0o600); err != nil {
		t.Fatal(err)
	}
	providerUID := uint32(os.Geteuid()) + 1
	cfg := wrapConfig{
		HealthMarker:       marker,
		ProviderCredential: &core.ProcessCredential{UID: providerUID, GID: providerUID},
	}
	server := startWrapDiagnostics(context.Background(), cfg, core.NewAdapterMetrics())
	if server == nil {
		t.Fatal("diagnostics proof was not accepted")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close diagnostics: %v", err)
	}
	cfg.ProviderCredential.UID = uint32(os.Geteuid())
	if server := startWrapDiagnostics(context.Background(), cfg, core.NewAdapterMetrics()); server != nil {
		t.Fatal("diagnostics enabled for same-UID provider")
	}
}

func TestACPFileReferenceReadsBoundedWorkspaceContent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("package main\n")
	if err := os.WriteFile(filepath.Join(root, "main.go"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	payload := fmt.Sprintf(`{"content":[{"kind":"text","text":"review"},{"kind":"file_reference","disposition":"file","path":"main.go","version":"v1","content_digest":"sha256:%x","bytes":%d,"media_type":"text/plain"}],"capability_fingerprint":"sha256:%064d"}`, digest, len(content), 1)
	prompt, err := acpPromptFromSessionSendAtRoot([]byte(payload), root)
	if err != nil {
		t.Fatalf("acpPromptFromSessionSendAtRoot() error = %v", err)
	}
	if len(prompt) != 2 || prompt[1]["type"] != "text" || prompt[1]["text"] != string(content) {
		t.Fatalf("prompt = %+v", prompt)
	}
}

func TestACPFileReferenceFailsClosedOnDigestAndSymlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	payload := `{"content":[{"kind":"file_reference","disposition":"file","path":"link.txt","version":"v1","content_digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","bytes":7,"media_type":"text/plain"}],"capability_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	if _, err := acpPromptFromSessionSendAtRoot([]byte(payload), root); err == nil {
		t.Fatal("symlink/digest reference unexpectedly accepted")
	}
}

func TestTaskClaimCodeInputRequiresBoundedNonTTYStdin(t *testing.T) {
	t.Parallel()

	code, err := readClaimCode(strings.NewReader("claim-code-1\n"))
	if err != nil || string(code) != "claim-code-1" {
		t.Fatalf("readClaimCode() = %q, %v", string(code), err)
	}
	if _, err := readClaimCode(strings.NewReader(strings.Repeat("x", 257))); err == nil {
		t.Fatal("oversized claim code unexpectedly accepted")
	}
	if err := runTaskCommand(context.Background(), []string{"claim", "claim_1"}, strings.NewReader("code\n"), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("runTaskCommand without --code-stdin = %v", err)
	}
}

func TestClaimLaunchRetryClassifierDistinguishesTransportFailure(t *testing.T) {
	t.Parallel()

	if claimLaunchRequiresReclaim(errors.New("read hello ack: websocket EOF")) {
		t.Fatal("transport hello failure must permit one bounded retry")
	}
	for _, message := range []string{"invalid hello ack", "unauthorized adapter", "credential revoked"} {
		if !claimLaunchRequiresReclaim(errors.New(message)) {
			t.Fatalf("claimLaunchRequiresReclaim(%q) = false", message)
		}
	}
	for _, frame := range []*protocol.Error{
		{Code: "unauthorized", Fatal: true},
		{Code: "invalid_hello", Fatal: true},
		{Code: "credential_expired", Fatal: true},
	} {
		if !claimProtocolErrorRequiresReclaim(frame) || !claimLaunchRequiresReclaim(fmt.Errorf("%w: %s", errClaimAuthRejection, frame.Code)) {
			t.Fatalf("protocol error %q was not classified as terminal reclaim", frame.Code)
		}
	}
	if claimProtocolErrorRequiresReclaim(&protocol.Error{Code: "temporary", Fatal: false}) {
		t.Fatal("non-fatal protocol error must not force reclaim")
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

func TestStartServeFailsClosedWithoutSessionCredentialSigner(t *testing.T) {
	t.Setenv("AGENTWHARF_SESSION_CREDENTIAL_SIGNER_KEY_FILE", "")
	_, err := startServe(context.Background(), serveConfig{
		Addr: "127.0.0.1:0", DBPath: filepath.Join(t.TempDir(), "events.db"),
		SessionID: "ses_local", Provider: "claude-code", ControlToken: "control-token", AdapterToken: "adapter-token",
	})
	if err == nil || !strings.Contains(err.Error(), "session credential signer") {
		t.Fatalf("startServe() error = %v, want missing signer failure", err)
	}
}

func TestStartServeRejectsUnsafeSessionCredentialSignerFile(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "signer-key")
	if err := os.WriteFile(keyFile, []byte("test-only-local-session-credential-signer"), 0o644); err != nil {
		t.Fatalf("write signer key: %v", err)
	}
	if err := os.Chmod(keyFile, 0o644); err != nil {
		t.Fatalf("chmod signer key: %v", err)
	}
	_, err := startServe(context.Background(), serveConfig{
		Addr: "127.0.0.1:0", DBPath: filepath.Join(t.TempDir(), "events.db"),
		SessionID: "ses_local", Provider: "claude-code", ControlToken: "control-token", AdapterToken: "adapter-token",
		SessionCredentialSignerKeyFile: keyFile,
	})
	if err == nil || !strings.Contains(err.Error(), "private and regular") {
		t.Fatalf("startServe() error = %v, want unsafe signer failure", err)
	}
}

func TestStartServeRejectsSessionCredentialSignerSymlink(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "signer-key")
	if err := os.WriteFile(keyFile, []byte("test-only-local-session-credential-signer"), 0o600); err != nil {
		t.Fatalf("write signer key: %v", err)
	}
	link := filepath.Join(t.TempDir(), "signer-link")
	if err := os.Symlink(keyFile, link); err != nil {
		t.Fatalf("symlink signer key: %v", err)
	}
	_, err := startServe(context.Background(), serveConfig{
		Addr: "127.0.0.1:0", DBPath: filepath.Join(t.TempDir(), "events.db"),
		SessionID: "ses_local", Provider: "claude-code", ControlToken: "control-token", AdapterToken: "adapter-token",
		SessionCredentialSignerKeyFile: link,
	})
	if err == nil || !strings.Contains(err.Error(), "private and regular") {
		t.Fatalf("startServe() error = %v, want symlink signer rejection", err)
	}
}

func TestParseServeConfigRejectsNonPositiveSessionCredentialSignerKeyVersion(t *testing.T) {
	for _, version := range []string{"0", "-1"} {
		if _, err := parseServeConfig([]string{"--session-credential-signer-key-version", version}, io.Discard); err == nil || !strings.Contains(err.Error(), "must be positive") {
			t.Fatalf("parseServeConfig(%s) error = %v, want non-positive signer rejection", version, err)
		}
	}
}

func TestLocalSessionAuthenticatorAcceptsOnlyCurrentLocalSessionBearer(t *testing.T) {
	issuer, err := auth.NewLocalSessionCredentialIssuer([]byte("test-only-local-session-credential-signer"), 1)
	if err != nil {
		t.Fatalf("new issuer: %v", err)
	}
	prepared, err := issuer.PrepareSessionCredential(context.Background(), auth.SessionCredentialRequest{
		SessionID: "ses_local", Lineage: auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetAttach, AttachID: "attach_1", JTI: "jti_1"},
		Generation: 1, RotationID: "rotation_1", RevocationID: "revocation_1", ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("prepare credential: %v", err)
	}
	if err := issuer.ActivateSessionCredential(context.Background(), prepared); err != nil {
		t.Fatalf("activate credential: %v", err)
	}
	base := static.New([]static.Token{{Token: "adapter-token", Subject: "adapter", Scopes: []auth.Scope{auth.SessionAdapter("ses_local")}}})
	authenticator := localSessionAuthenticator{Authenticator: base, staticAdapterCredential: base.AdapterCredential, sessionCredentialIssuer: issuer, sessionID: "ses_local"}
	principal, err := authenticator.Authenticate(context.Background(), prepared.Bearer)
	if err != nil || len(principal.Scopes) != 1 || principal.Scopes[0] != auth.SessionAdapter("ses_local") {
		t.Fatalf("Authenticate(prepared bearer) = %+v, %v", principal, err)
	}
	if _, err := authenticator.Authenticate(context.Background(), "unknown"); err == nil {
		t.Fatal("unknown local bearer unexpectedly authenticated")
	}
	generation, expiresAt, initialize, err := authenticator.AdapterCredential(context.Background(), prepared.Bearer, principal, "ses_local")
	if err != nil || generation != prepared.Generation || expiresAt != prepared.ExpiresAt.UnixNano() || !initialize {
		t.Fatalf("AdapterCredential() = %d, %d, %t, %v", generation, expiresAt, initialize, err)
	}
}

func TestRunUsageMentionsWharfEntrypoint(t *testing.T) {
	t.Parallel()

	err := run(context.Background(), nil, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "usage: wharf serve|wrap|claude|codex|gemini|logout|machine|attention-backfill [options]") {
		t.Fatalf("run() error = %v, want wharf usage", err)
	}
	if strings.Contains(err.Error(), "usage: agentwharf") {
		t.Fatalf("run() error = %v, must not mention legacy agentwharf entrypoint", err)
	}
}

func TestAttentionBackfillRequiresOperatorArguments(t *testing.T) {
	if err := runWithInput(context.Background(), []string{"attention-backfill"}, nil, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "usage: wharf attention-backfill") {
		t.Fatalf("missing checkpoint error = %v", err)
	}
	t.Setenv("AGENTWHARF_POSTGRES_DSN", "")
	if err := runWithInput(context.Background(), []string{"attention-backfill", "--checkpoint", "/tmp/checkpoint"}, nil, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "AGENTWHARF_POSTGRES_DSN is required") {
		t.Fatalf("missing DSN error = %v", err)
	}
	t.Setenv("AGENTWHARF_POSTGRES_DSN", "not-a-dsn-secret-marker")
	if err := runWithInput(context.Background(), []string{"attention-backfill", "--checkpoint", "/tmp/checkpoint"}, nil, io.Discard, io.Discard); err == nil || err.Error() != "open attention backfill postgres pool" {
		t.Fatalf("invalid DSN error = %v", err)
	}
	if err := runWithInput(context.Background(), []string{"attention-backfill", "--postgres-dsn", "postgres://secret@example"}, nil, io.Discard, io.Discard); err == nil || strings.Contains(err.Error(), "postgres://secret@example") {
		t.Fatalf("legacy DSN argument error = %v", err)
	}
}

func TestMachinePairingDisplayURLUsesConsoleMachinesRoute(t *testing.T) {
	t.Parallel()

	got := machinePairingDisplayURL("https://api.cloud.example/v1", "https://cloud.superwhv.me/machines/pair?device=debug#step")
	if got != "https://cloud.superwhv.me/app/machines" {
		t.Fatalf("machinePairingDisplayURL() = %q, want Console machines route", got)
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

func TestParseAgentEntrypointDefaultsToManagedClaudeSession(t *testing.T) {
	cfg, err := parseAgentEntrypointConfig("claude", nil, io.Discard)
	if err != nil {
		t.Fatalf("parseAgentEntrypointConfig() error = %v", err)
	}
	if cfg.Agent != "claude" ||
		cfg.Provider != "claude-code" ||
		cfg.Format != "acp" ||
		!cfg.Managed ||
		cfg.Pair ||
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
		cfg.Managed ||
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
	if !cfg.Managed || cfg.Pair || cfg.CloudAPIURL != defaultManagedCloudAPIURL {
		t.Fatalf("agent entrypoint config = %+v, want managed session", cfg)
	}
}

func TestMachineCredentialStoreSavesLoadsAndDeletes(t *testing.T) {
	credentialFile := filepath.Join(t.TempDir(), "state", "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

	credential := machineCredential{
		MachineID:    "machine_1",
		MachineToken: "machine-token",
		CloudAPIURL:  "https://cloud.superwhv.example/v1",
		HubWSURL:     "wss://hub.superwhv.example/ws",
		ExpiresAt:    "2026-06-19T10:00:00Z",
	}
	if err := saveMachineCredential(credential); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	loaded, err := loadMachineCredential()
	if err != nil {
		t.Fatalf("loadMachineCredential() error = %v", err)
	}
	if loaded.MachineID != credential.MachineID ||
		loaded.MachineToken != credential.MachineToken ||
		loaded.CloudAPIURL != credential.CloudAPIURL ||
		loaded.HubWSURL != credential.HubWSURL ||
		loaded.ExpiresAt != credential.ExpiresAt ||
		loaded.CreatedAt == "" {
		t.Fatalf("loaded credential = %+v", loaded)
	}
	if runtime.GOOS != "windows" {
		dirInfo, err := os.Stat(filepath.Dir(credentialFile))
		if err != nil {
			t.Fatalf("stat credential dir: %v", err)
		}
		if dirInfo.Mode().Perm() != 0o700 {
			t.Fatalf("credential dir mode = %o, want 0700", dirInfo.Mode().Perm())
		}
		fileInfo, err := os.Stat(credentialFile)
		if err != nil {
			t.Fatalf("stat credential file: %v", err)
		}
		if fileInfo.Mode().Perm() != 0o600 {
			t.Fatalf("credential file mode = %o, want 0600", fileInfo.Mode().Perm())
		}
	}

	if err := deleteMachineCredential(); err != nil {
		t.Fatalf("deleteMachineCredential() error = %v", err)
	}
	if _, err := loadMachineCredential(); !errors.Is(err, errMachineCredentialNotFound) {
		t.Fatalf("load deleted credential error = %v, want errMachineCredentialNotFound", err)
	}
}

func TestMachineCredentialStoreRejectsMalformedCredential(t *testing.T) {
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)
	if err := os.WriteFile(credentialFile, []byte(`{"machine_id":"machine_1"}`), 0o600); err != nil {
		t.Fatalf("write malformed credential: %v", err)
	}

	_, err := loadMachineCredential()
	if err == nil || errors.Is(err, errMachineCredentialNotFound) {
		t.Fatalf("loadMachineCredential() error = %v, want malformed credential error", err)
	}
}

func TestRunWrapPairingCreatesMachineSessionAndPublishesEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

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
			fmt.Fprint(w, `{"data":{"device_code":"device-code-1","user_code":"ABCD-E","verification_uri":"https://cloud.example/machines/pair","expires_at":"2026-06-18T10:10:00Z","interval_seconds":1}}`)
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
	if !strings.Contains(stderr.String(), "https://cloud.example/app/machines") ||
		!strings.Contains(stderr.String(), "device-code-1") ||
		!strings.Contains(stderr.String(), "ABCD-E") ||
		strings.Contains(stderr.String(), "machine-token") ||
		strings.Contains(stderr.String(), "paired-adapter-token") {
		t.Fatalf("pairing output leaked or missed data: %s", stderr.String())
	}
	credential, err := loadMachineCredential()
	if err != nil {
		t.Fatalf("load stored machine credential: %v", err)
	}
	if credential.MachineID != "machine_1" ||
		credential.MachineToken != "machine-token" ||
		credential.CloudAPIURL != controlPlane.URL ||
		credential.HubWSURL != "wss://ignored.example/ws" ||
		credential.ExpiresAt != "2026-06-19T10:00:00Z" {
		t.Fatalf("stored machine credential = %+v", credential)
	}
	event := readFrame(t, client).(*protocol.Event)
	if event.Type != "session.message" || !strings.Contains(string(event.Payload), "paired pong") {
		t.Fatalf("paired event = %+v payload=%s", event, string(event.Payload))
	}
}

func TestManagedWrapPairsAndStoresMachineCredential(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_managed",
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

	var pairingRequests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/machine-pairing-codes":
			pairingRequests++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"data":{"device_code":"device-code-managed","user_code":"MNGD-1","verification_uri":"https://cloud.superwhv.me/machines/pair","expires_at":"2026-06-18T10:10:00Z","interval_seconds":1}}`)
		case "/machine-pairing-codes/token":
			fmt.Fprint(w, `{"data":{"machine":{"id":"machine_managed"},"machine_token":"machine-token-managed","hub_ws_url":"wss://ignored.example/ws","expires_at":"2026-06-19T10:00:00Z"}}`)
		case "/machine-sessions":
			if r.Header.Get("Authorization") != "Bearer machine-token-managed" {
				t.Fatalf("machine session authorization = %q", r.Header.Get("Authorization"))
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"data":{"session":{"id":"ses_managed","host_type":"machine","host_id":"machine_managed","provider":"claude-code","status":"starting"},"hub_ws_url":%q,"adapter_token":"paired-adapter-token","expires_at":"2026-06-18T10:15:00Z"}}`, running.wsURL)
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	stderr := new(strings.Builder)
	stdin := strings.NewReader(`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"managed pong"}]}}`)
	_, err = runWrap(ctx, wrapConfig{
		Managed:     true,
		CloudAPIURL: controlPlane.URL,
		Agent:       "claude",
		Provider:    "claude-code",
		Format:      "jsonstream",
	}, stdin, stderr)
	if err != nil {
		t.Fatalf("runWrap() error = %v", err)
	}
	if pairingRequests != 1 {
		t.Fatalf("pairing requests = %d, want 1", pairingRequests)
	}
	if !strings.Contains(stderr.String(), "https://cloud.superwhv.me/app/machines") ||
		strings.Contains(stderr.String(), "machine-token-managed") ||
		strings.Contains(stderr.String(), "paired-adapter-token") {
		t.Fatalf("managed pairing output leaked or missed data: %s", stderr.String())
	}
	credential, err := loadMachineCredential()
	if err != nil {
		t.Fatalf("load stored machine credential: %v", err)
	}
	if credential.MachineID != "machine_managed" ||
		credential.MachineToken != "machine-token-managed" ||
		credential.CloudAPIURL != controlPlane.URL {
		t.Fatalf("stored machine credential = %+v", credential)
	}
}

func TestManagedWrapRetriesTransientPairingCreateFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

	var pairingRequests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/machine-pairing-codes":
			pairingRequests++
			if pairingRequests == 1 {
				http.Error(w, `{"error":{"message":"temporarily unavailable"}}`, http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"data":{"device_code":"device-code-retry","user_code":"RETRY-1","verification_uri":"https://cloud.superwhv.me/machines/pair","expires_at":"2026-06-18T10:10:00Z","interval_seconds":1}}`)
		case "/machine-pairing-codes/token":
			fmt.Fprint(w, `{"data":{"machine":{"id":"machine_retry"},"machine_token":"retry-machine-token","hub_ws_url":"wss://ignored.example/ws","expires_at":"2026-06-19T10:00:00Z"}}`)
		case "/machine-sessions":
			if r.Header.Get("Authorization") != "Bearer retry-machine-token" {
				t.Fatalf("machine session authorization = %q", r.Header.Get("Authorization"))
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"data":{"session":{"id":"ses_retry","host_type":"machine","host_id":"machine_retry","provider":"claude-code","status":"starting"},"hub_ws_url":"wss://hub.example/ws","adapter_token":"retry-adapter-token","expires_at":"2026-06-18T10:15:00Z"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	cfg, err := prepareManagedWrapSession(ctx, wrapConfig{
		Managed:     true,
		CloudAPIURL: controlPlane.URL,
		Agent:       "claude",
		Provider:    "claude-code",
	}, io.Discard)
	if err != nil {
		t.Fatalf("prepareManagedWrapSession() error = %v", err)
	}
	if pairingRequests != 2 {
		t.Fatalf("pairing requests = %d, want 2", pairingRequests)
	}
	if cfg.SessionID != "ses_retry" || cfg.HubURL != "wss://hub.example/ws" || cfg.AdapterToken != "retry-adapter-token" {
		t.Fatalf("managed config = %+v", cfg)
	}
}

func TestManagedWrapPersistentTransientStatusMentionsProxyRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)
	t.Setenv("http_proxy", "http://127.0.0.1:1082")

	var pairingRequests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/machine-pairing-codes" {
			t.Fatalf("unexpected cloud api path %s", r.URL.Path)
		}
		pairingRequests++
		http.Error(w, `{"error":{"message":"Service Unavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer controlPlane.Close()

	_, err := prepareManagedWrapSession(ctx, wrapConfig{
		Managed:     true,
		CloudAPIURL: controlPlane.URL,
		Agent:       "claude",
		Provider:    "claude-code",
	}, io.Discard)
	if err == nil {
		t.Fatal("prepareManagedWrapSession() error = nil, want transient status error")
	}
	if pairingRequests < 2 {
		t.Fatalf("pairing requests = %d, want retry before failing", pairingRequests)
	}
	message := err.Error()
	for _, want := range []string{
		"create machine pairing code: cloud api returned status 503",
		"http_proxy",
		"env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY wharf claude",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("error = %q, want substring %q", message, want)
		}
	}
	if strings.Contains(message, "127.0.0.1:1082") {
		t.Fatalf("error leaked proxy endpoint: %q", message)
	}
}

func TestPostCloudAPIJSONWithRetryRetriesNetworkFailureAndMentionsProxyRecovery(t *testing.T) {
	t.Setenv("https_proxy", "http://127.0.0.1:1082")

	var attempts int
	client := &http.Client{
		Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			attempts++
			return nil, errors.New("synthetic service unavailable")
		}),
	}

	_, _, err := postCloudAPIJSONWithRetry(context.Background(), client, "https://cloud.superwhv.example/v1/machine-pairing-codes", "secret-token", map[string]string{
		"platform": "darwin-arm64",
	})
	if err == nil {
		t.Fatal("postCloudAPIJSONWithRetry() error = nil, want network error")
	}
	if attempts < 2 {
		t.Fatalf("attempts = %d, want retry before failing", attempts)
	}
	message := err.Error()
	for _, want := range []string{
		"post cloud api request",
		"https_proxy",
		"env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY wharf claude",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("error = %q, want substring %q", message, want)
		}
	}
	for _, leaked := range []string{"secret-token", "127.0.0.1:1082"} {
		if strings.Contains(message, leaked) {
			t.Fatalf("error leaked %q: %q", leaked, message)
		}
	}
}

func TestPostCloudAPIJSONDoesNotRetryByDefault(t *testing.T) {
	var attempts int
	client := &http.Client{
		Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			attempts++
			return nil, errors.New("synthetic network error")
		}),
	}

	_, _, err := postCloudAPIJSON(context.Background(), client, "https://cloud.superwhv.example/v1/machine-sessions", "secret-token", map[string]string{
		"provider": "claude-code",
	})
	if err == nil {
		t.Fatal("postCloudAPIJSON() error = nil, want network error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 for default Cloud API POST", attempts)
	}
}

func TestManagedWrapReusesStoredMachineCredential(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_reused",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "reused-adapter-token",
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

	var sessionRequests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/machine-sessions" {
			t.Fatalf("unexpected cloud api path %s", r.URL.Path)
		}
		sessionRequests++
		if r.Header.Get("Authorization") != "Bearer stored-machine-token" {
			t.Fatalf("machine session authorization = %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"data":{"session":{"id":"ses_reused","host_type":"machine","host_id":"machine_reused","provider":"claude-code","status":"starting"},"hub_ws_url":%q,"adapter_token":"reused-adapter-token","expires_at":"2026-06-18T10:15:00Z"}}`, running.wsURL)
	}))
	defer controlPlane.Close()

	if err := saveMachineCredential(machineCredential{
		MachineID:    "machine_reused",
		MachineToken: "stored-machine-token",
		CloudAPIURL:  controlPlane.URL,
	}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	stderr := new(strings.Builder)
	stdin := strings.NewReader(`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"reused pong"}]}}`)
	_, err = runWrap(ctx, wrapConfig{
		Managed:     true,
		CloudAPIURL: controlPlane.URL,
		Agent:       "claude",
		Provider:    "claude-code",
		Format:      "jsonstream",
	}, stdin, stderr)
	if err != nil {
		t.Fatalf("runWrap() error = %v", err)
	}
	if sessionRequests != 1 {
		t.Fatalf("machine session requests = %d, want 1", sessionRequests)
	}
	if strings.Contains(stderr.String(), "Pair this machine") ||
		strings.Contains(stderr.String(), "device_code") ||
		strings.Contains(stderr.String(), "user_code") {
		t.Fatalf("reuse path printed pairing prompt: %s", stderr.String())
	}
}

func TestManagedWrapForcePairOverwritesStoredMachineCredential(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_repaired",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "new-adapter-token",
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

	if err := saveMachineCredential(machineCredential{
		MachineID:    "machine_old",
		MachineToken: "old-machine-token",
		CloudAPIURL:  "https://old-cloud.example/v1",
	}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	var pairingRequests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/machine-pairing-codes":
			pairingRequests++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"data":{"device_code":"device-code-new","user_code":"NEW1-PAIR","verification_uri":"https://cloud.example/machines/pair","expires_at":"2026-06-18T10:10:00Z","interval_seconds":1}}`)
		case "/machine-pairing-codes/token":
			fmt.Fprint(w, `{"data":{"machine":{"id":"machine_new"},"machine_token":"new-machine-token","hub_ws_url":"wss://ignored.example/ws","expires_at":"2026-06-19T10:00:00Z"}}`)
		case "/machine-sessions":
			if r.Header.Get("Authorization") != "Bearer new-machine-token" {
				t.Fatalf("machine session authorization = %q", r.Header.Get("Authorization"))
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"data":{"session":{"id":"ses_repaired","host_type":"machine","host_id":"machine_new","provider":"claude-code","status":"starting"},"hub_ws_url":%q,"adapter_token":"new-adapter-token","expires_at":"2026-06-18T10:15:00Z"}}`, running.wsURL)
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	_, err = runWrap(ctx, wrapConfig{
		Managed:     true,
		Pair:        true,
		CloudAPIURL: controlPlane.URL,
		Agent:       "claude",
		Provider:    "claude-code",
		Format:      "jsonstream",
	}, strings.NewReader(`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"new pong"}]}}`), io.Discard)
	if err != nil {
		t.Fatalf("runWrap() error = %v", err)
	}
	if pairingRequests != 1 {
		t.Fatalf("pairing requests = %d, want 1", pairingRequests)
	}
	credential, err := loadMachineCredential()
	if err != nil {
		t.Fatalf("load stored machine credential: %v", err)
	}
	if credential.MachineID != "machine_new" ||
		credential.MachineToken != "new-machine-token" ||
		credential.CloudAPIURL != controlPlane.URL {
		t.Fatalf("stored machine credential = %+v", credential)
	}
}

func TestManagedWrapRepairsRevokedMachineCredential(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
		SessionID:    "ses_repaired",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "repaired-adapter-token",
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

	controlPlaneURL := ""
	var sessionRequests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/machine-sessions":
			sessionRequests++
			switch sessionRequests {
			case 1:
				if r.Header.Get("Authorization") != "Bearer stale-machine-token" {
					t.Fatalf("first machine session authorization = %q", r.Header.Get("Authorization"))
				}
				http.Error(w, `{"error":"machine revoked"}`, http.StatusUnauthorized)
			case 2:
				if r.Header.Get("Authorization") != "Bearer repaired-machine-token" {
					t.Fatalf("second machine session authorization = %q", r.Header.Get("Authorization"))
				}
				w.WriteHeader(http.StatusCreated)
				fmt.Fprintf(w, `{"data":{"session":{"id":"ses_repaired","host_type":"machine","host_id":"machine_repaired","provider":"claude-code","status":"starting"},"hub_ws_url":%q,"adapter_token":"repaired-adapter-token","expires_at":"2026-06-18T10:15:00Z"}}`, running.wsURL)
			default:
				t.Fatalf("unexpected machine session request %d", sessionRequests)
			}
		case "/machine-pairing-codes":
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"data":{"device_code":"device-code-repair","user_code":"RPR1-PAIR","verification_uri":"https://cloud.example/machines/pair","expires_at":"2026-06-18T10:10:00Z","interval_seconds":1}}`)
		case "/machine-pairing-codes/token":
			fmt.Fprint(w, `{"data":{"machine":{"id":"machine_repaired"},"machine_token":"repaired-machine-token","hub_ws_url":"wss://ignored.example/ws","expires_at":"2026-06-19T10:00:00Z"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()
	controlPlaneURL = controlPlane.URL

	if err := saveMachineCredential(machineCredential{
		MachineID:    "machine_stale",
		MachineToken: "stale-machine-token",
		CloudAPIURL:  controlPlaneURL,
	}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	stderr := new(strings.Builder)
	_, err = runWrap(ctx, wrapConfig{
		Managed:     true,
		CloudAPIURL: controlPlaneURL,
		Agent:       "claude",
		Provider:    "claude-code",
		Format:      "jsonstream",
	}, strings.NewReader(`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"repaired pong"}]}}`), stderr)
	if err != nil {
		t.Fatalf("runWrap() error = %v", err)
	}
	if sessionRequests != 2 {
		t.Fatalf("machine session requests = %d, want 2", sessionRequests)
	}
	if !strings.Contains(stderr.String(), "Local machine pairing is no longer valid; pairing again.") ||
		!strings.Contains(stderr.String(), "device-code-repair") {
		t.Fatalf("repair output = %s", stderr.String())
	}
	credential, err := loadMachineCredential()
	if err != nil {
		t.Fatalf("load stored machine credential: %v", err)
	}
	if credential.MachineID != "machine_repaired" ||
		credential.MachineToken != "repaired-machine-token" {
		t.Fatalf("stored machine credential = %+v", credential)
	}
}

func TestManagedWrapDoesNotRepairAccessError(t *testing.T) {
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)

	var sessionRequests int
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/machine-sessions" {
			t.Fatalf("unexpected cloud api path %s", r.URL.Path)
		}
		sessionRequests++
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error":{"code":"forbidden","message":"machine access required"}}`)
	}))
	defer controlPlane.Close()

	if err := saveMachineCredential(machineCredential{
		MachineID:    "machine_no_access",
		MachineToken: "stored-machine-token",
		CloudAPIURL:  controlPlane.URL,
	}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	_, err := runWrap(context.Background(), wrapConfig{
		Managed:     true,
		CloudAPIURL: controlPlane.URL,
		Agent:       "claude",
		Provider:    "claude-code",
		Format:      "jsonstream",
	}, strings.NewReader(""), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "machine access required") {
		t.Fatalf("runWrap() error = %v, want machine access required", err)
	}
	if sessionRequests != 1 {
		t.Fatalf("machine session requests = %d, want 1", sessionRequests)
	}
	credential, err := loadMachineCredential()
	if err != nil {
		t.Fatalf("load stored machine credential: %v", err)
	}
	if credential.MachineToken != "stored-machine-token" {
		t.Fatalf("stored machine credential = %+v", credential)
	}
}

func TestRunLogoutRemovesMachineCredential(t *testing.T) {
	credentialFile := filepath.Join(t.TempDir(), "machine.json")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", credentialFile)
	if err := saveMachineCredential(machineCredential{
		MachineID:    "machine_1",
		MachineToken: "machine-token",
		CloudAPIURL:  "https://cloud.superwhv.example/v1",
	}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}

	stdout := new(strings.Builder)
	if err := runWithInput(context.Background(), []string{"logout"}, nil, stdout, io.Discard); err != nil {
		t.Fatalf("run logout error = %v", err)
	}
	if _, err := loadMachineCredential(); !errors.Is(err, errMachineCredentialNotFound) {
		t.Fatalf("load deleted credential error = %v, want errMachineCredentialNotFound", err)
	}
	if !strings.Contains(stdout.String(), "local machine pairing removed") {
		t.Fatalf("logout output = %s", stdout.String())
	}

	stdout.Reset()
	if err := runWithInput(context.Background(), []string{"machine", "unlink"}, nil, stdout, io.Discard); err != nil {
		t.Fatalf("run machine unlink error = %v", err)
	}
	if !strings.Contains(stdout.String(), "no local machine pairing found") {
		t.Fatalf("machine unlink output = %s", stdout.String())
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

func TestRunWrapV2ProviderDoesNotStartAfterAdmissionRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	startSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		frame, err := readFrameFromConn(ctx, conn)
		if err != nil {
			return
		}
		hello, ok := frame.(*protocol.Hello)
		if !ok || hello.ProtocolVersion != protocol.ProtocolVersionV2 {
			t.Errorf("hello = %+v", frame)
			return
		}
		if err := writeFrameToConn(ctx, conn, &protocol.HelloAck{ProtocolVersion: protocol.ProtocolVersionV2, Sessions: []protocol.SessionSummary{{SessionID: "ses_v2", Provider: "claude-code"}}, ConnectionAuthority: &protocol.ConnectionAuthorityReceipt{SessionID: "ses_v2", ConnectionEpoch: 1, CredentialGeneration: 1, AcceptedFence: 1, WriterLeaseID: "lease_v2", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}}); err != nil {
			return
		}
		frame, err = readProviderStartFrame(ctx, conn)
		if err != nil {
			return
		}
		start, ok := frame.(*protocol.ProviderStart)
		if !ok || start.Attempt != 1 {
			t.Errorf("provider start frame = %T", frame)
			return
		}
		startSeen <- struct{}{}
		if err := writeFrameToConn(ctx, conn, &protocol.ProviderStartPrepare{Attempt: start.Attempt}); err != nil {
			return
		}
		if _, err := readFrameFromConn(ctx, conn); err != nil {
			return
		}
		_ = writeFrameToConn(ctx, conn, &protocol.ProviderStartAck{Attempt: start.Attempt, Status: protocol.ProviderStartRejected})
	}))
	defer server.Close()
	_, err := runWrap(ctx, wrapConfig{HubURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "ses_v2", Provider: "claude-code", AdapterToken: "adapter-token", Format: "jsonstream", ProtocolVersion: protocol.ProtocolVersionV2, ProviderCommand: []string{os.Args[0]}}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "provider start admission rejected") {
		t.Fatalf("runWrap(v2 rejected start) error = %v", err)
	}
	select {
	case <-startSeen:
	case <-time.After(time.Second):
		t.Fatal("v2 provider start request was not observed")
	}
}

func TestRunWrapV2ProviderStopsAfterFinalAdmissionRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "provider-state")
	t.Setenv("AGENTWHARF_START_BLOCK_HELPER", "1")
	t.Setenv("AGENTWHARF_START_BLOCK_MARKER", marker)
	proofSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if _, err := readFrameFromConn(ctx, conn); err != nil {
			return
		}
		if err := writeFrameToConn(ctx, conn, &protocol.HelloAck{ProtocolVersion: protocol.ProtocolVersionV2, Sessions: []protocol.SessionSummary{{SessionID: "ses_v2", Provider: "claude-code"}}, ConnectionAuthority: &protocol.ConnectionAuthorityReceipt{SessionID: "ses_v2", ConnectionEpoch: 1, CredentialGeneration: 1, AcceptedFence: 1, WriterLeaseID: "lease_v2", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}}); err != nil {
			return
		}
		if frame, err := readProviderStartFrame(ctx, conn); err != nil || frame.FrameName() != protocol.FrameProviderStart {
			t.Errorf("provider start request = %T, %v", frame, err)
			return
		}
		if err := writeFrameToConn(ctx, conn, &protocol.ProviderStartPrepare{Attempt: 1}); err != nil {
			return
		}
		if frame, err := readFrameFromConn(ctx, conn); err != nil || frame.FrameName() != protocol.FrameProviderStartStarted {
			t.Errorf("provider start proof = %T, %v", frame, err)
			return
		}
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if content, err := os.ReadFile(marker); err == nil && string(content) == "ready" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if content, _ := os.ReadFile(marker); string(content) != "ready" {
			t.Errorf("provider did not become ready before final rejection: %q", content)
			return
		}
		proofSeen <- struct{}{}
		_ = writeFrameToConn(ctx, conn, &protocol.ProviderStartAck{Attempt: 1, Status: protocol.ProviderStartRejected})
	}))
	defer server.Close()
	_, err := runWrap(ctx, wrapConfig{HubURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "ses_v2", Provider: "claude-code", AdapterToken: "adapter-token", Format: "jsonstream", ProtocolVersion: protocol.ProtocolVersionV2, ProviderCommand: []string{os.Args[0]}}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "provider start admission rejected") {
		t.Fatalf("runWrap(v2 final rejected start) error = %v", err)
	}
	assertSignal(t, proofSeen, "provider start proof")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if content, err := os.ReadFile(marker); err == nil && string(content) == "stopped" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	content, _ := os.ReadFile(marker)
	t.Fatalf("started provider was not stopped after final rejection: %q", content)
}

func TestRunWrapV2ReAdmitsEveryRestartedChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t.Setenv("AGENTWHARF_RESTART_CRASH_HELPER", "1")
	attempts := make(chan int, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if _, err := readFrameFromConn(ctx, conn); err != nil {
			return
		}
		if err := writeFrameToConn(ctx, conn, &protocol.HelloAck{ProtocolVersion: protocol.ProtocolVersionV2, Sessions: []protocol.SessionSummary{{SessionID: "ses_v2", Provider: "claude-code"}}, ConnectionAuthority: &protocol.ConnectionAuthorityReceipt{SessionID: "ses_v2", ConnectionEpoch: 1, CredentialGeneration: 1, AcceptedFence: 1, WriterLeaseID: "lease_v2", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}}); err != nil {
			return
		}
		for expected := 1; expected <= 2; expected++ {
			frame, err := readProviderStartFrame(ctx, conn)
			if err != nil {
				return
			}
			start, ok := frame.(*protocol.ProviderStart)
			if !ok || start.Attempt != expected {
				t.Errorf("provider start = %T %+v, want attempt %d", frame, frame, expected)
				return
			}
			if err := writeFrameToConn(ctx, conn, &protocol.ProviderStartPrepare{Attempt: expected}); err != nil {
				return
			}
			frame, err = readFrameFromConn(ctx, conn)
			if started, ok := frame.(*protocol.ProviderStartStarted); err != nil || !ok || started.Attempt != expected {
				t.Errorf("provider start proof = %T %+v, %v", frame, frame, err)
				return
			}
			attempts <- expected
			if err := writeFrameToConn(ctx, conn, &protocol.ProviderStartAck{Attempt: expected, Status: protocol.ProviderStartAdmitted, RecoveryHandle: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"}); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	_, _ = runWrap(ctx, wrapConfig{HubURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "ses_v2", Provider: "claude-code", AdapterToken: "adapter-token", Format: "jsonstream", ProtocolVersion: protocol.ProtocolVersionV2, ProviderCommand: []string{os.Args[0], "-test.run=TestProcessSupervisorHelperProcess"}}, nil, io.Discard)
	for _, want := range []int{1, 2} {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("admitted attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for admitted attempt %d", want)
		}
	}
}

func TestRunControlCapabilityAndOutcomeProposalsAreCanonical(t *testing.T) {
	t.Parallel()

	cfg := wrapConfig{ProtocolVersion: protocol.ProtocolVersionV2, SessionID: "ses_run_control"}
	frames := make([]protocol.Frame, 0, 3)
	write := func(frame protocol.Frame) error {
		frames = append(frames, frame)
		return nil
	}
	if err := publishRunControlCapability(context.Background(), nil, cfg, write); err != nil {
		t.Fatalf("publishRunControlCapability() error = %v", err)
	}
	command := &protocol.Command{CommandID: "cmd_run_control_1", Type: protocol.CommandSessionInterrupt, SessionID: cfg.SessionID}
	if err := acknowledgeRunControl(context.Background(), command, write, cfg, "interrupt", "ready", nil); err != nil {
		t.Fatalf("acknowledgeRunControl() error = %v", err)
	}
	if len(frames) != 3 {
		t.Fatalf("proposal frames = %d, want 3", len(frames))
	}
	capability, ok := frames[0].(*protocol.Event)
	if !ok || capability.Type != "session.run.capabilities" || capability.ProposalID == "" {
		t.Fatalf("capability frame = %#v", frames[0])
	}
	if _, err := protocol.DecodeRunControlCapabilityPayload(capability.Payload); err != nil {
		t.Fatalf("capability payload decode error = %v", err)
	}
	outcome, ok := frames[2].(*protocol.Event)
	if !ok || outcome.Type != "session.run.outcome" || outcome.ProposalID == "" {
		t.Fatalf("outcome frame = %#v", frames[2])
	}
	decoded, err := protocol.DecodeRunControlOutcomePayload(outcome.Payload)
	if err != nil {
		t.Fatalf("outcome payload decode error = %v; payload=%s", err, outcome.Payload)
	}
	if decoded.CommandID != command.CommandID || decoded.Operation != "interrupt" || decoded.Outcome != "completed" || decoded.CompletionState == nil || *decoded.CompletionState != "ready" {
		t.Fatalf("decoded outcome = %+v", decoded)
	}
}

func TestProviderStartAdmissionRetainsOpaqueRecoveryHandle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		frame, err := readProviderStartFrame(ctx, conn)
		start, ok := frame.(*protocol.ProviderStart)
		if err != nil || !ok || start.Attempt != 1 {
			t.Errorf("provider start = %T %+v, %v", frame, frame, err)
			return
		}
		if err := writeFrameToConn(ctx, conn, &protocol.ProviderStartPrepare{Attempt: start.Attempt}); err != nil {
			return
		}
		frame, err = readFrameFromConn(ctx, conn)
		started, ok := frame.(*protocol.ProviderStartStarted)
		if err != nil || !ok || started.Attempt != start.Attempt {
			t.Errorf("provider started = %T %+v, %v", frame, frame, err)
			return
		}
		_ = writeFrameToConn(ctx, conn, &protocol.ProviderStartAck{Attempt: start.Attempt, Status: protocol.ProviderStartAdmitted, RecoveryHandle: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"})
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	admission := newProviderStartAdmission(protocol.ProtocolVersionV2, conn, func(frame protocol.Frame) error {
		return writeFrameToConn(ctx, conn, frame)
	})
	if err := admission.PrepareProcessStart(ctx, 1); err != nil {
		t.Fatalf("PrepareProcessStart() = %v", err)
	}
	if err := admission.ConfirmProcessStarted(ctx, 1); err != nil {
		t.Fatalf("ConfirmProcessStarted() = %v", err)
	}
	if _, err := admission.RecoveryStartHandle(); err != nil {
		t.Fatalf("RecoveryStartHandle() = %v", err)
	}
}

func TestProviderStartAdmissionInvalidationClearsRecoveryReference(t *testing.T) {
	admission := &providerStartAdmission{recoveryHandle: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"}
	if _, err := admission.RecoveryStartHandle(); err != nil {
		t.Fatalf("RecoveryStartHandle() before invalidation = %v", err)
	}
	admission.invalidate()
	if _, err := admission.RecoveryStartHandle(); err == nil {
		t.Fatal("RecoveryStartHandle() succeeded after durable lifecycle invalidation")
	}
	if err := admission.VerifyRecoveryStart(context.Background()); err == nil {
		t.Fatal("VerifyRecoveryStart() succeeded after durable lifecycle invalidation")
	}
}

func TestProviderStartAdmissionWatcherInvalidatesClosedSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverConn := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		serverConn <- conn
		<-ctx.Done()
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	admission := &providerStartAdmission{conn: conn, recoveryHandle: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"}
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	admission.watchLifecycle(watchCtx, stop)
	select {
	case peer := <-serverConn:
		_ = peer.Close(websocket.StatusPolicyViolation, "authority revoked")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server connection")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := admission.RecoveryStartHandle(); err != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("closed Hub socket did not invalidate recovery reference")
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

func TestRunWrapACPProviderSendsIdleHeartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Setenv("AGENTWHARF_ACP_IDLE_HELPER", "1")
	t.Setenv("AGENTWHARF_HEARTBEAT_INTERVAL", "25ms")
	t.Setenv("AGENTWHARF_HEARTBEAT_TIMEOUT", "1s")

	helloSeen := make(chan struct{}, 1)
	readySeen := make(chan struct{}, 1)
	pingSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")

		frame, err := readFrameFromConn(ctx, conn)
		if err != nil {
			t.Errorf("read hello: %v", err)
			return
		}
		hello, ok := frame.(*protocol.Hello)
		if !ok || hello.Role != protocol.RoleAdapter || hello.SessionID != "ses_idle" {
			t.Errorf("hello frame = %+v", frame)
			return
		}
		helloSeen <- struct{}{}
		if err := writeFrameToConn(ctx, conn, &protocol.HelloAck{
			ProtocolVersion: protocol.ProtocolVersion,
			Sessions:        []protocol.SessionSummary{{SessionID: "ses_idle", State: "starting", Provider: "claude-code", ReplayFrom: 1}},
		}); err != nil {
			t.Errorf("write hello ack: %v", err)
			return
		}

		for {
			frame, err := readFrameFromConn(ctx, conn)
			if err != nil {
				return
			}
			switch typed := frame.(type) {
			case *protocol.Event:
				if typed.Type == "session.state" {
					readySeen <- struct{}{}
				}
			case *protocol.Ping:
				if typed.Nonce == "" {
					t.Errorf("heartbeat ping nonce is empty")
					return
				}
				pingSeen <- struct{}{}
				_ = writeFrameToConn(ctx, conn, &protocol.Pong{Nonce: typed.Nonce})
				return
			}
		}
	}))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	runDone := make(chan error, 1)
	go func() {
		runDone <- runWithInput(ctx, []string{
			"wrap",
			"--hub", wsURL,
			"--session-id", "ses_idle",
			"--adapter-token", "adapter-token",
			"--agent", "claude",
			"--acp",
			"--", os.Args[0],
		}, nil, io.Discard, io.Discard)
	}()

	assertSignal(t, helloSeen, "adapter hello")
	assertSignal(t, readySeen, "provider ready event")
	assertSignal(t, pingSeen, "adapter heartbeat ping")
	cancel()
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("run wrap error = %v", err)
	}
}

func TestAdapterHeartbeatTimesOutWithoutPong(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done, _ := startAdapterHeartbeat(ctx, heartbeatConfig{
		Interval: time.Millisecond,
		Timeout:  10 * time.Millisecond,
	}, func(protocol.Frame) error {
		return nil
	})

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "hub heartbeat timed out") {
			t.Fatalf("heartbeat error = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat timeout")
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
	frame, err := readFrameFromConn(ctx, conn)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame
}

func writeFrameToConn(ctx context.Context, conn *websocket.Conn, frame protocol.Frame) error {
	data, err := protocol.Encode(frame)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func readFrameFromConn(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, fmt.Errorf("websocket message type = %v, want text", typ)
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("decode frame %s: %w", string(data), err)
	}
	return frame, nil
}

func readProviderStartFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	for {
		frame, err := readFrameFromConn(ctx, conn)
		if err != nil {
			return nil, err
		}
		if event, ok := frame.(*protocol.Event); ok && event.Type == "session.run.capabilities" {
			continue
		}
		return frame, nil
	}
}

func assertSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestMain(m *testing.M) {
	if os.Getenv("AGENTWHARF_RESTART_CRASH_HELPER") == "1" {
		os.Exit(2)
	}
	if os.Getenv("AGENTWHARF_START_BLOCK_HELPER") == "1" {
		runWrapStartBlockProviderHelper()
		return
	}
	if os.Getenv("AGENTWHARF_ACP_PERMISSION_HELPER") == "1" {
		runWrapACPPermissionProviderHelper()
		return
	}
	if os.Getenv("AGENTWHARF_ACP_IDLE_HELPER") == "1" {
		runWrapACPIdleProviderHelper()
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
	keyFile, err := os.CreateTemp("", "agentwharf-test-session-signer-*")
	if err != nil {
		os.Exit(1)
	}
	keyPath := keyFile.Name()
	if _, err := keyFile.WriteString("test-only-local-session-credential-signer"); err != nil {
		_ = keyFile.Close()
		_ = os.Remove(keyPath)
		os.Exit(1)
	}
	if err := keyFile.Close(); err != nil || os.Setenv("AGENTWHARF_SESSION_CREDENTIAL_SIGNER_KEY_FILE", keyPath) != nil {
		_ = os.Remove(keyPath)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.Remove(keyPath)
	os.Exit(code)
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

func runWrapStartBlockProviderHelper() {
	marker := os.Getenv("AGENTWHARF_START_BLOCK_MARKER")
	if marker == "" {
		os.Exit(30)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	if os.WriteFile(marker, []byte("ready"), 0600) != nil {
		os.Exit(30)
	}
	<-signals
	_ = os.WriteFile(marker, []byte("stopped"), 0600)
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

func runWrapACPIdleProviderHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	init := readACPRequest(scanner)
	if init["method"] != "initialize" {
		os.Exit(40)
	}
	writeACPResponse(init["id"], map[string]any{"protocolVersion": 1})

	sessionNew := readACPRequest(scanner)
	if sessionNew["method"] != "session/new" {
		os.Exit(41)
	}
	writeACPResponse(sessionNew["id"], map[string]any{"sessionId": "acp_ses_idle"})

	for {
		time.Sleep(time.Hour)
	}
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
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
