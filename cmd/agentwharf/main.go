package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/adapter/acp"
	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/adapter/fallback/jsonstream"
	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/auth/static"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/internal/buildinfo"
	"github.com/winghv/agentwharf/masking"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
	"github.com/winghv/agentwharf/store/sqlite"
	"nhooyr.io/websocket"
)

const (
	defaultServeAddr          = "127.0.0.1:8765"
	defaultSessionID          = "local"
	defaultProvider           = "claude-code"
	defaultControlToken       = "local-control-token"
	defaultAdapterToken       = "local-adapter-token"
	defaultWrapHubURL         = "ws://" + defaultServeAddr
	defaultManagedCloudAPIURL = "https://cloud.superwhv.me/v1"
	defaultHeartbeatInterval  = 20 * time.Second
	defaultHeartbeatTimeout   = 60 * time.Second
	cloudAPIMaxAttempts       = 3
)

var (
	errUnsafeDefaultToken = errors.New("default local tokens require a loopback listen address")
	errClaimAuthRejection = errors.New("claim authentication rejected")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	return runWithInput(ctx, args, os.Stdin, stdout, stderr)
}

func runWithInput(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: wharf serve|hub|wrap|claude|codex|gemini|logout|version|upgrade|attention-backfill [options]")
	}

	switch args[0] {
	case "version", "--version", "-v":
		if len(args) != 1 {
			return errors.New("usage: wharf version")
		}
		_, _ = fmt.Fprintf(stdout, "wharf %s\n", buildinfo.Version)
		return nil
	case "upgrade":
		return runUpgradeCommand(ctx, args[1:], stdin, stdout, stderr)
	case "serve":
		return runServeCommand(ctx, args[1:], stdout, stderr)
	case "hub":
		cfg, err := parseServeConfig(args[1:], stderr)
		if err != nil {
			return err
		}
		running, err := startServe(ctx, cfg)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "wharf hub listening on %s\n", running.wsURL)
		_, _ = fmt.Fprintf(stdout, "session_id=%s provider=%s\n", cfg.SessionID, cfg.Provider)
		return running.wait()
	case "wrap":
		cfg, err := parseWrapConfig(args[1:], stderr)
		if err != nil {
			return err
		}
		effective, err := runWrap(ctx, cfg, stdin, stderr)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "wharf wrap sent events for session_id=%s provider=%s\n", effective.SessionID, effective.Provider)
		return nil
	case "claude", "codex", "gemini":
		go maybePrintUpdateReminder(ctx, stderr)
		cfg, err := parseAgentEntrypointConfig(args[0], args[1:], stderr)
		if err != nil {
			return err
		}
		if cfg.PairOnly {
			// User-facing onboarding: pair, start the daemon, and exit. No
			// interactive Provider session is opened.
			return runPairOnly(ctx, cfg, stdout, stderr)
		}
		effective, err := runWrap(ctx, cfg, stdin, stderr)
		if !cfg.StartupSmoke {
			// After pairing (or reusing an existing pairing) plus one session, hand
			// off to the background daemon so the machine stays online without a
			// manual wharf serve. Best-effort: a failed daemon start only prints a hint.
			ensureBackgroundDaemon(stderr)
		}
		if err != nil {
			return err
		}
		if cfg.StartupSmoke {
			_, _ = fmt.Fprintln(stdout, "provider_start_smoke_ok: true")
			return nil
		}
		_, _ = fmt.Fprintf(stdout, "wharf %s ended session_id=%s provider=%s\n", args[0], effective.SessionID, effective.Provider)
		return nil
	case "logout":
		if len(args) != 1 {
			return fmt.Errorf("unexpected logout arguments: %v", args[1:])
		}
		return runMachineLogout(stdout)
	case "task":
		return runTaskCommand(ctx, args[1:], stdin, stdout, stderr)
	case "attention-backfill":
		return runAttentionBackfill(ctx, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

type machineTaskClaimExchangeRequest struct {
	ClaimCode string `json:"claim_code"`
}

type machineTaskClaimExchangeResponse struct {
	Data struct {
		SessionID    string `json:"session_id"`
		Provider     string `json:"provider"`
		HubWSURL     string `json:"hub_ws_url"`
		AdapterToken string `json:"adapter_token"`
		// The platform issues a long-lived adapter token alongside a short-lived
		// client token, so the exchange response carries separate expiry fields;
		// expires_at remains the single pre-long-lived-token fallback.
		AdapterExpiresAt  string `json:"adapter_expires_at"`
		ClientExpiresAt   string `json:"client_expires_at"`
		ExpiresAt         string `json:"expires_at"`
		ModelID           string `json:"model_id"`
		ReasoningEffortID string `json:"reasoning_effort_id"`
		PermissionModeID  string `json:"permission_mode_id"`
		Session           struct {
			Provider string `json:"provider"`
		} `json:"session"`
	} `json:"data"`
}

const taskClaimUsage = "usage: wharf task claim <claim_id> --code-stdin [--startup-smoke]"

func runTaskCommand(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) < 2 || args[0] != "claim" {
		return errors.New(taskClaimUsage)
	}
	claimID := strings.TrimSpace(args[1])
	if claimID == "" || strings.ContainsAny(claimID, "/\\") {
		return errors.New("claim unavailable")
	}
	codeStdin := false
	startupSmoke := false
	for _, arg := range args[2:] {
		if arg == "--code-stdin" {
			codeStdin = true
			continue
		}
		if arg == "--startup-smoke" {
			startupSmoke = true
			continue
		}
		return errors.New(taskClaimUsage)
	}
	if codeStdin && claimInputIsTTY(stdin) {
		return errors.New("claim code input unavailable")
	}
	var code []byte
	var err error
	if codeStdin {
		code, err = readClaimCode(stdin)
	} else {
		if !claimInputIsTTY(stdin) {
			return errors.New("claim code input unavailable")
		}
		code, err = readClaimCodeTTY(stdin, stderr)
	}
	if err != nil {
		return err
	}
	defer func() {
		for index := range code {
			code[index] = 0
		}
	}()
	credential, err := loadMachineCredential()
	if err != nil {
		return errors.New("claim unavailable")
	}
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machine-task-claims/"+url.PathEscape(claimID)+"/exchange")
	if err != nil {
		return errors.New("claim unavailable")
	}
	status, body, err := postCloudAPIJSON(ctx, &http.Client{Timeout: 30 * time.Second}, endpoint, credential.MachineToken, machineTaskClaimExchangeRequest{ClaimCode: string(code)})
	if err != nil || status < http.StatusOK || status >= http.StatusMultipleChoices {
		return errors.New("claim unavailable")
	}
	var handoff machineTaskClaimExchangeResponse
	if err := decodeCloudAPIJSON(body, &handoff); err != nil || handoff.Data.SessionID == "" || handoff.Data.HubWSURL == "" || handoff.Data.AdapterToken == "" ||
		(handoff.Data.AdapterExpiresAt == "" && handoff.Data.ExpiresAt == "") {
		return errors.New("claim unavailable")
	}
	provider := strings.TrimSpace(handoff.Data.Provider)
	if provider == "" {
		provider = strings.TrimSpace(handoff.Data.Session.Provider)
	}
	if provider == "" || strings.ContainsAny(provider, " \t\r\n") {
		return errors.New("claim unavailable")
	}
	expiresAtRaw := handoff.Data.AdapterExpiresAt
	if expiresAtRaw == "" {
		expiresAtRaw = handoff.Data.ExpiresAt
	}
	expiresAt, err := time.Parse(time.RFC3339, expiresAtRaw)
	if err != nil || !expiresAt.After(time.Now().UTC()) {
		return errors.New("claim unavailable")
	}
	agent := provider
	if provider == "claude-code" {
		agent = "claude"
	}
	cfg := wrapConfig{
		HubURL:          handoff.Data.HubWSURL,
		SessionID:       handoff.Data.SessionID,
		Agent:           agent,
		Provider:        provider,
		AdapterToken:    handoff.Data.AdapterToken,
		Format:          "acp",
		ProviderCommand: defaultProviderCommand(agent),
		ProtocolVersion: protocol.HubProtocolVersion,
		StartupSmoke:    startupSmoke,
		LaunchSettings:  wrapLaunchSettings{ModelID: handoff.Data.ModelID, ReasoningEffortID: handoff.Data.ReasoningEffortID, PermissionModeID: handoff.Data.PermissionModeID},
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := runWrap(ctx, cfg, strings.NewReader(""), stderr); err == nil {
			if startupSmoke {
				_, _ = fmt.Fprintln(stdout, "provider_start_smoke_ok: true")
			}
			return nil
		} else if claimLaunchRequiresReclaim(err) {
			return errors.New("reclaim required")
		} else if attempt == 1 {
			return errors.New("reclaim required")
		}
	}
	return errors.New("reclaim required")
}

func claimLaunchRequiresReclaim(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errClaimAuthRejection) || errors.Is(err, core.ErrInvalidHelloAck) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "unauthorized") || strings.Contains(lower, "invalid hello ack") || strings.Contains(lower, "credential")
}

func claimInputIsTTY(input io.Reader) bool {
	file, ok := input.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func readClaimCode(input io.Reader) ([]byte, error) {
	line, err := bufio.NewReader(io.LimitReader(input, 257)).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.New("claim code input unavailable")
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	if line == "" || len(line) > 256 || strings.ContainsAny(line, "\r\n") {
		return nil, errors.New("claim code input unavailable")
	}
	return []byte(line), nil
}

func readClaimCodeTTY(input io.Reader, prompt io.Writer) ([]byte, error) {
	file, ok := input.(*os.File)
	if !ok || !claimInputIsTTY(input) {
		return nil, errors.New("claim code input unavailable")
	}
	if err := setTerminalEcho(file, false); err != nil {
		return nil, errors.New("claim code input unavailable")
	}
	defer func() { _ = setTerminalEcho(file, true) }()
	if prompt != nil {
		_, _ = fmt.Fprint(prompt, "Claim code: ")
	}
	code, err := readClaimCode(file)
	if prompt != nil {
		_, _ = fmt.Fprintln(prompt)
	}
	return code, err
}

func setTerminalEcho(file *os.File, enabled bool) error {
	argument := "-echo"
	if enabled {
		argument = "echo"
	}
	command := exec.Command("stty", argument)
	command.Stdin = file
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	return command.Run()
}

func runAttentionBackfill(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("attention-backfill", flag.ContinueOnError)
	flags.SetOutput(stderr)
	checkpoint := flags.String("checkpoint", "", "absolute checkpoint path")
	batch := flags.Int("batch-size", 256, "sessions per bounded transaction")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *checkpoint == "" {
		return errors.New("usage: wharf attention-backfill --checkpoint ABSOLUTE_PATH [--batch-size 1..256]")
	}
	dsn := os.Getenv("AGENTWHARF_POSTGRES_DSN")
	if dsn == "" {
		return errors.New("AGENTWHARF_POSTGRES_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("open attention backfill postgres pool")
	}
	defer pool.Close()
	result, err := postgres.New(pool).RunAttentionBackfill(ctx, postgres.FileAttentionBackfillCheckpointStore{Path: *checkpoint}, *batch)
	if err != nil {
		return fmt.Errorf("run attention backfill: %w", err)
	}
	_, _ = fmt.Fprintf(stdout, "attention-backfill processed=%d incomplete=%d done=%t\n", result.Processed, result.Incomplete, result.Done)
	return nil
}

func runMachineLogout(stdout io.Writer) error {
	existed, err := machineCredentialExists()
	if err != nil {
		return err
	}
	if err := deleteMachineCredential(); err != nil {
		return err
	}
	if stdout == nil {
		return nil
	}
	if existed {
		_, _ = fmt.Fprintln(stdout, "wharf: local machine pairing removed")
	} else {
		_, _ = fmt.Fprintln(stdout, "wharf: no local machine pairing found")
	}
	return nil
}

type serveConfig struct {
	Addr                              string
	DBPath                            string
	SessionID                         string
	Provider                          string
	ControlToken                      string
	AdapterToken                      string
	SessionCredentialSignerKeyFile    string
	SessionCredentialSignerKeyVersion int64
}

// wrapLaunchSettings carries the provider-neutral launch settings (model,
// reasoning effort, permission mode) requested for a Session. All fields are
// optional; empty means "provider default". Values are validated against the
// ACP config-option capability before any provider mutation.
type wrapLaunchSettings struct {
	ModelID           string
	ReasoningEffortID string
	PermissionModeID  string
}

func (s wrapLaunchSettings) requested() bool {
	return s.ModelID != "" || s.ReasoningEffortID != "" || s.PermissionModeID != ""
}

type wrapConfig struct {
	HubURL             string
	SessionID          string
	Agent              string
	Provider           string
	AdapterToken       string
	Format             string
	SecretDir          string
	Managed            bool
	Pair               bool
	CloudAPIURL        string
	Heartbeat          heartbeatConfig
	ProviderCommand    []string
	HealthMarker       string
	ProviderCredential *core.ProcessCredential
	ProtocolVersion    int
	StartupSmoke       bool
	PairOnly           bool
	Session            bool
	WorkingDirectory   string
	LaunchSettings     wrapLaunchSettings
	Stderr             io.Writer
	Stdin              io.Reader
	Interactive        bool
	// ProviderSessionID enables ACP session/load during machine recovery.
	// OnProviderSession receives the opaque provider id after a successful
	// session/new or session/load and must only persist it locally.
	ProviderSessionID   string
	OnProviderSession   func(string)
	OnAdapterCredential func(string, time.Time)
}

type heartbeatConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

type machinePairingCreateRequest struct {
	Platform string `json:"platform"`
}

type machinePairingCodeResponse struct {
	Data struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURI string `json:"verification_uri"`
		ExpiresAt       string `json:"expires_at"`
		IntervalSeconds int    `json:"interval_seconds"`
	} `json:"data"`
}

type machinePairingTokenRequest struct {
	DeviceCode string `json:"device_code"`
}

type machineTokenResponse struct {
	Data struct {
		Machine struct {
			ID string `json:"id"`
		} `json:"machine"`
		MachineToken  string `json:"machine_token"`
		RefreshSecret string `json:"refresh_secret"`
		HubWSURL      string `json:"hub_ws_url"`
		ExpiresAt     string `json:"expires_at"`
	} `json:"data"`
}

type machineSessionCreateRequest struct {
	Provider string `json:"provider"`
}

type machineSessionResponse struct {
	Data struct {
		Session struct {
			ID       string `json:"id"`
			HostType string `json:"host_type"`
			HostID   string `json:"host_id"`
			Provider string `json:"provider"`
			Status   string `json:"status"`
		} `json:"session"`
		HubWSURL     string `json:"hub_ws_url"`
		AdapterToken string `json:"adapter_token"`
		ExpiresAt    string `json:"expires_at"`
	} `json:"data"`
}

func parseServeConfig(args []string, stderr io.Writer) (serveConfig, error) {
	cfg := serveConfig{
		Addr:                              defaultServeAddr,
		DBPath:                            defaultDBPath(),
		SessionID:                         defaultSessionID,
		Provider:                          defaultProvider,
		ControlToken:                      defaultControlToken,
		AdapterToken:                      defaultAdapterToken,
		SessionCredentialSignerKeyFile:    envOrDefault("AGENTWHARF_SESSION_CREDENTIAL_SIGNER_KEY_FILE", ""),
		SessionCredentialSignerKeyVersion: 1,
	}

	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.Addr, "addr", cfg.Addr, "listen address")
	flags.StringVar(&cfg.DBPath, "db", cfg.DBPath, "SQLite event store path")
	flags.StringVar(&cfg.SessionID, "session-id", cfg.SessionID, "local session id")
	flags.StringVar(&cfg.Provider, "provider", cfg.Provider, "provider name")
	flags.StringVar(&cfg.ControlToken, "control-token", cfg.ControlToken, "client control token")
	flags.StringVar(&cfg.AdapterToken, "adapter-token", cfg.AdapterToken, "adapter token")
	flags.StringVar(&cfg.SessionCredentialSignerKeyFile, "session-credential-signer-key-file", cfg.SessionCredentialSignerKeyFile, "local session credential signer key file")
	flags.Int64Var(&cfg.SessionCredentialSignerKeyVersion, "session-credential-signer-key-version", cfg.SessionCredentialSignerKeyVersion, "local session credential signer key version")
	if err := flags.Parse(args); err != nil {
		return serveConfig{}, err
	}
	if flags.NArg() != 0 {
		return serveConfig{}, fmt.Errorf("unexpected serve arguments: %v", flags.Args())
	}
	if cfg.SessionCredentialSignerKeyVersion < 1 {
		return serveConfig{}, errors.New("session credential signer key version must be positive")
	}
	return normalizeServeConfig(cfg)
}

func parseWrapConfig(args []string, stderr io.Writer) (wrapConfig, error) {
	cfg := wrapConfig{
		HubURL:          envOrDefault("AGENTWHARF_HUB_URL", defaultWrapHubURL),
		SessionID:       envOrDefault("AGENTWHARF_SESSION_ID", defaultSessionID),
		Agent:           envOrDefault("AGENTWHARF_AGENT", "claude"),
		Provider:        envOrDefault("AGENTWHARF_PROVIDER", ""),
		AdapterToken:    envOrDefault("AGENTWHARF_ADAPTER_TOKEN", defaultAdapterToken),
		Format:          envOrDefault("AGENTWHARF_FORMAT", "jsonstream"),
		SecretDir:       envOrDefault("AGENTWHARF_SECRET_DIR", ""),
		CloudAPIURL:     envOrDefault("AGENTWHARF_CLOUD_API_URL", envOrDefault("AGENTWHARF_CONTROL_PLANE_URL", "")),
		ProtocolVersion: protocol.ProtocolVersion,
	}
	var useACP bool
	var useJSONStream bool

	flags := flag.NewFlagSet("wrap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.HubURL, "hub", cfg.HubURL, "Hub WebSocket URL")
	flags.StringVar(&cfg.SessionID, "session-id", cfg.SessionID, "session id")
	flags.StringVar(&cfg.Agent, "agent", cfg.Agent, "agent name")
	flags.StringVar(&cfg.Provider, "provider", "", "provider name override")
	flags.StringVar(&cfg.AdapterToken, "adapter-token", cfg.AdapterToken, "adapter token")
	flags.StringVar(&cfg.Format, "format", cfg.Format, "input format: jsonstream or acp")
	flags.StringVar(&cfg.SecretDir, "secret-dir", cfg.SecretDir, "directory containing injected secret files for masking")
	flags.BoolVar(&cfg.Pair, "pair", false, "pair this machine with SuperWHV before connecting")
	flags.StringVar(&cfg.CloudAPIURL, "cloud", cfg.CloudAPIURL, "SuperWHV Cloud API base URL, usually ending in /v1")
	flags.BoolVar(&useACP, "acp", false, "read ACP JSON frames from stdin")
	flags.BoolVar(&useJSONStream, "jsonstream", false, "read Claude stream-json lines from stdin")
	flags.BoolVar(&cfg.StartupSmoke, "startup-smoke", false, "exit successfully after Provider admission and ACP initialization")
	flags.IntVar(&cfg.ProtocolVersion, "protocol-version", cfg.ProtocolVersion, "Adapter protocol version (1 or 2)")
	if err := flags.Parse(args); err != nil {
		return wrapConfig{}, err
	}
	cfg.ProviderCommand = append([]string(nil), flags.Args()...)
	if useACP && useJSONStream {
		return wrapConfig{}, errors.New("wrap format flags are mutually exclusive")
	}
	if useACP {
		cfg.Format = "acp"
	}
	if useJSONStream {
		cfg.Format = "jsonstream"
	}
	return normalizeWrapConfig(cfg)
}

// splitAgentEntrypointArgs separates wharf's own entrypoint flags from the
// passthrough agent arguments. Everything from the first non-wharf token on is
// forwarded verbatim to the underlying provider command, so
// `wharf claude --model X` behaves like `claude --model X`.
func splitAgentEntrypointArgs(args []string) (wharfArgs, agentArgs []string) {
	valueFlags := map[string]bool{
		"--hub": true, "--session-id": true, "--provider": true, "--adapter-token": true,
		"--secret-dir": true, "--cloud": true, "--protocol-version": true,
	}
	boolFlags := map[string]bool{
		"--pair": true, "--startup-smoke": true, "--session": true, "--pair-only": true,
		"--help": true, "-h": true,
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, _, hasValue := strings.Cut(arg, "=")
		switch {
		case boolFlags[name]:
			wharfArgs = append(wharfArgs, arg)
		case valueFlags[name]:
			wharfArgs = append(wharfArgs, arg)
			if !hasValue && i+1 < len(args) {
				i++
				wharfArgs = append(wharfArgs, args[i])
			}
		default:
			agentArgs = append(agentArgs, args[i:]...)
			return wharfArgs, agentArgs
		}
	}
	return wharfArgs, agentArgs
}

func parseAgentEntrypointConfig(agent string, args []string, stderr io.Writer) (wrapConfig, error) {
	managed := !hasInjectedHubSession()
	cfg := wrapConfig{
		HubURL:          envOrDefault("AGENTWHARF_HUB_URL", defaultWrapHubURL),
		SessionID:       envOrDefault("AGENTWHARF_SESSION_ID", defaultSessionID),
		Agent:           agent,
		Provider:        envOrDefault("AGENTWHARF_PROVIDER", ""),
		AdapterToken:    envOrDefault("AGENTWHARF_ADAPTER_TOKEN", defaultAdapterToken),
		Format:          "acp",
		SecretDir:       envOrDefault("AGENTWHARF_SECRET_DIR", ""),
		Managed:         managed,
		ProviderCommand: defaultProviderCommand(agent),
	}
	if cfg.Managed {
		cfg.CloudAPIURL = envOrDefault("AGENTWHARF_CLOUD_API_URL", envOrDefault("AGENTWHARF_CONTROL_PLANE_URL", defaultManagedCloudAPIURL))
	}

	wharfArgs, agentArgs := splitAgentEntrypointArgs(args)

	flags := flag.NewFlagSet(agent, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.HubURL, "hub", cfg.HubURL, "Hub WebSocket URL")
	flags.StringVar(&cfg.SessionID, "session-id", cfg.SessionID, "session id")
	flags.StringVar(&cfg.Provider, "provider", cfg.Provider, "provider name override")
	flags.StringVar(&cfg.AdapterToken, "adapter-token", cfg.AdapterToken, "adapter token")
	flags.StringVar(&cfg.SecretDir, "secret-dir", cfg.SecretDir, "directory containing injected secret files for masking")
	flags.StringVar(&cfg.CloudAPIURL, "cloud", cfg.CloudAPIURL, "SuperWHV Cloud API base URL")
	flags.BoolVar(&cfg.Pair, "pair", cfg.Pair, "pair this machine with SuperWHV before connecting")
	flags.BoolVar(&cfg.StartupSmoke, "startup-smoke", false, "exit successfully after Provider admission and ACP initialization")
	flags.BoolVar(&cfg.Session, "session", false, "run an interactive agent session after pairing instead of pairing only")
	flags.BoolVar(&cfg.PairOnly, "pair-only", cfg.PairOnly, "pair and hand off to the background daemon without running a Session")
	flags.IntVar(&cfg.ProtocolVersion, "protocol-version", protocol.HubProtocolVersion, "Adapter protocol version (1 or 2)")
	if err := flags.Parse(wharfArgs); err != nil {
		return wrapConfig{}, err
	}
	if len(agentArgs) > 0 {
		cfg.ProviderCommand = append(cfg.ProviderCommand, agentArgs...)
	}
	if cfg.Pair {
		cfg.Managed = true
	}
	if cfg.Managed && cfg.CloudAPIURL == "" {
		cfg.CloudAPIURL = defaultManagedCloudAPIURL
	}
	// Running a real Provider session is the user-facing default, so
	// `wharf claude` launches the agent and mirrors it to the Hub. --pair-only
	// opts back into onboarding pair + daemon handoff, and --session/--startup-smoke
	// remain compatible no-ops for the pair-only short-circuit.
	if cfg.Session || cfg.StartupSmoke {
		cfg.PairOnly = false
	}
	return normalizeWrapConfig(cfg)
}

func defaultDBPath() string {
	dir, err := os.UserConfigDir()
	if err != nil || dir == "" {
		return "agentwharf.db"
	}
	return filepath.Join(dir, "agentwharf", "events.db")
}

func normalizeServeConfig(cfg serveConfig) (serveConfig, error) {
	if cfg.Addr == "" {
		cfg.Addr = defaultServeAddr
	}
	if cfg.DBPath == "" {
		cfg.DBPath = defaultDBPath()
	}
	if cfg.SessionID == "" {
		cfg.SessionID = defaultSessionID
	}
	if cfg.Provider == "" {
		cfg.Provider = defaultProvider
	}
	if cfg.ControlToken == "" {
		token, err := randomToken()
		if err != nil {
			return serveConfig{}, err
		}
		cfg.ControlToken = token
	}
	if cfg.AdapterToken == "" {
		token, err := randomToken()
		if err != nil {
			return serveConfig{}, err
		}
		cfg.AdapterToken = token
	}
	if cfg.SessionCredentialSignerKeyFile == "" {
		cfg.SessionCredentialSignerKeyFile = strings.TrimSpace(os.Getenv("AGENTWHARF_SESSION_CREDENTIAL_SIGNER_KEY_FILE"))
	}
	if cfg.SessionCredentialSignerKeyVersion == 0 {
		cfg.SessionCredentialSignerKeyVersion = 1
	}
	if !isLoopbackAddr(cfg.Addr) && usesDefaultToken(cfg) {
		return serveConfig{}, errUnsafeDefaultToken
	}
	return cfg, nil
}

func normalizeWrapConfig(cfg wrapConfig) (wrapConfig, error) {
	if cfg.HubURL == "" {
		cfg.HubURL = defaultWrapHubURL
	}
	if cfg.Heartbeat.Interval <= 0 {
		cfg.Heartbeat.Interval = durationEnvOrDefault("AGENTWHARF_HEARTBEAT_INTERVAL", defaultHeartbeatInterval)
	}
	if cfg.Heartbeat.Timeout <= 0 {
		cfg.Heartbeat.Timeout = durationEnvOrDefault("AGENTWHARF_HEARTBEAT_TIMEOUT", defaultHeartbeatTimeout)
	}
	if cfg.Heartbeat.Timeout < cfg.Heartbeat.Interval {
		cfg.Heartbeat.Timeout = cfg.Heartbeat.Interval
	}
	cfg.CloudAPIURL = strings.TrimSpace(cfg.CloudAPIURL)
	if cfg.Managed && cfg.CloudAPIURL == "" {
		cfg.CloudAPIURL = defaultManagedCloudAPIURL
	}
	if cfg.Pair && cfg.CloudAPIURL == "" {
		return wrapConfig{}, errors.New("wrap --pair requires --cloud or AGENTWHARF_CLOUD_API_URL")
	}
	if cfg.SessionID == "" {
		cfg.SessionID = defaultSessionID
	}
	if cfg.Agent == "" {
		cfg.Agent = "claude"
	}
	if cfg.Provider == "" {
		cfg.Provider = providerForAgent(cfg.Agent)
	}
	if cfg.AdapterToken == "" && !cfg.Pair && !cfg.Managed {
		token, err := randomToken()
		if err != nil {
			return wrapConfig{}, err
		}
		cfg.AdapterToken = token
	}
	switch cfg.Format {
	case "jsonstream", "acp":
	default:
		return wrapConfig{}, fmt.Errorf("unsupported wrap format %q", cfg.Format)
	}
	if cfg.StartupSmoke && (cfg.Format != "acp" || len(cfg.ProviderCommand) == 0) {
		return wrapConfig{}, errors.New("startup smoke requires an ACP provider command")
	}
	if !cfg.Pair && !cfg.Managed && cfg.AdapterToken == defaultAdapterToken && !isLoopbackURL(cfg.HubURL) {
		return wrapConfig{}, errUnsafeDefaultToken
	}
	cfg.SecretDir = filepath.Clean(cfg.SecretDir)
	if cfg.SecretDir == "." {
		cfg.SecretDir = ""
	}
	if cfg.ProtocolVersion == 0 {
		// Programmatic legacy callers historically omitted this field. CLI
		// entrypoints set the v2 default explicitly above.
		cfg.ProtocolVersion = protocol.ProtocolVersion
	}
	if cfg.ProtocolVersion != protocol.ProtocolVersion && cfg.ProtocolVersion != protocol.ProtocolVersionV2 {
		return wrapConfig{}, errors.New("wrap protocol version must be 1 or 2")
	}
	if marker := strings.TrimSpace(os.Getenv("SUPERWHV_FIXED_ENTRY_HEALTH_PATH")); marker != "" {
		uid, uidErr := strconv.ParseUint(os.Getenv("AGENTWHARF_PROVIDER_UID"), 10, 32)
		gid, gidErr := strconv.ParseUint(os.Getenv("AGENTWHARF_PROVIDER_GID"), 10, 32)
		if uidErr != nil || gidErr != nil || uid == 0 || gid == 0 {
			return wrapConfig{}, errors.New("fixed entry requires non-root provider uid and gid")
		}
		cfg.HealthMarker = marker
		cfg.ProviderCredential = &core.ProcessCredential{UID: uint32(uid), GID: uint32(gid)}
	}
	if cfg.Stderr == nil {
		cfg.Stderr = io.Discard
	}
	return cfg, nil
}

func providerForAgent(agent string) string {
	switch agent {
	case "claude", "claude-code":
		return defaultProvider
	default:
		return agent
	}
}

func defaultProviderCommand(agent string) []string {
	switch agent {
	case "claude", "claude-code":
		return []string{"claude-agent-acp"}
	case "codex":
		return []string{"codex-acp"}
	default:
		return []string{agent}
	}
}

func validateProviderCommand(cfg wrapConfig) error {
	if len(cfg.ProviderCommand) == 0 {
		return nil
	}
	command := strings.TrimSpace(cfg.ProviderCommand[0])
	if command == "" {
		return errors.New("provider command is empty")
	}
	if _, err := exec.LookPath(command); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return fmt.Errorf("provider command %s not found in PATH; install the Agent Client Protocol bridge or pass an explicit provider command", command)
		}
		return fmt.Errorf("check provider command %s: %w", command, err)
	}
	return nil
}

func hasInjectedHubSession() bool {
	return os.Getenv("AGENTWHARF_HUB_URL") != "" &&
		os.Getenv("AGENTWHARF_SESSION_ID") != "" &&
		os.Getenv("AGENTWHARF_ADAPTER_TOKEN") != ""
}

func envOrDefault(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationEnvOrDefault(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}

func randomToken() (string, error) {
	var token [24]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func usesDefaultToken(cfg serveConfig) bool {
	return cfg.ControlToken == defaultControlToken || cfg.AdapterToken == defaultAdapterToken
}

func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isLoopbackURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func runWrap(ctx context.Context, cfg wrapConfig, stdin io.Reader, pairOutput io.Writer) (wrapConfig, error) {
	cfg, err := normalizeWrapConfig(cfg)
	if err != nil {
		return cfg, err
	}
	explicitStdin := stdin != nil
	if stdin == nil {
		stdin = io.Reader(os.Stdin)
	}
	if pairOutput != nil {
		cfg.Stderr = pairOutput
	}
	// An explicitly supplied character-device stdin means a real terminal: run
	// the official claude/codex CLI and mirror its transcript to the Hub. The
	// nil-stdin default (and pipes) stay headless, and smoke/pair-only never
	// enter the interactive path.
	if explicitStdin && !cfg.StartupSmoke && !cfg.PairOnly {
		if file, ok := stdin.(*os.File); ok {
			if info, err := file.Stat(); err == nil && info.Mode()&os.ModeCharDevice != 0 {
				cfg.Interactive = true
				cfg.Stdin = stdin
			}
		}
	}
	if err := validateProviderCommand(cfg); err != nil {
		return cfg, err
	}
	metrics := core.NewAdapterMetrics()
	diagnostics := startWrapDiagnostics(ctx, cfg, metrics)
	if diagnostics != nil {
		defer diagnostics.Close()
	}
	metrics.SetWorkerCounts(1, 0, 0)
	defer metrics.SetWorkerCounts(0, 0, 0)
	if cfg.Managed {
		cfg, err = prepareManagedWrapSession(ctx, cfg, pairOutput)
		if err != nil {
			return cfg, err
		}
		// Keep the machine task consumer alive while an interactive TUI is
		// running. Starting it only after runWrap returns leaves Console-created
		// auto claims in `starting` until the local CLI exits.
		if !cfg.StartupSmoke && !cfg.PairOnly {
			ensureBackgroundDaemon(cfg.Stderr)
		}
	} else if cfg.Pair {
		cfg, err = pairWrapSession(ctx, cfg, pairOutput)
		if err != nil {
			return cfg, err
		}
	}

	conn, _, err := websocket.Dial(ctx, cfg.HubURL, nil)
	if err != nil {
		return cfg, fmt.Errorf("connect hub: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	state, err := core.NewAdapterConnectionState(core.AdapterConnectionConfig{
		SessionID:       cfg.SessionID,
		Provider:        cfg.Provider,
		Token:           cfg.AdapterToken,
		ProtocolVersion: cfg.ProtocolVersion,
	})
	if err != nil {
		return cfg, err
	}
	hello := state.Hello()
	if err := writeCLIProtocolFrame(ctx, conn, &hello); err != nil {
		return cfg, fmt.Errorf("send adapter hello: %w", err)
	}
	frame, err := readCLIProtocolFrame(ctx, conn)
	if err != nil {
		return cfg, fmt.Errorf("read hello ack: %w", err)
	}
	if protocolErr, ok := frame.(*protocol.Error); ok && claimProtocolErrorRequiresReclaim(protocolErr) {
		return cfg, fmt.Errorf("%w: %s: %s", errClaimAuthRejection, protocolErr.Code, protocolErr.Message)
	}
	ack, ok := frame.(*protocol.HelloAck)
	if !ok {
		return cfg, fmt.Errorf("read hello ack: got %T", frame)
	}
	if _, err := state.MarkAccepted(*ack); err != nil {
		return cfg, err
	}
	masker, err := eventMaskerFromSecretDir(cfg.SecretDir)
	if err != nil {
		return cfg, err
	}

	if len(cfg.ProviderCommand) > 0 {
		if cfg.ProtocolVersion == protocol.ProtocolVersionV2 {
			if ack.ConnectionAuthority == nil {
				return cfg, errors.New("provider start requires v2 connection authority")
			}
		}
		connection := newHubConnection(cfg, conn, ack.ConnectionAuthority)
		defer connection.close()
		if cfg.Interactive {
			// Real terminal: run the official claude/codex CLI directly and mirror
			// its transcript to the Hub instead of the headless ACP bridge.
			return cfg, runOfficialProvider(ctx, cfg, connection, masker, metrics)
		}
		return cfg, runWrapProvider(ctx, cfg, connection, masker, metrics, ack.ConnectionAuthority)
	}

	events, err := translateWrapInput(ctx, cfg, stdin)
	if err != nil {
		return cfg, err
	}
	for _, ev := range events {
		event, err := maskEvent(masker, ev)
		if err != nil {
			return cfg, err
		}
		if err := writeCLIProtocolFrame(ctx, conn, &event); err != nil {
			return cfg, fmt.Errorf("send event %s: %w", event.Type, err)
		}
		metrics.IncMaskedEvent()
	}
	return cfg, nil
}

func startWrapDiagnostics(ctx context.Context, cfg wrapConfig, metrics *core.AdapterMetrics) *core.AdapterDiagnosticsServer {
	operatorUID, fixedEntryUID, ok := diagnosticsIdentityFromEnv(uint32(os.Geteuid()))
	if !ok || cfg.HealthMarker == "" || cfg.ProviderCredential == nil || cfg.ProviderCredential.UID == 0 || cfg.ProviderCredential.UID == operatorUID || cfg.ProviderCredential.UID == fixedEntryUID {
		return nil
	}
	root := filepath.Dir(cfg.HealthMarker)
	if core.ValidateAdapterDiagnosticsRoot(root, cfg.HealthMarker, uint32(fixedEntryUID)) != nil {
		return nil
	}
	socketPath := filepath.Join(root, "diagnostics.sock")
	server, err := core.StartAdapterDiagnosticsWithConfig(ctx, core.AdapterDiagnosticsConfig{
		SocketPath:    socketPath,
		RootPath:      root,
		MarkerPath:    cfg.HealthMarker,
		Metrics:       metrics,
		FixedEntryUID: uint32(fixedEntryUID),
		OperatorUID:   uint32(operatorUID),
		ProviderUID:   cfg.ProviderCredential.UID,
	})
	if err != nil {
		return nil
	}
	return server
}

func diagnosticsIdentityFromEnv(effectiveUID uint32) (operatorUID, fixedEntryUID uint32, ok bool) {
	operatorRaw := strings.TrimSpace(os.Getenv("AGENTWHARF_DIAGNOSTICS_OPERATOR_UID"))
	fixedEntryRaw := strings.TrimSpace(os.Getenv("AGENTWHARF_FIXED_ENTRY_UID"))
	if operatorRaw == "" && fixedEntryRaw == "" {
		// HD-036 starts the fixed entry as root. Its effective identity is the
		// trusted proof when a legacy provisioner has no reserved env fields.
		if effectiveUID == 0 {
			return 0, 0, true
		}
		return 0, 0, false
	}
	operator, operatorErr := strconv.ParseUint(operatorRaw, 10, 32)
	fixedEntry, fixedEntryErr := strconv.ParseUint(fixedEntryRaw, 10, 32)
	if operatorErr != nil || fixedEntryErr != nil || operator != fixedEntry {
		return 0, 0, false
	}
	return uint32(operator), uint32(fixedEntry), true
}

func claimProtocolErrorRequiresReclaim(protocolErr *protocol.Error) bool {
	if protocolErr == nil {
		return false
	}
	code := strings.ToLower(strings.TrimSpace(protocolErr.Code))
	return protocolErr.Fatal || code == "unauthorized" || code == "invalid_hello" ||
		strings.Contains(code, "credential") || strings.Contains(code, "expired") || strings.Contains(code, "revoked")
}

func prepareManagedWrapSession(ctx context.Context, cfg wrapConfig, output io.Writer) (wrapConfig, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	if !cfg.Pair {
		credential, err := loadMachineCredential()
		switch {
		case err == nil && sameCloudAPIURL(credential.CloudAPIURL, cfg.CloudAPIURL):
			session, err := createMachineSession(ctx, client, cfg.CloudAPIURL, credential.MachineToken, cfg.Provider)
			if err == nil {
				return applyMachineSession(cfg, session)
			}
			if isInvalidMachineCredentialError(err) {
				// The 24-hour bearer expired while offline. Recover from the
				// pairing-time refresh secret instead of re-pairing.
				if recovered, recoverErr := recoverMachineCredential(ctx, client, credential); recoverErr == nil {
					if session, sessionErr := createMachineSession(ctx, client, cfg.CloudAPIURL, recovered.MachineToken, cfg.Provider); sessionErr == nil {
						return applyMachineSession(cfg, session)
					}
				}
				if deleteErr := deleteMachineCredential(); deleteErr != nil {
					return cfg, deleteErr
				}
				if output != nil {
					_, _ = fmt.Fprintln(output, "Local machine pairing is no longer valid; pairing again.")
				}
				return pairWrapSessionWithClient(ctx, client, cfg, output)
			}
			return cfg, err
		case err == nil:
			return pairWrapSessionWithClient(ctx, client, cfg, output)
		case errors.Is(err, errMachineCredentialNotFound):
			return pairWrapSessionWithClient(ctx, client, cfg, output)
		default:
			if deleteErr := deleteMachineCredential(); deleteErr != nil {
				return cfg, deleteErr
			}
			if output != nil {
				_, _ = fmt.Fprintln(output, "Local machine pairing is unreadable; pairing again.")
			}
			return pairWrapSessionWithClient(ctx, client, cfg, output)
		}
	}
	return pairWrapSessionWithClient(ctx, client, cfg, output)
}

func pairWrapSession(ctx context.Context, cfg wrapConfig, output io.Writer) (wrapConfig, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	return pairWrapSessionWithClient(ctx, client, cfg, output)
}

// pairMachineCredential performs managed pairing and persists the machine
// credential, without creating or running a Session. It is shared by the
// pair-only entrypoint and the session-running wrap flow.
func pairMachineCredential(ctx context.Context, client *http.Client, cfg wrapConfig, output io.Writer) (machineCredential, error) {
	createURL, err := cloudAPIEndpoint(cfg.CloudAPIURL, "/machine-pairing-codes")
	if err != nil {
		return machineCredential{}, err
	}
	var pairing machinePairingCodeResponse
	status, body, err := postCloudAPIJSONWithRetry(ctx, client, createURL, "", machinePairingCreateRequest{
		Platform: runtime.GOOS + "-" + runtime.GOARCH,
	})
	if err != nil {
		return machineCredential{}, err
	}
	if status != http.StatusCreated {
		return machineCredential{}, newCloudStatusError("create machine pairing code", status, body)
	}
	if err := decodeCloudAPIJSON(body, &pairing); err != nil {
		return machineCredential{}, fmt.Errorf("decode machine pairing response: %w", err)
	}
	if pairing.Data.DeviceCode == "" || pairing.Data.UserCode == "" {
		return machineCredential{}, errors.New("machine pairing response missing codes")
	}
	if output != nil {
		_, _ = fmt.Fprintf(output, "Pair this machine at %s\ndevice_code: %s\nuser_code: %s\n",
			machinePairingDisplayURL(cfg.CloudAPIURL, pairing.Data.VerificationURI),
			pairing.Data.DeviceCode,
			pairing.Data.UserCode)
	}

	machineToken, err := exchangeMachineToken(ctx, client, cfg.CloudAPIURL, pairing)
	if err != nil {
		return machineCredential{}, err
	}
	credential := machineCredential{
		MachineID:     machineToken.Data.Machine.ID,
		MachineToken:  machineToken.Data.MachineToken,
		RefreshSecret: machineToken.Data.RefreshSecret,
		CloudAPIURL:   cfg.CloudAPIURL,
		HubWSURL:      machineToken.Data.HubWSURL,
		ExpiresAt:     machineToken.Data.ExpiresAt,
	}
	if err := saveMachineCredential(credential); err != nil {
		return machineCredential{}, err
	}
	return credential, nil
}

func pairWrapSessionWithClient(ctx context.Context, client *http.Client, cfg wrapConfig, output io.Writer) (wrapConfig, error) {
	credential, err := pairMachineCredential(ctx, client, cfg, output)
	if err != nil {
		return cfg, err
	}
	session, err := createMachineSession(ctx, client, cfg.CloudAPIURL, credential.MachineToken, cfg.Provider)
	if err != nil {
		return cfg, err
	}
	return applyMachineSession(cfg, session)
}

func applyMachineSession(cfg wrapConfig, session machineSessionResponse) (wrapConfig, error) {
	if session.Data.Session.ID == "" || session.Data.HubWSURL == "" || session.Data.AdapterToken == "" {
		return cfg, errors.New("machine session response missing session, hub url, or adapter token")
	}
	cfg.SessionID = session.Data.Session.ID
	cfg.HubURL = session.Data.HubWSURL
	cfg.AdapterToken = session.Data.AdapterToken
	return cfg, nil
}

// runPairOnly pairs the machine (or reuses an existing pairing), prints a
// concise success message, starts the background daemon, and exits. It is the
// user-facing onboarding flow: pair once here, then manage everything from the
// Console while wharf serve keeps the machine online.
func runPairOnly(ctx context.Context, cfg wrapConfig, stdout, stderr io.Writer) error {
	normalized, err := normalizeWrapConfig(cfg)
	if err != nil {
		return err
	}
	if !normalized.Managed {
		normalized.Managed = true
		if normalized.CloudAPIURL == "" {
			normalized.CloudAPIURL = defaultManagedCloudAPIURL
		}
	}
	client := &http.Client{Timeout: 30 * time.Second}
	credential, err := loadMachineCredential()
	if err != nil || !sameCloudAPIURL(credential.CloudAPIURL, normalized.CloudAPIURL) {
		credential, err = pairMachineCredential(ctx, client, normalized, stderr)
		if err != nil {
			return err
		}
	}
	if credential.MachineID == "" || credential.MachineToken == "" {
		return errors.New("pairing did not produce a usable machine credential")
	}
	_, _ = fmt.Fprintln(stdout, "Pairing complete. This machine is connected to SuperWHV.")
	_, _ = fmt.Fprintln(stdout, "The background daemon (wharf serve) is running; you can close this window. Manage tasks from the Console.")
	ensureBackgroundDaemon(stderr)
	return nil
}

func exchangeMachineToken(ctx context.Context, client *http.Client, baseURL string, pairing machinePairingCodeResponse) (machineTokenResponse, error) {
	exchangeURL, err := cloudAPIEndpoint(baseURL, "/machine-pairing-codes/token")
	if err != nil {
		return machineTokenResponse{}, err
	}
	interval := time.Duration(pairing.Data.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Second
	}
	deadline := time.NewTimer(10 * time.Minute)
	defer deadline.Stop()
	for {
		status, body, err := postCloudAPIJSON(ctx, client, exchangeURL, "", machinePairingTokenRequest{
			DeviceCode: pairing.Data.DeviceCode,
		})
		if err != nil {
			return machineTokenResponse{}, err
		}
		switch status {
		case http.StatusOK:
			var response machineTokenResponse
			if err := decodeCloudAPIJSON(body, &response); err != nil {
				return machineTokenResponse{}, fmt.Errorf("decode machine token response: %w", err)
			}
			if response.Data.MachineToken == "" {
				return machineTokenResponse{}, errors.New("machine token response missing token")
			}
			return response, nil
		case http.StatusPreconditionRequired:
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return machineTokenResponse{}, ctx.Err()
			case <-deadline.C:
				timer.Stop()
				return machineTokenResponse{}, errors.New("machine pairing timed out")
			case <-timer.C:
			}
		default:
			return machineTokenResponse{}, newCloudStatusError("exchange machine pairing token", status, body)
		}
	}
}

func createMachineSession(ctx context.Context, client *http.Client, baseURL string, machineToken string, provider string) (machineSessionResponse, error) {
	sessionURL, err := cloudAPIEndpoint(baseURL, "/machine-sessions")
	if err != nil {
		return machineSessionResponse{}, err
	}
	status, body, err := postCloudAPIJSON(ctx, client, sessionURL, machineToken, machineSessionCreateRequest{
		Provider: provider,
	})
	if err != nil {
		return machineSessionResponse{}, err
	}
	if status != http.StatusCreated {
		return machineSessionResponse{}, newCloudStatusError("create machine session", status, body)
	}
	var response machineSessionResponse
	if err := decodeCloudAPIJSON(body, &response); err != nil {
		return machineSessionResponse{}, fmt.Errorf("decode machine session response: %w", err)
	}
	return response, nil
}

type cloudStatusError struct {
	Operation string
	Status    int
	Message   string
}

func (err cloudStatusError) Error() string {
	if err.Message != "" {
		return fmt.Sprintf("%s: cloud api returned status %d: %s", err.Operation, err.Status, err.Message)
	}
	return fmt.Sprintf("%s: cloud api returned status %d", err.Operation, err.Status)
}

func newCloudStatusError(operation string, status int, body []byte) cloudStatusError {
	message := cloudAPIErrorMessage(body)
	if hint := cloudAPIProxyHintForStatus(status); hint != "" {
		message = appendCloudAPIDiagnostic(message, hint)
	}
	return cloudStatusError{
		Operation: operation,
		Status:    status,
		Message:   message,
	}
}

func isInvalidMachineCredentialError(err error) bool {
	var statusErr cloudStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	if statusErr.Status == http.StatusUnauthorized {
		return true
	}
	if statusErr.Status != http.StatusForbidden {
		return false
	}
	message := strings.ToLower(statusErr.Message)
	return strings.Contains(message, "invalid") ||
		strings.Contains(message, "revoked") ||
		strings.Contains(message, "expired")
}

func cloudAPIErrorMessage(body []byte) string {
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		if message := strings.TrimSpace(envelope.Error.Message); message != "" {
			return message
		}
	}
	var legacy struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &legacy); err == nil {
		return strings.TrimSpace(legacy.Error)
	}
	return ""
}

func postCloudAPIJSON(ctx context.Context, client *http.Client, endpoint string, bearerToken string, payload any) (int, []byte, error) {
	return postCloudAPIJSONOnce(ctx, client, endpoint, bearerToken, payload, true)
}

func postCloudAPIJSONWithRetry(ctx context.Context, client *http.Client, endpoint string, bearerToken string, payload any) (int, []byte, error) {
	for attempt := 1; attempt <= cloudAPIMaxAttempts; attempt++ {
		status, body, err := postCloudAPIJSONOnce(ctx, client, endpoint, bearerToken, payload, attempt == cloudAPIMaxAttempts)
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil, fmt.Errorf("post cloud api request: %w", ctx.Err())
			}
			if attempt < cloudAPIMaxAttempts {
				if err := waitCloudAPIRetry(ctx, attempt); err != nil {
					return 0, nil, err
				}
				continue
			}
			return 0, nil, err
		}
		if isTransientCloudAPIStatus(status) && attempt < cloudAPIMaxAttempts {
			if err := waitCloudAPIRetry(ctx, attempt); err != nil {
				return 0, nil, err
			}
			continue
		}
		return status, body, nil
	}
	return 0, nil, errors.New("cloud api request exhausted retries")
}

func postCloudAPIJSONOnce(ctx context.Context, client *http.Client, endpoint string, bearerToken string, payload any, includeProxyHint bool) (int, []byte, error) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal cloud api request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, nil, fmt.Errorf("create cloud api request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		hint := ""
		if includeProxyHint {
			hint = cloudAPIProxyHint()
		}
		return 0, nil, fmt.Errorf("post cloud api request: %w%s", err, hint)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	closeErr := resp.Body.Close()
	if err != nil {
		return 0, nil, fmt.Errorf("read cloud api response: %w", err)
	}
	if closeErr != nil {
		return 0, nil, fmt.Errorf("close cloud api response: %w", closeErr)
	}
	return resp.StatusCode, data, nil
}

func isTransientCloudAPIStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func getCloudAPIJSON(ctx context.Context, client *http.Client, endpoint string, bearerToken string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("create cloud api request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("get cloud api request: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	closeErr := resp.Body.Close()
	if err != nil {
		return 0, nil, fmt.Errorf("read cloud api response: %w", err)
	}
	if closeErr != nil {
		return 0, nil, fmt.Errorf("close cloud api response: %w", closeErr)
	}
	return resp.StatusCode, data, nil
}

func waitCloudAPIRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt*attempt) * 100 * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("cloud api retry canceled: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func cloudAPIProxyHintForStatus(status int) string {
	if !isTransientCloudAPIStatus(status) {
		return ""
	}
	return cloudAPIProxyHint()
}

func cloudAPIProxyHint() string {
	names := activeProxyEnvNames()
	if len(names) == 0 {
		return ""
	}
	return fmt.Sprintf(". Proxy environment variables are set (%s). If pairing fails through the proxy, retry with: env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY wharf claude", strings.Join(names, ", "))
}

func activeProxyEnvNames() []string {
	proxyEnvNames := []string{"http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"}
	names := make([]string, 0, len(proxyEnvNames))
	for _, name := range proxyEnvNames {
		if os.Getenv(name) != "" {
			names = append(names, name)
		}
	}
	return names
}

func appendCloudAPIDiagnostic(message string, diagnostic string) string {
	if diagnostic == "" {
		return message
	}
	if strings.TrimSpace(message) == "" {
		return strings.TrimPrefix(diagnostic, ". ")
	}
	return message + diagnostic
}

func decodeCloudAPIJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func cloudAPIEndpoint(baseURL string, path string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse cloud api url: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("cloud api url must include scheme and host")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/" + strings.TrimLeft(path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func machinePairingDisplayURL(baseURL string, verificationURI string) string {
	for _, raw := range []string{verificationURI, baseURL} {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			continue
		}
		parsed.Path = "/app/machines"
		parsed.RawQuery = ""
		parsed.Fragment = ""
		return parsed.String()
	}
	return strings.TrimSpace(verificationURI)
}

func sameCloudAPIURL(left string, right string) bool {
	return strings.TrimRight(strings.TrimSpace(left), "/") == strings.TrimRight(strings.TrimSpace(right), "/")
}

func runWrapProvider(ctx context.Context, cfg wrapConfig, connection *hubConnection, masker *core.EventMasker, metrics *core.AdapterMetrics, authority *protocol.ConnectionAuthorityReceipt) error {
	stopHealth, err := core.StartFixedEntryHealth(ctx, cfg.HealthMarker)
	if err != nil {
		return err
	}
	defer stopHealth()
	if cfg.Format == "acp" {
		return runWrapACPProvider(ctx, cfg, connection, masker, metrics, authority)
	}

	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create provider stdin pipe: %w", err)
	}
	defer stdinReader.Close()
	defer stdinWriter.Close()
	stdoutReader, stdoutWriter := io.Pipe()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	writeFrame := func(frame protocol.Frame) error { return connection.write(runCtx, frame) }
	startAdmission := newProviderStartAdmission(cfg.ProtocolVersion, connection.read, writeFrame, metrics)
	var processAdmission core.ProcessStartAdmission
	if startAdmission != nil {
		processAdmission = startAdmission
	}
	command, err := providerProcessCommand(cfg, stdinReader, stdoutWriter, providerStderr(masker))
	if err != nil {
		return err
	}
	command.Credential = cfg.ProviderCredential
	maxRestarts := 0
	if cfg.Provider == "claude-code" && cfg.SecretDir != "" {
		maxRestarts = -1
	}
	processConfig := core.ProcessConfig{
		Command:        command,
		MaxRestarts:    maxRestarts,
		StartAdmission: processAdmission,
	}
	var supervisor *core.ProcessSupervisor
	var recoveryGroup *core.GroupSupervisor
	if startAdmission != nil {
		recoveryGroup, err = core.NewGroupSupervisor(core.GroupSupervisorConfig{
			MaxWorkers:                 1,
			AllowReferenceOnlyRecovery: true,
			NewWorker: func(workerCfg core.SessionWorkerConfig) (core.SessionWorkerRunner, error) {
				if supervisor == nil {
					return nil, errors.New("provider supervisor is unavailable")
				}
				workerCfg.Metrics = metrics
				return supervisor, nil
			},
		})
		if err != nil {
			return err
		}
	}
	if startAdmission != nil {
		processConfig, err = recoveryGroup.BindRecoveryStartAdmission(processConfig, startAdmission)
		if err != nil {
			return err
		}
	}
	supervisor, err = core.NewProcessSupervisor(processConfig)
	if err != nil {
		return err
	}

	processDone := make(chan error, 1)
	outputDone := make(chan error, 1)
	commandDone := make(chan error, 1)
	rotation := newCredentialRotationManager(runCtx, authority, writeFrame, connection.credentials, connection.currentAuthority)
	if rotation != nil {
		rotation.setActivationCallback(cfg.OnAdapterCredential)
	}
	heartbeatDone, observePong := startAdapterHeartbeat(runCtx, cfg.Heartbeat, writeFrame, func() {
		_ = rotation.requestIfDue(time.Now())
	})
	startSupervisor := func() {
		go func() {
			err := supervisor.Run(runCtx)
			_ = stdoutWriter.Close()
			processDone <- err
		}()
	}
	startSupervisor()
	metrics.SetWorkerCounts(1, 1, 0)
	defer metrics.SetWorkerCounts(0, 0, 0)
	if err := waitForFirstProviderStartAdmission(runCtx, startAdmission, processDone); err != nil {
		cancel()
		stopProviderSupervisor(supervisor)
		return err
	}
	if startAdmission != nil {
		if err := composeProviderGroupRecovery(runCtx, cfg, processConfig, supervisor, recoveryGroup, startAdmission); err != nil {
			cancel()
			stopProviderSupervisor(supervisor)
			return err
		}
	}
	removeReconnectRunControl := connection.setReconnectProposalFactory("run-control", func() (*protocol.Event, error) {
		return newRunControlCapabilityEvent(cfg)
	})
	defer removeReconnectRunControl()
	if err := publishRunControlCapability(runCtx, nil, cfg, writeFrame); err != nil {
		cancel()
		stopProviderSupervisor(supervisor)
		return err
	}
	go func() {
		outputDone <- streamProviderOutput(runCtx, cfg, stdoutReader, func(event protocol.Event) error {
			masked, err := maskEvent(masker, event)
			if err != nil {
				return err
			}
			metrics.IncMaskedEvent()
			return writeFrame(&masked)
		})
	}()
	var stopInProgress atomic.Bool
	go func() {
		commandDone <- forwardHubCommandsToProvider(runCtx, connection.read, stdinWriter, writeFrame, observePong, rotation, startAdmission, supervisor, &stopInProgress, cfg)
	}()

	commandFinished := false
	var commandErr error
	var processErr error
	var outputErr error
	processFinished := false
	outputFinished := false
	for {
		if processFinished && outputFinished {
			if stopInProgress.Load() && !commandFinished {
				// A successful Stop closes Provider stdout before its durable outcome
				// receipt arrives. Keep the Adapter transport alive until the command
				// goroutine confirms that Hub persistence completed.
			} else {
				cancel()
				_ = stdinWriter.Close()
				if processErr != nil {
					return processErr
				}
				if outputErr != nil {
					return outputErr
				}
				return commandErr
			}
		}
		select {
		case err := <-processDone:
			processFinished = true
			processErr = ignoreContextError(err)
			_ = stdinWriter.Close()
		case err := <-outputDone:
			outputFinished = true
			outputErr = ignoreContextError(err)
		case err := <-commandDone:
			commandFinished = true
			commandErr = ignoreContextError(err)
			if err != nil {
				cancel()
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = supervisor.Stop(stopCtx)
				stopCancel()
				return err
			}
		case err, ok := <-heartbeatDone:
			if ok && err != nil {
				cancel()
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = supervisor.Stop(stopCtx)
				stopCancel()
				return err
			}
		case <-ctx.Done():
			cancel()
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = supervisor.Stop(stopCtx)
			stopCancel()
			return fmt.Errorf("wrap provider context done (process_done=%t output_done=%t): %w", processFinished, outputFinished, ctx.Err())
		}
	}
}

func runWrapACPProvider(ctx context.Context, cfg wrapConfig, connection *hubConnection, masker *core.EventMasker, metrics *core.AdapterMetrics, authority *protocol.ConnectionAuthorityReceipt) error {
	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create provider stdin pipe: %w", err)
	}
	defer stdinReader.Close()
	defer stdinWriter.Close()
	stdoutReader, stdoutWriter := io.Pipe()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	writeFrame := func(frame protocol.Frame) error { return connection.write(runCtx, frame) }
	startAdmission := newProviderStartAdmission(cfg.ProtocolVersion, connection.read, writeFrame, metrics)
	var processAdmission core.ProcessStartAdmission
	if startAdmission != nil {
		processAdmission = startAdmission
	}
	command, err := providerProcessCommand(cfg, stdinReader, stdoutWriter, providerStderr(masker))
	if err != nil {
		return err
	}
	command.Credential = cfg.ProviderCredential
	maxRestarts := 0
	if cfg.Provider == "claude-code" && cfg.SecretDir != "" {
		maxRestarts = -1
	}
	processConfig := core.ProcessConfig{
		Command:        command,
		MaxRestarts:    maxRestarts,
		StartAdmission: processAdmission,
	}
	var supervisor *core.ProcessSupervisor
	var recoveryGroup *core.GroupSupervisor
	if startAdmission != nil {
		recoveryGroup, err = core.NewGroupSupervisor(core.GroupSupervisorConfig{
			MaxWorkers:                 1,
			AllowReferenceOnlyRecovery: true,
			NewWorker: func(workerCfg core.SessionWorkerConfig) (core.SessionWorkerRunner, error) {
				if supervisor == nil {
					return nil, errors.New("provider supervisor is unavailable")
				}
				workerCfg.Metrics = metrics
				return supervisor, nil
			},
		})
		if err != nil {
			return err
		}
	}
	if startAdmission != nil {
		processConfig, err = recoveryGroup.BindRecoveryStartAdmission(processConfig, startAdmission)
		if err != nil {
			return err
		}
	}
	supervisor, err = core.NewProcessSupervisor(processConfig)
	if err != nil {
		return err
	}

	processDone := make(chan error, 1)
	startSupervisor := func() {
		go func() {
			err := supervisor.Run(runCtx)
			_ = stdoutWriter.Close()
			processDone <- err
		}()
	}
	startSupervisor()
	metrics.SetWorkerCounts(1, 1, 0)
	defer metrics.SetWorkerCounts(0, 0, 0)
	if err := waitForFirstProviderStartAdmission(runCtx, startAdmission, processDone); err != nil {
		cancel()
		stopProviderSupervisor(supervisor)
		return err
	}
	if startAdmission != nil {
		if err := composeProviderGroupRecovery(runCtx, cfg, processConfig, supervisor, recoveryGroup, startAdmission); err != nil {
			cancel()
			stopProviderSupervisor(supervisor)
			return err
		}
	}
	removeReconnectRunControl := connection.setReconnectProposalFactory("run-control", func() (*protocol.Event, error) {
		return newRunControlCapabilityEvent(cfg)
	})
	defer removeReconnectRunControl()
	if err := publishRunControlCapability(runCtx, nil, cfg, writeFrame); err != nil {
		cancel()
		stopProviderSupervisor(supervisor)
		return err
	}

	scanner := bufio.NewScanner(stdoutReader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	if err := writeACPRequest(stdinWriter, 1, "initialize", map[string]any{
		"protocolVersion": 1,
		"clientInfo": map[string]any{
			"name":    "agentwharf",
			"version": buildinfo.Version,
		},
		"clientCapabilities": map[string]any{
			"fs": map[string]any{
				"readTextFile":  true,
				"writeTextFile": true,
			},
			"terminal": false,
		},
	}); err != nil {
		cancel()
		return err
	}
	if _, err := readACPResponse(runCtx, scanner, 1); err != nil {
		cancel()
		return err
	}

	cwd, err := providerWorkingDirectory(cfg.WorkingDirectory)
	if err != nil {
		cancel()
		return err
	}
	sessionMethod := "session/new"
	sessionParams := map[string]any{"cwd": cwd, "mcpServers": []any{}}
	if strings.TrimSpace(cfg.ProviderSessionID) != "" {
		sessionMethod = "session/load"
		sessionParams["sessionId"] = cfg.ProviderSessionID
	}
	if err := writeACPRequest(stdinWriter, 2, sessionMethod, sessionParams); err != nil {
		cancel()
		return err
	}
	sessionResult, err := readACPResponse(runCtx, scanner, 2)
	if err != nil {
		if sessionMethod != "session/load" {
			cancel()
			return err
		}
		// A provider may not implement loadSession, or the opaque id may have
		// expired. Consume that error and start a fresh provider context while
		// keeping the same Hub Session and durable transcript.
		if cfg.Stderr != nil {
			_, _ = fmt.Fprintf(cfg.Stderr, "agentwharf: provider session load failed; starting a new provider context\n")
		}
		if err := writeACPRequest(stdinWriter, 3, "session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}}); err != nil {
			cancel()
			return err
		}
		sessionResult, err = readACPResponse(runCtx, scanner, 3)
		if err != nil {
			cancel()
			return err
		}
	}
	providerSessionID := stringFieldFromAny(sessionResult["sessionId"])
	if providerSessionID == "" {
		cancel()
		return errors.New("acp session/new response missing sessionId")
	}
	if cfg.OnProviderSession != nil {
		cfg.OnProviderSession(providerSessionID)
	}
	settingsTracker := newACPSettingsTracker(sessionResult)
	if cfg.ProtocolVersion == protocol.ProtocolVersionV2 && cfg.LaunchSettings.requested() {
		applyACPLaunchSettings(runCtx, settingsTracker, providerSessionID, stdinWriter, scanner, cfg.LaunchSettings, cfg.Stderr)
	}
	var settingsMu sync.Mutex
	removeReconnectSettings := connection.setReconnectProposalFactory("settings", func() (*protocol.Event, error) {
		if cfg.ProtocolVersion != protocol.ProtocolVersionV2 {
			return nil, nil
		}
		settingsMu.Lock()
		state, ok := settingsTracker.Current()
		settingsMu.Unlock()
		if !ok {
			return nil, nil
		}
		return newACPSettingsCapabilityEvent(cfg.SessionID, state)
	})
	defer removeReconnectSettings()
	if cfg.ProtocolVersion == protocol.ProtocolVersionV2 {
		if state, ok := settingsTracker.Current(); ok {
			if err := publishACPSettingsCapability(writeFrame, cfg.SessionID, state); err != nil {
				cancel()
				return fmt.Errorf("publish initial acp settings capability: %w", err)
			}
		}
	}
	readyProposalID, err := sendACPProviderReadyEvent(runCtx, writeFrame, cfg, providerSessionID, cwd, masker, metrics)
	if err != nil {
		cancel()
		return err
	}
	if cfg.StartupSmoke {
		if readyProposalID == "" {
			stopProviderSupervisor(supervisor)
			return errors.New("startup smoke requires a durable v2 Provider ready proposal")
		}
		if err := waitStartupSmokeReceipt(runCtx, connection.read, writeFrame, readyProposalID, "Provider ready"); err != nil {
			stopProviderSupervisor(supervisor)
			return err
		}
		if err := publishStartupSmokeEnded(runCtx, connection.read, writeFrame, cfg); err != nil {
			stopProviderSupervisor(supervisor)
			return err
		}
		stopProviderSupervisor(supervisor)
		return nil
	}

	outputDone := make(chan error, 1)
	commandDone := make(chan error, 1)
	var permissionMu sync.Mutex
	pendingPermissions := make(map[string]acpPendingPermission)
	responses := newACPResponseRouter()
	rotation := newCredentialRotationManager(runCtx, authority, writeFrame, connection.credentials, connection.currentAuthority)
	if rotation != nil {
		rotation.setActivationCallback(cfg.OnAdapterCredential)
	}
	heartbeatDone, observePong := startAdapterHeartbeat(runCtx, cfg.Heartbeat, writeFrame, func() {
		_ = rotation.requestIfDue(time.Now())
	})
	go func() {
		outputDone <- streamACPProviderOutput(runCtx, cfg, scanner, responses, func(line []byte, sourceSequence uint64) error {
			trackACPPermissionRequest(line, pendingPermissions, &permissionMu)
			settingsMu.Lock()
			defer settingsMu.Unlock()
			state, changed, err := settingsTracker.ObserveProviderLine(line, providerSessionID, sourceSequence)
			if err != nil {
				return fmt.Errorf("observe acp settings update: %w", err)
			}
			if cfg.ProtocolVersion == protocol.ProtocolVersionV2 && changed {
				return publishACPSettingsCapability(writeFrame, cfg.SessionID, state)
			}
			return nil
		}, func(event protocol.Event) error {
			masked, err := maskEvent(masker, event)
			if err != nil {
				return err
			}
			metrics.IncMaskedEvent()
			return writeFrame(&masked)
		})
	}()
	var stopInProgress atomic.Bool
	go func() {
		commandDone <- forwardHubCommandsToACPProvider(runCtx, connection.read, stdinWriter, writeFrame, observePong, rotation, providerSessionID, 3, pendingPermissions, &permissionMu, responses, settingsTracker, &settingsMu, startAdmission, supervisor, &stopInProgress, cfg)
	}()

	commandFinished := false
	var commandErr error
	processFinished := false
	outputFinished := false
	var processErr error
	var outputErr error
	for {
		if processFinished && outputFinished {
			if stopInProgress.Load() && !commandFinished {
				// Wait for the Stop outcome receipt before allowing a clean Provider
				// exit to tear down the Adapter transport.
			} else {
				cancel()
				_ = stdinWriter.Close()
				if processErr != nil {
					return processErr
				}
				if outputErr != nil {
					return outputErr
				}
				return commandErr
			}
		}
		select {
		case err := <-processDone:
			processFinished = true
			processErr = ignoreContextError(err)
			_ = stdinWriter.Close()
		case err := <-outputDone:
			outputFinished = true
			outputErr = ignoreContextError(err)
			if outputErr != nil {
				cancel()
				stopProviderSupervisor(supervisor)
				return outputErr
			}
		case err := <-commandDone:
			commandFinished = true
			commandErr = ignoreContextError(err)
			if err != nil {
				cancel()
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = supervisor.Stop(stopCtx)
				stopCancel()
				return err
			}
		case err, ok := <-heartbeatDone:
			if ok && err != nil {
				cancel()
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = supervisor.Stop(stopCtx)
				stopCancel()
				return err
			}
		case <-ctx.Done():
			cancel()
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = supervisor.Stop(stopCtx)
			stopCancel()
			return fmt.Errorf("wrap acp provider context done (process_done=%t output_done=%t): %w", processFinished, outputFinished, ctx.Err())
		}
	}
}

// providerStartAdmission is the Adapter-side, per-child handshake. The first
// exchange owns the socket reader directly; later exchanges are delivered by
// the command reader so ProcessSupervisor retries cannot race command routing.
type providerStartAdmission struct {
	read    func(context.Context) (protocol.Frame, error)
	write   func(protocol.Frame) error
	metrics *core.AdapterMetrics

	mu             sync.Mutex
	direct         bool
	recoveryHandle string
	prepare        chan *protocol.ProviderStartPrepare
	ack            chan *protocol.ProviderStartAck
	firstAdmitted  chan struct{}
	firstAdmitOnce sync.Once
	invalidated    bool
}

func newProviderStartAdmission(version int, read func(context.Context) (protocol.Frame, error), write func(protocol.Frame) error, metrics *core.AdapterMetrics) *providerStartAdmission {
	if version != protocol.ProtocolVersionV2 || read == nil || write == nil {
		return nil
	}
	return &providerStartAdmission{
		read: read, write: write, metrics: metrics, direct: true,
		prepare: make(chan *protocol.ProviderStartPrepare, 1), ack: make(chan *protocol.ProviderStartAck, 1),
		firstAdmitted: make(chan struct{}),
	}
}

func (a *providerStartAdmission) receiptFailure(err error) error {
	if a != nil && a.metrics != nil && err != nil {
		a.metrics.IncReceiptFailure()
	}
	return err
}

func (a *providerStartAdmission) PrepareProcessStart(ctx context.Context, attempt int) error {
	if a == nil || attempt < 1 {
		return a.receiptFailure(errors.New("provider start admission is unavailable"))
	}
	a.mu.Lock()
	direct := a.direct
	a.mu.Unlock()
	if err := a.write(&protocol.ProviderStart{Attempt: attempt}); err != nil {
		return a.receiptFailure(fmt.Errorf("request provider start admission: %w", err))
	}
	prepare, err := a.nextPrepare(ctx, direct)
	if err != nil || prepare.Attempt != attempt {
		return a.receiptFailure(errors.New("provider start preparation rejected"))
	}
	return nil
}

func (a *providerStartAdmission) ConfirmProcessStarted(ctx context.Context, attempt int) error {
	if a == nil || attempt < 1 {
		return a.receiptFailure(errors.New("provider start admission is unavailable"))
	}
	a.mu.Lock()
	direct := a.direct
	a.mu.Unlock()
	if err := a.write(&protocol.ProviderStartStarted{Attempt: attempt}); err != nil {
		return a.receiptFailure(fmt.Errorf("confirm provider start: %w", err))
	}
	ack, err := a.nextAck(ctx, direct)
	if err != nil || ack.Attempt != attempt || ack.Status != protocol.ProviderStartAdmitted || ack.RecoveryHandle == "" {
		return a.receiptFailure(errors.New("provider start admission rejected"))
	}
	a.mu.Lock()
	if a.invalidated {
		a.mu.Unlock()
		return a.receiptFailure(errors.New("provider start authority is unavailable"))
	}
	a.direct = false
	a.recoveryHandle = ack.RecoveryHandle
	a.mu.Unlock()
	a.firstAdmitOnce.Do(func() { close(a.firstAdmitted) })
	return nil
}

func (a *providerStartAdmission) nextPrepare(ctx context.Context, direct bool) (*protocol.ProviderStartPrepare, error) {
	if direct {
		frame, err := a.read(ctx)
		if err != nil {
			return nil, err
		}
		prepare, ok := frame.(*protocol.ProviderStartPrepare)
		if !ok {
			return nil, errors.New("provider start preparation rejected")
		}
		return prepare, nil
	}
	select {
	case prepare := <-a.prepare:
		return prepare, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *providerStartAdmission) nextAck(ctx context.Context, direct bool) (*protocol.ProviderStartAck, error) {
	if direct {
		frame, err := a.read(ctx)
		if err != nil {
			return nil, err
		}
		ack, ok := frame.(*protocol.ProviderStartAck)
		if !ok {
			return nil, errors.New("provider start admission rejected")
		}
		return ack, nil
	}
	select {
	case ack := <-a.ack:
		return ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *providerStartAdmission) deliver(frame protocol.Frame) error {
	if a == nil {
		return errors.New("unexpected provider start lifecycle frame")
	}
	switch typed := frame.(type) {
	case *protocol.ProviderStartPrepare:
		select {
		case a.prepare <- typed:
			return nil
		default:
			return errors.New("unexpected provider start preparation")
		}
	case *protocol.ProviderStartAck:
		select {
		case a.ack <- typed:
			return nil
		default:
			return errors.New("unexpected provider start acknowledgement")
		}
	default:
		return errors.New("unexpected provider start lifecycle frame")
	}
}

// RecoveryStartHandle returns only the opaque committed-start reference that a
// later GroupSupervisor recovery fence may compare. It cannot expose a Store
// key, credential, path, content, or Provider configuration.
func (a *providerStartAdmission) RecoveryStartHandle() (core.RecoveryStartHandle, error) {
	if a == nil {
		return core.RecoveryStartHandle{}, errors.New("provider start admission is unavailable")
	}
	a.mu.Lock()
	if a.invalidated {
		a.mu.Unlock()
		return core.RecoveryStartHandle{}, errors.New("provider start authority is unavailable")
	}
	handle := a.recoveryHandle
	a.mu.Unlock()
	return core.NewRecoveryStartHandle(handle)
}

// VerifyRecoveryStart turns the Hub's Store-backed connection lifecycle into
// the local recovery fence. Hub closes this socket when replacement,
// revocation, terminalization, expiry or quarantine invalidates the durable
// tuple; no bearer or Store key is carried into the Adapter.
func (a *providerStartAdmission) VerifyRecoveryStart(ctx context.Context) error {
	if a == nil || a.read == nil {
		return errors.New("provider start authority is unavailable")
	}
	if _, err := a.RecoveryStartHandle(); err != nil {
		return err
	}
	return nil
}

func (a *providerStartAdmission) invalidate() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.invalidated = true
	a.recoveryHandle = ""
	a.mu.Unlock()
}

// composeProviderGroupRecovery is the production composition point for the
// real GroupSupervisor.Recover path. The existing ProcessSupervisor remains
// the child runner; GroupSupervisor stores only session membership and the
// opaque reference, while Hub/Store remains the durable authority.

func composeProviderGroupRecovery(ctx context.Context, cfg wrapConfig, processConfig core.ProcessConfig, supervisor *core.ProcessSupervisor, group *core.GroupSupervisor, admission *providerStartAdmission) error {
	if supervisor == nil || group == nil || admission == nil {
		return errors.New("provider recovery composition is unavailable")
	}
	handle, err := admission.RecoveryStartHandle()
	if err != nil {
		return err
	}
	recovery := core.GroupWorkerRecovery{
		Admission: core.GroupWorkerAdmission{
			WorkerID:  cfg.SessionID,
			SessionID: cfg.SessionID,
			Worker: core.SessionWorkerConfig{
				SessionID: cfg.SessionID,
				Provider:  processConfig,
			},
		},
		StartHandle:       handle,
		StartHandleSource: admission,
	}
	if err := group.Recover(ctx, recovery); err != nil {
		return err
	}
	return nil
}

func waitForFirstProviderStartAdmission(ctx context.Context, admission *providerStartAdmission, processDone <-chan error) error {
	if admission == nil {
		return nil
	}
	select {
	case <-admission.firstAdmitted:
		return nil
	default:
	}
	select {
	case <-admission.firstAdmitted:
		return nil
	case err := <-processDone:
		select {
		case <-admission.firstAdmitted:
			return nil
		default:
		}
		if err == nil {
			return errors.New("provider exited before start admission")
		}
		return err
	case <-ctx.Done():
		return fmt.Errorf("wait provider start admission: %w", ctx.Err())
	}
}

func stopProviderSupervisor(supervisor *core.ProcessSupervisor) {
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	_ = supervisor.Stop(stopCtx)
}

// cwdEventBasename reduces an absolute working directory to its final segment
// for transmission in a durable protocol event.
//
// The full path is host content: it carries the operating-system account name
// and the operator's whole directory layout, and `session.state` is durable, so
// an absolute path would be retained in the EventStore and every replay of it.
// `docs/03-sandbox-security.md` already forbids 文件路径 in records that leave
// Adapter-only state, and the owner-approved decision for this field is
// basename only. The reduction therefore happens here, before the event is
// built, rather than in a consumer: a client-side basename would still leave
// the full path durably stored.
//
// Control characters are stripped and both slash styles are split so a
// Windows-style path reduces identically. A trailing ".." leaves the effective
// directory undeterminable from the string alone, and an over-long segment
// cannot be a real directory name, so both yield "" and the field is omitted
// rather than carrying something misleading. The result is idempotent: feeding
// an already-reduced basename back through returns it unchanged.
func cwdEventBasename(raw string) string {
	sanitized := strings.Map(func(r rune) rune {
		if r <= 0x1f || r == 0x7f {
			return -1
		}
		return r
	}, raw)
	segments := strings.FieldsFunc(sanitized, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	if len(segments) == 0 || segments[len(segments)-1] == ".." {
		return ""
	}
	basename := segments[len(segments)-1]
	if len(basename) > 255 {
		return ""
	}
	return basename
}

// acpProviderReadyPayload builds the durable `session.state` ready payload.
//
// Kept separate from the send path so the reduction of cwd is testable without
// a websocket: a test that rebuilt this map itself would assert only against its
// own copy and would keep passing if the real payload regressed to the full
// path.
func acpProviderReadyPayload(provider string, providerSessionID string, cwd string) ([]byte, error) {
	// Omit the key entirely when the path cannot be reduced safely, so a
	// consumer sees an absent field rather than an empty or misleading one.
	metadata := map[string]any{}
	if basename := cwdEventBasename(cwd); basename != "" {
		metadata["cwd"] = basename
	}
	return json.Marshal(map[string]any{
		"state":               "ready",
		"provider":            provider,
		"provider_session_id": providerSessionID,
		"metadata":            metadata,
		"source":              "acp",
	})
}

func sendACPProviderReadyEvent(ctx context.Context, writeFrame func(protocol.Frame) error, cfg wrapConfig, providerSessionID string, cwd string, masker *core.EventMasker, metrics *core.AdapterMetrics) (string, error) {
	payload, err := acpProviderReadyPayload(cfg.Provider, providerSessionID, cwd)
	if err != nil {
		return "", fmt.Errorf("marshal acp ready event: %w", err)
	}
	event := protocol.Event{
		Type:      "session.state",
		SessionID: cfg.SessionID,
		Time:      time.Now().UTC().UnixMilli(),
		Payload:   payload,
	}
	if cfg.ProtocolVersion == protocol.ProtocolVersionV2 {
		event.ProposalID, err = randomToken()
		if err != nil {
			return "", fmt.Errorf("generate acp ready proposal: %w", err)
		}
	}
	event, err = maskEvent(masker, event)
	if err != nil {
		return "", err
	}
	metrics.IncMaskedEvent()
	if err := writeFrame(&event); err != nil {
		return "", fmt.Errorf("send acp ready event: %w", err)
	}
	return event.ProposalID, nil
}

func publishStartupSmokeEnded(ctx context.Context, readFrame func(context.Context) (protocol.Frame, error), writeFrame func(protocol.Frame) error, cfg wrapConfig) error {
	proposalID, err := randomToken()
	if err != nil {
		return fmt.Errorf("generate startup smoke terminal proposal: %w", err)
	}
	payload, err := json.Marshal(map[string]any{"state": "ended", "reason": "startup_smoke_complete"})
	if err != nil {
		return fmt.Errorf("marshal startup smoke terminal event: %w", err)
	}
	if err := writeFrame(&protocol.Event{
		Type: "session.state", SessionID: cfg.SessionID, Time: time.Now().UTC().UnixMilli(),
		Payload: payload, ProposalID: proposalID,
	}); err != nil {
		return fmt.Errorf("publish startup smoke terminal event: %w", err)
	}
	return waitStartupSmokeReceipt(ctx, readFrame, writeFrame, proposalID, "terminal")
}

func waitStartupSmokeReceipt(ctx context.Context, readFrame func(context.Context) (protocol.Frame, error), writeFrame func(protocol.Frame) error, proposalID string, stage string) error {
	return waitEventReceipt(ctx, readFrame, writeFrame, proposalID, "startup smoke "+stage)
}

func waitEventReceipt(ctx context.Context, readFrame func(context.Context) (protocol.Frame, error), writeFrame func(protocol.Frame) error, proposalID string, label string) error {
	for {
		frame, err := readFrame(ctx)
		if err != nil {
			return fmt.Errorf("read %s receipt: %w", label, err)
		}
		switch typed := frame.(type) {
		case *protocol.EventReceipt:
			if typed.ProposalID != proposalID {
				continue
			}
			if typed.Status != protocol.EventReceiptAccepted {
				return fmt.Errorf("%s receipt rejected", label)
			}
			return nil
		case *protocol.Ping:
			if err := writeFrame(&protocol.Pong{Nonce: typed.Nonce}); err != nil {
				return fmt.Errorf("send %s pong: %w", label, err)
			}
		case *protocol.Error:
			return fmt.Errorf("%s event rejected: %s", label, typed.Code)
		}
	}
}

func publishRunControlCapability(ctx context.Context, conn *websocket.Conn, cfg wrapConfig, writeFrame func(protocol.Frame) error) error {
	event, err := newRunControlCapabilityEvent(cfg)
	if err != nil || event == nil {
		return err
	}
	return writeFrame(event)
}

func newRunControlCapabilityEvent(cfg wrapConfig) (*protocol.Event, error) {
	if cfg.ProtocolVersion != protocol.ProtocolVersionV2 {
		return nil, nil
	}
	proposalID, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("generate run-control capability proposal: %w", err)
	}
	payload, err := json.Marshal(protocol.RunControlCapabilityPayload{
		SchemaVersion: 1, InterruptSupported: true, StopSupported: true,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal run-control capability: %w", err)
	}
	return &protocol.Event{
		Type: "session.run.capabilities", SessionID: cfg.SessionID,
		Time: time.Now().UTC().UnixMilli(), Payload: payload, ProposalID: proposalID,
	}, nil
}

func handleProviderRunControl(ctx context.Context, command *protocol.Command, supervisor *core.ProcessSupervisor, readFrame func(context.Context) (protocol.Frame, error), writeFrame func(protocol.Frame) error, stopInProgress *atomic.Bool, cfg wrapConfig, stop bool) error {
	if command == nil || supervisor == nil {
		return errors.New("run-control provider is unavailable")
	}
	operation := "interrupt"
	completionState := "ready"
	if stop {
		operation = "stop"
		completionState = "ended"
	}
	var operationErr error
	if stop {
		if stopInProgress != nil {
			stopInProgress.Store(true)
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		operationErr = supervisor.Stop(stopCtx)
		cancel()
	} else {
		operationErr = supervisor.Interrupt(ctx)
	}
	return acknowledgeRunControl(ctx, command, readFrame, writeFrame, cfg, operation, completionState, operationErr)
}

func acknowledgeRunControl(ctx context.Context, command *protocol.Command, readFrame func(context.Context) (protocol.Frame, error), writeFrame func(protocol.Frame) error, cfg wrapConfig, operation, completionState string, operationErr error) error {
	if command == nil || readFrame == nil || writeFrame == nil {
		return errors.New("run-control acknowledgement is unavailable")
	}
	if err := writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted}); err != nil {
		return fmt.Errorf("ack run-control command %s: %w", command.CommandID, err)
	}
	outcome := "completed"
	var completion *string
	var reason *string
	if operationErr == nil {
		completion = &completionState
	} else if errors.Is(operationErr, context.DeadlineExceeded) {
		outcome = "timeout"
		code := "run_control_timeout"
		reason = &code
	} else if operation == "stop" {
		outcome = "outcome_unknown"
		code := "adapter_cleanup_uncertain"
		reason = &code
	} else {
		outcome = "rejected"
		code := "provider_rejected"
		reason = &code
	}
	proposalID, err := randomToken()
	if err != nil {
		return fmt.Errorf("generate run-control outcome proposal: %w", err)
	}
	payload, err := json.Marshal(map[string]any{
		"cmd_id": command.CommandID, "operation": operation, "outcome": outcome,
		"completion_state": completion, "reason_code": reason,
	})
	if err != nil {
		return fmt.Errorf("marshal run-control outcome: %w", err)
	}
	if err := writeFrame(&protocol.Event{
		Type: "session.run.outcome", SessionID: cfg.SessionID,
		Time: time.Now().UTC().UnixMilli(), Payload: payload, ProposalID: proposalID,
	}); err != nil {
		return fmt.Errorf("publish run-control outcome %s: %w", command.CommandID, err)
	}
	receiptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := waitEventReceipt(receiptCtx, readFrame, writeFrame, proposalID, "run-control outcome "+command.CommandID); err != nil {
		return err
	}
	return nil
}

const maxProviderCredentialBytes = 64 * 1024

func providerStderr(masker *core.EventMasker) io.Writer {
	if masker == nil {
		return os.Stderr
	}
	return masker.MaskWriter(nonClosingWriter{Writer: os.Stderr})
}

type nonClosingWriter struct{ io.Writer }

func (nonClosingWriter) Close() error { return nil }

// providerWorkingDirectory resolves the directory the provider should work in.
// An empty or "." value keeps the process's own working directory; a relative
// value is resolved against it so the ACP session/new cwd is always absolute.
func providerWorkingDirectory(value string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get provider cwd: %w", err)
	}
	if strings.TrimSpace(value) == "" || value == "." {
		return cwd, nil
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("resolve provider working directory: %w", err)
	}
	return filepath.Clean(abs), nil
}

// providerProcessCommand keeps the sandbox environment file-path-only. The
// approved Claude provider bridge reads those paths only for its child process.
func providerProcessCommand(cfg wrapConfig, stdin io.Reader, stdout io.Writer, stderr io.Writer) (core.ProcessCommand, error) {
	if len(cfg.ProviderCommand) == 0 {
		return core.ProcessCommand{}, errors.New("provider command is required")
	}
	env, err := providerChildEnvironment(cfg, os.Environ())
	if err != nil {
		return core.ProcessCommand{}, err
	}
	return core.ProcessCommand{Path: cfg.ProviderCommand[0], Args: cfg.ProviderCommand[1:], Env: env, Stdin: stdin, Stdout: stdout, Stderr: stderr}, nil
}

// providerChildEnvironment forwards ANTHROPIC_* environment to the provider
// child. ANTHROPIC_AUTH_TOKEN and ANTHROPIC_BASE_URL keep the strict legacy
// contract: their parent values must be bounded regular files inside the
// injected secret directory. Any other ANTHROPIC_* name (model mappings,
// custom headers from a provider configuration file) is file-loaded when its
// value resolves inside the secret directory and otherwise passed through
// verbatim, so the sandbox environment stays file-path-only for secrets.
func providerChildEnvironment(cfg wrapConfig, parent []string) ([]string, error) {
	env := make([]string, 0, 4)
	if cfg.Provider != "claude-code" || cfg.SecretDir == "" {
		return env, nil
	}
	for _, entry := range parent {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || name == "" || value == "" || !strings.HasPrefix(name, "ANTHROPIC_") {
			continue
		}
		strictCredential := name == "ANTHROPIC_AUTH_TOKEN" || name == "ANTHROPIC_BASE_URL"
		if strictCredential {
			loaded, err := readProviderCredentialFile(cfg.SecretDir, value, name == "ANTHROPIC_AUTH_TOKEN")
			if err != nil {
				return nil, fmt.Errorf("load %s for provider child: %w", name, err)
			}
			env = append(env, name+"="+loaded)
			continue
		}
		if loaded, ok := readProviderConfigFile(cfg.SecretDir, value); ok {
			value = loaded
		}
		env = append(env, name+"="+value)
	}
	return env, nil
}

// readProviderConfigFile loads an ANTHROPIC_* configuration file inside the
// secret directory without the credential minimum length; it reports false
// when the value is not a bounded regular file inside that directory so the
// value can pass through verbatim.
func readProviderConfigFile(secretDir string, valuePath string) (string, bool) {
	if !filepath.IsAbs(valuePath) {
		return "", false
	}
	root, err := filepath.EvalSymlinks(filepath.Clean(secretDir))
	if err != nil {
		return "", false
	}
	resolvedPath, err := filepath.EvalSymlinks(filepath.Clean(valuePath))
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(root, resolvedPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", false
	}
	info, err := os.Stat(resolvedPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxProviderCredentialBytes {
		return "", false
	}
	data, err := os.ReadFile(resolvedPath)
	if err != nil || len(data) > maxProviderCredentialBytes {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r"), true
}

func environmentValue(env []string, name string) string {
	prefix := name + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func readProviderCredentialFile(secretDir string, valuePath string, requireMinSecretLength bool) (string, error) {
	if !filepath.IsAbs(secretDir) || !filepath.IsAbs(valuePath) {
		return "", errors.New("secret directory and credential path must be absolute")
	}
	root, err := filepath.EvalSymlinks(filepath.Clean(secretDir))
	if err != nil {
		return "", fmt.Errorf("resolve secret directory: %w", err)
	}
	path := filepath.Clean(valuePath)
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve credential file: %w", err)
	}
	rel, err := filepath.Rel(root, resolvedPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", errors.New("credential path is outside the injected secret directory")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open credential file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("stat credential file: %w", err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("recheck credential file: %w", err)
	}
	if !info.Mode().IsRegular() || !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, pathInfo) || info.Size() > maxProviderCredentialBytes {
		return "", errors.New("credential file must be a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxProviderCredentialBytes+1))
	if err != nil {
		return "", fmt.Errorf("read credential file: %w", err)
	}
	if len(data) > maxProviderCredentialBytes {
		return "", errors.New("credential file must be a bounded regular file")
	}
	value := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if strings.IndexByte(value, 0) >= 0 {
		return "", errors.New("credential file must not contain NUL bytes")
	}
	if requireMinSecretLength && len(value) < masking.MinSecretLength {
		return "", fmt.Errorf("credential file must contain at least %d bytes of text", masking.MinSecretLength)
	}
	return value, nil
}

func eventMaskerFromSecretDir(dir string) (*core.EventMasker, error) {
	if dir == "" {
		return core.NewEventMasker(nil), nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read secret dir: %w", err)
	}
	secrets := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat secret file %s: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read secret file %s: %w", entry.Name(), err)
		}
		if len(data) > 0 {
			secrets = append(secrets, string(data))
			trimmed := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
			if trimmed != "" && trimmed != string(data) {
				secrets = append(secrets, trimmed)
			}
		}
	}
	return core.NewEventMasker(secrets), nil
}

func maskEvent(masker *core.EventMasker, event protocol.Event) (protocol.Event, error) {
	if masker == nil {
		return event, nil
	}
	masked, err := masker.MaskEvent(event)
	if err != nil {
		return protocol.Event{}, fmt.Errorf("mask event %s: %w", event.Type, err)
	}
	return masked, nil
}

func translateWrapInput(ctx context.Context, cfg wrapConfig, stdin io.Reader) ([]protocol.Event, error) {
	switch cfg.Format {
	case "jsonstream":
		translator, err := jsonstream.NewTranslator(jsonstream.Config{
			SessionID: cfg.SessionID,
			Provider:  cfg.Provider,
		})
		if err != nil {
			return nil, err
		}
		return translator.TranslateReader(ctx, stdin)
	case "acp":
		mapper, err := acp.NewMapper(acp.Config{
			SessionID: cfg.SessionID,
			Provider:  cfg.Provider,
		})
		if err != nil {
			return nil, err
		}
		return mapper.MapReader(ctx, stdin)
	default:
		return nil, fmt.Errorf("unsupported wrap format %q", cfg.Format)
	}
}

func streamProviderOutput(ctx context.Context, cfg wrapConfig, stdout io.Reader, send func(protocol.Event) error) error {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		events, err := translateWrapLine(cfg, scanner.Bytes())
		if err != nil {
			return err
		}
		for _, event := range events {
			if err := send(event); err != nil {
				return fmt.Errorf("send provider event %s: %w", event.Type, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan provider stdout: %w", err)
	}
	return nil
}

func translateWrapLine(cfg wrapConfig, line []byte) ([]protocol.Event, error) {
	switch cfg.Format {
	case "jsonstream":
		translator, err := jsonstream.NewTranslator(jsonstream.Config{
			SessionID: cfg.SessionID,
			Provider:  cfg.Provider,
		})
		if err != nil {
			return nil, err
		}
		return translator.TranslateLine(line)
	case "acp":
		mapper, err := acp.NewMapper(acp.Config{
			SessionID: cfg.SessionID,
			Provider:  cfg.Provider,
		})
		if err != nil {
			return nil, err
		}
		return mapper.MapLine(line)
	default:
		return nil, fmt.Errorf("unsupported wrap format %q", cfg.Format)
	}
}

func forwardHubCommandsToProvider(ctx context.Context, readFrame func(context.Context) (protocol.Frame, error), stdin io.WriteCloser, writeFrame func(protocol.Frame) error, observePong func(string), rotation *credentialRotationManager, startAdmission *providerStartAdmission, supervisor *core.ProcessSupervisor, stopInProgress *atomic.Bool, cfg wrapConfig) error {
	defer stdin.Close()
	accepted := newAcceptedCommandSet(2048)
	for {
		frame, err := readFrame(ctx)
		if err != nil {
			return ignoreContextError(err)
		}
		switch typed := frame.(type) {
		case *protocol.CredentialRotationCredential:
			rotation.recordCredentialExpiry(typed)
			if err := rotation.handle(ctx, typed); err != nil {
				return err
			}
		case *protocol.CredentialRotationActivation:
			if err := rotation.handle(ctx, typed); err != nil {
				return err
			}
		case *protocol.ProviderStartPrepare, *protocol.ProviderStartAck:
			if err := startAdmission.deliver(typed); err != nil {
				return err
			}
		case *protocol.Command:
			if accepted.Contains(typed.CommandID) {
				if err := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}); err != nil {
					return fmt.Errorf("re-ack provider command %s: %w", typed.CommandID, err)
				}
				continue
			}
			if typed.Type == protocol.CommandSessionInterrupt || typed.Type == protocol.CommandSessionStop {
				return handleProviderRunControl(ctx, typed, supervisor, readFrame, writeFrame, stopInProgress, cfg, typed.Type == protocol.CommandSessionStop)
			}
			if err := writeProviderCommand(stdin, typed); err != nil {
				return err
			}
			accepted.Add(typed.CommandID)
			ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}
			if err := writeFrame(&ack); err != nil {
				return fmt.Errorf("ack provider command %s: %w", typed.CommandID, err)
			}
		case *protocol.Ping:
			if err := writeFrame(&protocol.Pong{Nonce: typed.Nonce}); err != nil {
				return fmt.Errorf("send pong: %w", err)
			}
		case *protocol.Pong:
			observePong(typed.Nonce)
		case *protocol.Error:
			return fmt.Errorf("hub error %s: %s", typed.Code, typed.Message)
		}
	}
}

func writeProviderCommand(stdin io.Writer, cmd *protocol.Command) error {
	data, err := protocol.Encode(cmd)
	if err != nil {
		return fmt.Errorf("encode provider command %s: %w", cmd.CommandID, err)
	}
	if _, err := stdin.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write provider command %s: %w", cmd.CommandID, err)
	}
	return nil
}

type acpPendingPermission struct {
	RPCID   any
	Options []map[string]any
}

func streamACPProviderOutput(ctx context.Context, cfg wrapConfig, scanner *bufio.Scanner, responses *acpResponseRouter, observe func([]byte, uint64) error, send func(protocol.Event) error) error {
	var sourceSequence uint64
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		sourceSequence++
		line := append([]byte(nil), scanner.Bytes()...)
		if responses != nil && responses.Deliver(line, sourceSequence) {
			continue
		}
		if observe != nil {
			if err := observe(line, sourceSequence); err != nil {
				return err
			}
		}
		events, err := translateWrapLine(cfg, line)
		if err != nil {
			return err
		}
		for _, event := range events {
			if err := send(event); err != nil {
				return fmt.Errorf("send acp provider event %s: %w", event.Type, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan acp provider stdout: %w", err)
	}
	return nil
}

func forwardHubCommandsToACPProvider(ctx context.Context, readFrame func(context.Context) (protocol.Frame, error), stdin io.WriteCloser, writeFrame func(protocol.Frame) error, observePong func(string), rotation *credentialRotationManager, providerSessionID string, nextID int64, pendingPermissions map[string]acpPendingPermission, permissionMu *sync.Mutex, responses *acpResponseRouter, settingsTracker *acpSettingsTracker, settingsMu *sync.Mutex, startAdmission *providerStartAdmission, supervisor *core.ProcessSupervisor, stopInProgress *atomic.Bool, cfg wrapConfig) error {
	defer stdin.Close()
	var pendingSettings *acpSettingsReservation
	accepted := newAcceptedCommandSet(2048)
	for {
		frame, err := readFrame(ctx)
		if err != nil {
			return ignoreContextError(err)
		}
		switch typed := frame.(type) {
		case *protocol.CredentialRotationCredential:
			rotation.recordCredentialExpiry(typed)
			if err := rotation.handle(ctx, typed); err != nil {
				return err
			}
		case *protocol.CredentialRotationActivation:
			if err := rotation.handle(ctx, typed); err != nil {
				return err
			}
		case *protocol.ProviderStartPrepare, *protocol.ProviderStartAck:
			if err := startAdmission.deliver(typed); err != nil {
				return err
			}
		case *protocol.Command:
			if accepted.Contains(typed.CommandID) {
				if err := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}); err != nil {
					return fmt.Errorf("re-ack acp provider command %s: %w", typed.CommandID, err)
				}
				continue
			}
			switch typed.Type {
			case protocol.CommandSessionInterrupt:
				if err := writeACPRequest(stdin, nextID, "session/cancel", map[string]any{"sessionId": providerSessionID}); err != nil {
					return fmt.Errorf("write acp provider interrupt %s: %w", typed.CommandID, err)
				}
				nextID++
				accepted.Add(typed.CommandID)
				if err := acknowledgeRunControl(ctx, typed, readFrame, writeFrame, cfg, "interrupt", "ready", nil); err != nil {
					return err
				}
			case protocol.CommandSessionSend:
				prompt, err := acpPromptFromSessionSend(typed.Payload)
				if err != nil {
					ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: err.Error()}
					if writeErr := writeFrame(&ack); writeErr != nil {
						return fmt.Errorf("reject acp provider command %s: %w", typed.CommandID, writeErr)
					}
					continue
				}
				if err := writeACPRequest(stdin, nextID, "session/prompt", map[string]any{
					"sessionId": providerSessionID,
					"prompt":    prompt,
				}); err != nil {
					return fmt.Errorf("write acp provider prompt %s: %w", typed.CommandID, err)
				}
				nextID++
				accepted.Add(typed.CommandID)
				ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}
				if err := writeFrame(&ack); err != nil {
					return fmt.Errorf("ack acp provider command %s: %w", typed.CommandID, err)
				}
			case protocol.CommandPermissionRespond:
				pending, result, err := acpPermissionResult(typed.Payload, pendingPermissions, permissionMu)
				if err != nil {
					ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: err.Error()}
					if writeErr := writeFrame(&ack); writeErr != nil {
						return fmt.Errorf("reject acp permission response %s: %w", typed.CommandID, writeErr)
					}
					continue
				}
				if err := writeACPResult(stdin, pending.RPCID, result); err != nil {
					return fmt.Errorf("write acp permission response %s: %w", typed.CommandID, err)
				}
				accepted.Add(typed.CommandID)
				ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}
				if err := writeFrame(&ack); err != nil {
					return fmt.Errorf("ack acp permission response %s: %w", typed.CommandID, err)
				}
			case protocol.CommandSettingsChange:
				if cfg.ProtocolVersion != protocol.ProtocolVersionV2 || typed.SessionID != cfg.SessionID || pendingSettings != nil {
					ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: "settings_unsupported"}
					if pendingSettings != nil {
						ack.Reason = "settings_change_pending"
					}
					if err := writeFrame(&ack); err != nil {
						return fmt.Errorf("reject acp settings command %s: %w", typed.CommandID, err)
					}
					continue
				}
				change, err := protocol.DecodeSettingsChangePayload(typed.Payload)
				settingsMu.Lock()
				state, available := settingsTracker.Current()
				settingsMu.Unlock()
				reason := "settings_unsupported"
				if err == nil && available {
					reason = validateACPSettingsChange(state, change)
				}
				if err != nil || !available || reason != "" {
					ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: reason}
					if err := writeFrame(&ack); err != nil {
						return fmt.Errorf("reject acp settings command %s: %w", typed.CommandID, err)
					}
					continue
				}
				reservation := acpSettingsReservation{
					Command: *typed, Change: change, Reserved: state,
					Deadline: time.Now().Add(acpSettingsOperationTimeout),
				}
				pendingSettings = &reservation
				accepted.Add(typed.CommandID)
				if err := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}); err != nil {
					pendingSettings = nil
					return fmt.Errorf("ack acp settings command %s: %w", typed.CommandID, err)
				}
			case protocol.CommandSessionStop:
				if err := handleProviderRunControl(ctx, typed, supervisor, readFrame, writeFrame, stopInProgress, cfg, true); err != nil {
					return err
				}
				return nil
			default:
				ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: "unsupported acp provider command"}
				if err := writeFrame(&ack); err != nil {
					return fmt.Errorf("reject acp provider command %s: %w", typed.CommandID, err)
				}
			}
		case *protocol.SettingsDeliveryExecute:
			// The Store reservation version is first disclosed to the Adapter by
			// this post-CAS frame. Local identity is therefore bound by the same
			// authenticated Session WebSocket, exact command ID, one pending slot,
			// and one-time consumption; the Hub remains authoritative for the
			// positive Store generation carried here.
			if pendingSettings == nil || cfg.ProtocolVersion != protocol.ProtocolVersionV2 || typed.SessionID != cfg.SessionID || typed.CommandID != pendingSettings.Command.CommandID || typed.ReservationVersion < 1 {
				return errors.New("acp settings execute has no matching reservation")
			}
			reservation := *pendingSettings
			pendingSettings = nil
			if !time.Now().Before(reservation.Deadline) {
				return errors.New("acp settings execute arrived after the operation fence")
			}
			operationCtx, cancel := context.WithDeadline(ctx, reservation.Deadline)
			execution := executeACPSettingsChange(operationCtx, reservation, providerSessionID, stdin, responses, settingsTracker, settingsMu, &nextID)
			cancel()
			if !execution.PublishResult {
				stopProviderSupervisor(supervisor)
				return errors.New("acp settings provider result requires fresh reconnect readback")
			}
			settingsMu.Lock()
			latest, available := settingsTracker.Current()
			if !available {
				settingsMu.Unlock()
				stopProviderSupervisor(supervisor)
				return errors.New("acp settings capability disappeared after execution")
			}
			execution = reconcileACPSettingsExecution(execution, latest)
			if err := publishACPSettingsCapability(writeFrame, cfg.SessionID, execution.State); err != nil {
				settingsMu.Unlock()
				return fmt.Errorf("publish acp settings readback: %w", err)
			}
			if err := publishACPSettingsEffective(writeFrame, cfg.SessionID, reservation, execution); err != nil {
				settingsMu.Unlock()
				return fmt.Errorf("publish acp settings effective result: %w", err)
			}
			settingsMu.Unlock()
			if execution.TerminateProvider {
				stopProviderSupervisor(supervisor)
				return errors.New("acp settings provider operation exceeded its safe fence")
			}
		case *protocol.EventReceipt:
			// Durable capability/effective receipts are terminal acknowledgements;
			// the Adapter has no additional side effect to perform.
		case *protocol.Ping:
			if err := writeFrame(&protocol.Pong{Nonce: typed.Nonce}); err != nil {
				return fmt.Errorf("send pong: %w", err)
			}
		case *protocol.Pong:
			observePong(typed.Nonce)
		case *protocol.Error:
			return fmt.Errorf("hub error %s: %s", typed.Code, typed.Message)
		}
	}
}

type acceptedCommandSet struct {
	mu    sync.Mutex
	limit int
	ids   map[string]struct{}
	order []string
}

func newAcceptedCommandSet(limit int) *acceptedCommandSet {
	if limit < 1 {
		limit = 1
	}
	return &acceptedCommandSet{limit: limit, ids: make(map[string]struct{}, limit)}
}

func (s *acceptedCommandSet) Contains(id string) bool {
	if s == nil || id == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.ids[id]
	return ok
}

func (s *acceptedCommandSet) Add(id string) {
	if s == nil || id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[id]; ok {
		return
	}
	s.ids[id] = struct{}{}
	s.order = append(s.order, id)
	for len(s.order) > s.limit {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.ids, oldest)
	}
}

func trackACPPermissionRequest(line []byte, pending map[string]acpPendingPermission, mu *sync.Mutex) {
	var message map[string]any
	if err := json.Unmarshal(line, &message); err != nil {
		return
	}
	if message["method"] != "session/request_permission" {
		return
	}
	requestID := stringFieldFromAny(message["id"])
	if requestID == "" {
		return
	}
	params, _ := message["params"].(map[string]any)
	mu.Lock()
	pending[requestID] = acpPendingPermission{
		RPCID:   message["id"],
		Options: acpPermissionOptions(params["options"]),
	}
	mu.Unlock()
}

func acpPermissionResult(payload []byte, pending map[string]acpPendingPermission, mu *sync.Mutex) (acpPendingPermission, map[string]any, error) {
	var decoded struct {
		RequestID string `json:"request_id"`
		Decision  string `json:"decision"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return acpPendingPermission{}, nil, fmt.Errorf("invalid permission response payload: %w", err)
	}
	if decoded.RequestID == "" {
		return acpPendingPermission{}, nil, errors.New("permission response missing request_id")
	}
	mu.Lock()
	pendingPermission, ok := pending[decoded.RequestID]
	if ok {
		delete(pending, decoded.RequestID)
	}
	mu.Unlock()
	if !ok {
		return acpPendingPermission{}, nil, fmt.Errorf("permission request %s not pending", decoded.RequestID)
	}
	return pendingPermission, map[string]any{
		"outcome": acpPermissionOutcome(decoded.Decision, pendingPermission.Options),
	}, nil
}

func acpPermissionOutcome(decision string, options []map[string]any) map[string]any {
	preferReject := decision != "approve"
	for _, option := range options {
		kind := stringFieldFromAny(option["kind"])
		optionID := stringFieldFromAny(option["optionId"])
		if optionID == "" {
			optionID = stringFieldFromAny(option["option_id"])
		}
		if optionID == "" {
			continue
		}
		if preferReject && kind == "reject" {
			return map[string]any{"outcome": "selected", "optionId": optionID}
		}
		if !preferReject && kind != "reject" {
			return map[string]any{"outcome": "selected", "optionId": optionID}
		}
	}
	return map[string]any{"outcome": "cancelled"}
}

func acpPermissionOptions(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	options := make([]map[string]any, 0, len(items))
	for _, item := range items {
		option, ok := item.(map[string]any)
		if ok {
			options = append(options, option)
		}
	}
	return options
}

func writeACPRequest(stdin io.Writer, id int64, method string, params map[string]any) error {
	encoded, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return fmt.Errorf("encode acp request %s: %w", method, err)
	}
	if _, err := stdin.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write acp request %s: %w", method, err)
	}
	return nil
}

func writeACPResult(stdin io.Writer, id any, result map[string]any) error {
	encoded, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
	if err != nil {
		return fmt.Errorf("encode acp response %v: %w", id, err)
	}
	if _, err := stdin.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write acp response %v: %w", id, err)
	}
	return nil
}

func readACPResponse(ctx context.Context, scanner *bufio.Scanner, id int64) (map[string]any, error) {
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &fields); err != nil {
			return nil, fmt.Errorf("decode acp response %d: %w", id, err)
		}
		if fields["method"] != nil {
			if fields["id"] != nil {
				return nil, fmt.Errorf("acp request %d received an unexpected Provider request during initialization", id)
			}
			continue
		}
		responseKey, ok := acpRPCIDKey(fields["id"])
		if !ok {
			continue
		}
		if responseKey != acpNumericRPCIDKey(id) {
			if responseKey == "s:"+fmt.Sprint(id) {
				return nil, fmt.Errorf("acp response %d changed the JSON-RPC id type", id)
			}
			continue
		}
		rawError := fields["error"]
		rawResult := fields["result"]
		if rawError != nil && string(rawError) != "null" {
			if rawResult != nil {
				return nil, fmt.Errorf("acp response %d has both result and error", id)
			}
			return nil, fmt.Errorf("acp request %d failed: %s", id, compactJSON(rawError))
		}
		var result map[string]any
		if rawResult == nil || json.Unmarshal(rawResult, &result) != nil {
			return nil, fmt.Errorf("acp response %d missing result", id)
		}
		return result, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan acp response %d: %w", id, err)
	}
	return nil, fmt.Errorf("acp response %d not received", id)
}

func acpPromptFromSessionSend(payload []byte) ([]map[string]any, error) {
	return acpPromptFromSessionSendAtRoot(payload, ".")
}

func acpPromptFromSessionSendAtRoot(payload []byte, rootPath string) ([]map[string]any, error) {
	filePayload, err := protocol.DecodeFileReferenceSendPayload(payload)
	if err != nil {
		return nil, fmt.Errorf("invalid session.send payload: %w", err)
	}
	var decoded struct {
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("invalid session.send payload: %w", err)
	}
	prompt := make([]map[string]any, 0, len(decoded.Content))
	if !filePayload.HasReferences {
		for _, raw := range decoded.Content {
			var part struct {
				Kind string `json:"kind"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(raw, &part); err != nil {
				return nil, fmt.Errorf("invalid session.send content: %w", err)
			}
			if part.Kind != "text" || part.Text == "" {
				continue
			}
			prompt = append(prompt, map[string]any{"type": "text", "text": part.Text})
		}
	} else {
		root, err := os.OpenRoot(rootPath)
		if err != nil {
			return nil, errors.New("file reference workspace unavailable")
		}
		defer root.Close()
		for index, raw := range decoded.Content {
			var part map[string]any
			if err := json.Unmarshal(raw, &part); err != nil {
				return nil, fmt.Errorf("invalid file-reference content %d", index)
			}
			kind := stringFieldFromAny(part["kind"])
			if kind == "text" {
				text := stringFieldFromAny(part["text"])
				if text == "" {
					return nil, fmt.Errorf("invalid file-reference text %d", index)
				}
				prompt = append(prompt, map[string]any{"type": "text", "text": text})
				continue
			}
			if kind != "file_reference" {
				return nil, fmt.Errorf("unsupported session content %d", index)
			}
			content, err := readACPFileReference(root, part)
			if err != nil {
				return nil, fmt.Errorf("file reference %d rejected: %w", index, err)
			}
			prompt = append(prompt, content)
		}
	}
	if len(prompt) == 0 {
		return nil, errors.New("session.send payload has no text content")
	}
	return prompt, nil
}

func readACPFileReference(root *os.Root, part map[string]any) (map[string]any, error) {
	if len(part) != 7 {
		return nil, errors.New("invalid file-reference fields")
	}
	path := stringFieldFromAny(part["path"])
	disposition := stringFieldFromAny(part["disposition"])
	digest := stringFieldFromAny(part["content_digest"])
	mediaType := stringFieldFromAny(part["media_type"])
	declaredBytes, ok := part["bytes"].(float64)
	if !ok || declaredBytes < 0 || declaredBytes > 10*1024*1024 || declaredBytes != float64(int64(declaredBytes)) {
		return nil, errors.New("invalid file-reference size")
	}
	if disposition != "file" && disposition != "image" || path == "" || strings.HasPrefix(path, "/") || strings.Contains(path, "\\") || strings.Contains(path, "..") || strings.Contains(path, "\x00") {
		return nil, errors.New("unsafe file-reference path")
	}
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		return nil, errors.New("invalid file-reference digest")
	}
	if disposition == "image" && !allowedACPImageType(mediaType) {
		return nil, errors.New("unsupported image type")
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, errors.New("reference unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != int64(declaredBytes) {
		return nil, errors.New("reference changed")
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(declaredBytes)+1))
	if err != nil || int64(len(data)) != int64(declaredBytes) {
		return nil, errors.New("reference read failed")
	}
	digestSum := sha256.Sum256(data)
	if fmt.Sprintf("sha256:%x", digestSum[:]) != digest {
		return nil, errors.New("reference digest mismatch")
	}
	if disposition == "image" {
		return map[string]any{"type": "image", "data": base64.StdEncoding.EncodeToString(data), "mimeType": mediaType}, nil
	}
	if !utf8.Valid(data) {
		return nil, errors.New("non-text file requires image disposition")
	}
	return map[string]any{"type": "text", "text": string(data)}, nil
}

func allowedACPImageType(mediaType string) bool {
	switch mediaType {
	case "image/png", "image/jpeg", "image/webp", "image/gif":
		return true
	default:
		return false
	}
}

func stringFieldFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case nil:
		return ""
	default:
		return fmt.Sprint(typed)
	}
}

func ignoreContextError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
		return nil
	}
	return err
}

func startAdapterHeartbeat(ctx context.Context, cfg heartbeatConfig, writeFrame func(protocol.Frame) error, onPingSent func()) (<-chan error, func(string)) {
	done := make(chan error, 1)
	pongs := make(chan string, 1)
	observePong := func(nonce string) {
		select {
		case pongs <- nonce:
		default:
		}
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			nonce, err := randomToken()
			if err != nil {
				done <- fmt.Errorf("generate heartbeat nonce: %w", err)
				return
			}
			if err := writeFrame(&protocol.Ping{Nonce: nonce}); err != nil {
				done <- fmt.Errorf("send heartbeat ping: %w", err)
				return
			}
			if onPingSent != nil {
				onPingSent()
			}

			timeout := time.NewTimer(cfg.Timeout)
			for {
				select {
				case <-ctx.Done():
					timeout.Stop()
					return
				case got := <-pongs:
					if got == nonce {
						timeout.Stop()
						goto next
					}
				case <-timeout.C:
					done <- errors.New("hub heartbeat timed out")
					return
				}
			}
		next:
		}
	}()

	return done, observePong
}

func writeCLIProtocolFrame(ctx context.Context, conn *websocket.Conn, frame protocol.Frame) error {
	data, err := protocol.Encode(frame)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func readCLIProtocolFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, fmt.Errorf("websocket message type %v", typ)
	}
	return protocol.Decode(data)
}

type runningServe struct {
	server      *http.Server
	store       interface{ Close() error }
	diagnostics *core.AdapterDiagnosticsServer
	done        chan error
	addr        string
	wsURL       string
	waitOnce    sync.Once
	waitErr     error
}

func startServe(ctx context.Context, cfg serveConfig) (*runningServe, error) {
	cfg, err := normalizeServeConfig(cfg)
	if err != nil {
		return nil, err
	}
	issuer, err := newLocalSessionCredentialIssuer(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o700); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}
	eventStore, err := sqlite.Open(ctx, cfg.DBPath)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		_ = eventStore.Close()
		return nil, fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}

	baseAuthenticator := static.New([]static.Token{
		{
			Token:   cfg.ControlToken,
			Subject: "local-client",
			Scopes:  []auth.Scope{auth.SessionControl(cfg.SessionID)},
		},
		{
			Token:   cfg.AdapterToken,
			Subject: "local-adapter",
			Scopes:  []auth.Scope{auth.SessionAdapter(cfg.SessionID)},
		},
	})
	authenticator := localSessionAuthenticator{
		Authenticator:           baseAuthenticator,
		staticAdapterExpiresAt:  time.Now().AddDate(10, 0, 0).UnixNano(),
		sessionCredentialIssuer: issuer,
		sessionID:               cfg.SessionID,
		provider:                cfg.Provider,
	}
	sessionStore := localSessionStore{Store: eventStore, sessionID: cfg.SessionID}
	handshake := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: authenticator,
		EventStore:    sessionStore,
	})
	webSocketHandler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: sessionStore, SessionCredentialIssuer: issuer, SessionCredentialLifecycle: issuer, SessionCredentialEvidenceResolver: authenticator})
	server := &http.Server{
		Handler:           hub.NewObservabilityHandler(cfg.ControlToken, webSocketHandler),
		ReadHeaderTimeout: 5 * time.Second,
	}

	running := &runningServe{
		server: server,
		store:  eventStore,
		done:   make(chan error, 1),
		addr:   listener.Addr().String(),
		wsURL:  "ws://" + listener.Addr().String(),
	}

	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		running.done <- err
		close(running.done)
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	return running, nil
}

func newLocalSessionCredentialIssuer(cfg serveConfig) (*auth.LocalSessionCredentialIssuer, error) {
	if cfg.SessionCredentialSignerKeyFile == "" {
		return nil, errors.New("session credential signer key file is required")
	}
	info, err := os.Lstat(cfg.SessionCredentialSignerKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read session credential signer key: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("session credential signer key file must be private and regular")
	}
	file, err := os.Open(cfg.SessionCredentialSignerKeyFile)
	if err != nil {
		return nil, fmt.Errorf("open session credential signer key: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm()&0o077 != 0 || !os.SameFile(info, openedInfo) {
		return nil, errors.New("session credential signer key file changed or is unsafe")
	}
	key, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(key) == 0 || len(key) > 4096 {
		return nil, errors.New("session credential signer key is invalid")
	}
	issuer, err := auth.NewLocalSessionCredentialIssuer(key, cfg.SessionCredentialSignerKeyVersion)
	if err != nil {
		return nil, errors.New("session credential signer configuration is invalid")
	}
	return issuer, nil
}

func (r *runningServe) wait() error {
	r.waitOnce.Do(func() {
		serveErr := <-r.done
		var diagnosticsErr error
		if r.diagnostics != nil {
			diagnosticsErr = r.diagnostics.Close()
		}
		closeErr := r.store.Close()
		if serveErr != nil {
			r.waitErr = serveErr
		} else if closeErr != nil {
			r.waitErr = fmt.Errorf("close sqlite store: %w", closeErr)
		} else if diagnosticsErr != nil {
			r.waitErr = fmt.Errorf("close adapter diagnostics: %w", diagnosticsErr)
		}
	})
	return r.waitErr
}

type localSessionAuthenticator struct {
	auth.Authenticator
	staticAdapterExpiresAt  int64
	sessionCredentialIssuer *auth.LocalSessionCredentialIssuer
	sessionID               string
	provider                string
}

func (a localSessionAuthenticator) Authenticate(ctx context.Context, token string) (auth.Principal, error) {
	principal, err := a.Authenticator.Authenticate(ctx, token)
	if err == nil {
		return principal, nil
	}
	if a.sessionCredentialIssuer == nil {
		return auth.Principal{}, err
	}
	principal, issuerErr := a.sessionCredentialIssuer.AuthenticateSessionCredential(ctx, token)
	if issuerErr != nil || len(principal.Scopes) != 1 || principal.Scopes[0] != auth.SessionAdapter(a.sessionID) {
		return auth.Principal{}, err
	}
	return principal, nil
}

func (a localSessionAuthenticator) SessionAdmissionClaim(_ context.Context, _ auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	if sessionID != a.sessionID {
		return auth.SessionAdmissionClaim{}, auth.ErrUnauthorized
	}
	return auth.SessionAdmissionClaim{SessionID: sessionID, Provider: a.provider, ExpiresAt: time.Now().Add(5 * time.Minute)}, nil
}

type localSessionStore struct {
	*sqlite.Store
	sessionID string
}

func (s localSessionStore) SessionAdmissionTruth(_ context.Context, sessionID string) (store.SessionAdmissionTruth, error) {
	if sessionID != s.sessionID {
		return store.SessionAdmissionTruth{}, auth.ErrUnauthorized
	}
	return store.SessionAdmissionTruth{SessionID: sessionID, Exists: true, Complete: true, Live: true}, nil
}
