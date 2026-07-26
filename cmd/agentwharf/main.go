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
		return errors.New("usage: wharf serve|wrap|claude|codex|gemini|logout|machine|attention-backfill [options]")
	}

	switch args[0] {
	case "serve":
		cfg, err := parseServeConfig(args[1:], stderr)
		if err != nil {
			return err
		}
		running, err := startServe(ctx, cfg)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "wharf serve listening on %s\n", running.wsURL)
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
		cfg, err := parseAgentEntrypointConfig(args[0], args[1:], stderr)
		if err != nil {
			return err
		}
		effective, err := runWrap(ctx, cfg, stdin, stderr)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "wharf %s ended session_id=%s provider=%s\n", args[0], effective.SessionID, effective.Provider)
		return nil
	case "logout":
		if len(args) != 1 {
			return fmt.Errorf("unexpected logout arguments: %v", args[1:])
		}
		return runMachineLogout(stdout)
	case "machine":
		if len(args) == 2 && args[1] == "unlink" {
			return runMachineLogout(stdout)
		}
		return errors.New("usage: wharf machine unlink")
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
		ExpiresAt    string `json:"expires_at"`
		Session      struct {
			Provider string `json:"provider"`
		} `json:"session"`
	} `json:"data"`
}

func runTaskCommand(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) < 2 || args[0] != "claim" {
		return errors.New("usage: wharf task claim <claim_id> --code-stdin")
	}
	claimID := strings.TrimSpace(args[1])
	if claimID == "" || strings.ContainsAny(claimID, "/\\") {
		return errors.New("claim unavailable")
	}
	codeStdin := false
	for _, arg := range args[2:] {
		if arg == "--code-stdin" {
			codeStdin = true
			continue
		}
		return errors.New("usage: wharf task claim <claim_id> --code-stdin")
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
	if err := decodeCloudAPIJSON(body, &handoff); err != nil || handoff.Data.SessionID == "" || handoff.Data.HubWSURL == "" || handoff.Data.AdapterToken == "" || handoff.Data.ExpiresAt == "" {
		return errors.New("claim unavailable")
	}
	provider := strings.TrimSpace(handoff.Data.Provider)
	if provider == "" {
		provider = strings.TrimSpace(handoff.Data.Session.Provider)
	}
	if provider == "" || strings.ContainsAny(provider, " \t\r\n") {
		return errors.New("claim unavailable")
	}
	expiresAt, err := time.Parse(time.RFC3339, handoff.Data.ExpiresAt)
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
		ProtocolVersion: protocol.ProtocolVersion,
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := runWrap(ctx, cfg, strings.NewReader(""), stderr); err == nil {
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
		MachineToken string `json:"machine_token"`
		HubWSURL     string `json:"hub_ws_url"`
		ExpiresAt    string `json:"expires_at"`
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

	flags := flag.NewFlagSet(agent, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.HubURL, "hub", cfg.HubURL, "Hub WebSocket URL")
	flags.StringVar(&cfg.SessionID, "session-id", cfg.SessionID, "session id")
	flags.StringVar(&cfg.Provider, "provider", cfg.Provider, "provider name override")
	flags.StringVar(&cfg.AdapterToken, "adapter-token", cfg.AdapterToken, "adapter token")
	flags.StringVar(&cfg.SecretDir, "secret-dir", cfg.SecretDir, "directory containing injected secret files for masking")
	flags.StringVar(&cfg.CloudAPIURL, "cloud", cfg.CloudAPIURL, "SuperWHV Cloud API base URL")
	flags.BoolVar(&cfg.Pair, "pair", cfg.Pair, "pair this machine with SuperWHV before connecting")
	flags.IntVar(&cfg.ProtocolVersion, "protocol-version", protocol.ProtocolVersion, "Adapter protocol version (1 or 2)")
	if err := flags.Parse(args); err != nil {
		return wrapConfig{}, err
	}
	if flags.NArg() > 0 {
		cfg.ProviderCommand = append([]string(nil), flags.Args()...)
	}
	if cfg.Pair {
		cfg.Managed = true
	}
	if cfg.Managed && cfg.CloudAPIURL == "" {
		cfg.CloudAPIURL = defaultManagedCloudAPIURL
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
	if !cfg.Pair && !cfg.Managed && cfg.AdapterToken == defaultAdapterToken && !isLoopbackURL(cfg.HubURL) {
		return wrapConfig{}, errUnsafeDefaultToken
	}
	cfg.SecretDir = filepath.Clean(cfg.SecretDir)
	if cfg.SecretDir == "." {
		cfg.SecretDir = ""
	}
	if cfg.ProtocolVersion == 0 {
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
	if stdin == nil {
		stdin = io.Reader(os.Stdin)
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
		return cfg, fmt.Errorf("%w: %s", errClaimAuthRejection, protocolErr.Code)
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
		return cfg, runWrapProvider(ctx, cfg, conn, masker, metrics)
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
	operatorUID, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("AGENTWHARF_DIAGNOSTICS_OPERATOR_UID")), 10, 32)
	fixedEntryUID, fixedEntryErr := strconv.ParseUint(strings.TrimSpace(os.Getenv("AGENTWHARF_FIXED_ENTRY_UID")), 10, 32)
	if err != nil || fixedEntryErr != nil || operatorUID == 0 || fixedEntryUID == 0 || cfg.HealthMarker == "" || cfg.ProviderCredential == nil || cfg.ProviderCredential.UID == 0 || cfg.ProviderCredential.UID == uint32(operatorUID) || cfg.ProviderCredential.UID == uint32(fixedEntryUID) {
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

func pairWrapSessionWithClient(ctx context.Context, client *http.Client, cfg wrapConfig, output io.Writer) (wrapConfig, error) {
	createURL, err := cloudAPIEndpoint(cfg.CloudAPIURL, "/machine-pairing-codes")
	if err != nil {
		return cfg, err
	}
	var pairing machinePairingCodeResponse
	status, body, err := postCloudAPIJSONWithRetry(ctx, client, createURL, "", machinePairingCreateRequest{
		Platform: runtime.GOOS + "-" + runtime.GOARCH,
	})
	if err != nil {
		return cfg, err
	}
	if status != http.StatusCreated {
		return cfg, newCloudStatusError("create machine pairing code", status, body)
	}
	if err := decodeCloudAPIJSON(body, &pairing); err != nil {
		return cfg, fmt.Errorf("decode machine pairing response: %w", err)
	}
	if pairing.Data.DeviceCode == "" || pairing.Data.UserCode == "" {
		return cfg, errors.New("machine pairing response missing codes")
	}
	if output != nil {
		_, _ = fmt.Fprintf(output, "Pair this machine at %s\ndevice_code: %s\nuser_code: %s\n",
			machinePairingDisplayURL(cfg.CloudAPIURL, pairing.Data.VerificationURI),
			pairing.Data.DeviceCode,
			pairing.Data.UserCode)
	}

	machineToken, err := exchangeMachineToken(ctx, client, cfg.CloudAPIURL, pairing)
	if err != nil {
		return cfg, err
	}
	if err := saveMachineCredential(machineCredential{
		MachineID:    machineToken.Data.Machine.ID,
		MachineToken: machineToken.Data.MachineToken,
		CloudAPIURL:  cfg.CloudAPIURL,
		HubWSURL:     machineToken.Data.HubWSURL,
		ExpiresAt:    machineToken.Data.ExpiresAt,
	}); err != nil {
		return cfg, err
	}
	session, err := createMachineSession(ctx, client, cfg.CloudAPIURL, machineToken.Data.MachineToken, cfg.Provider)
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

func runWrapProvider(ctx context.Context, cfg wrapConfig, conn *websocket.Conn, masker *core.EventMasker, metrics *core.AdapterMetrics) error {
	stopHealth, err := core.StartFixedEntryHealth(ctx, cfg.HealthMarker)
	if err != nil {
		return err
	}
	defer stopHealth()
	if cfg.Format == "acp" {
		return runWrapACPProvider(ctx, cfg, conn, masker, metrics)
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
	var writeMu sync.Mutex
	writeFrame := func(frame protocol.Frame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeCLIProtocolFrame(runCtx, conn, frame)
	}
	startAdmission := newProviderStartAdmission(cfg.ProtocolVersion, conn, writeFrame, metrics)
	var processAdmission core.ProcessStartAdmission
	if startAdmission != nil {
		processAdmission = startAdmission
	}
	processConfig := core.ProcessConfig{
		Command: core.ProcessCommand{
			Path:       cfg.ProviderCommand[0],
			Args:       cfg.ProviderCommand[1:],
			Stdin:      stdinReader,
			Stdout:     stdoutWriter,
			Stderr:     os.Stderr,
			Credential: cfg.ProviderCredential,
		},
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
	heartbeatDone, observePong := startAdapterHeartbeat(runCtx, cfg.Heartbeat, writeFrame)
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
		startAdmission.watchLifecycle(runCtx, cancel)
	}
	if err := publishRunControlCapability(runCtx, conn, cfg, writeFrame); err != nil {
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
	go func() {
		commandDone <- forwardHubCommandsToProvider(runCtx, conn, stdinWriter, writeFrame, observePong, startAdmission, supervisor, cfg)
	}()

	var processErr error
	var outputErr error
	processFinished := false
	outputFinished := false
	for {
		if processFinished && outputFinished {
			cancel()
			_ = stdinWriter.Close()
			if processErr != nil {
				return processErr
			}
			return outputErr
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

func runWrapACPProvider(ctx context.Context, cfg wrapConfig, conn *websocket.Conn, masker *core.EventMasker, metrics *core.AdapterMetrics) error {
	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create provider stdin pipe: %w", err)
	}
	defer stdinReader.Close()
	defer stdinWriter.Close()
	stdoutReader, stdoutWriter := io.Pipe()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var writeMu sync.Mutex
	writeFrame := func(frame protocol.Frame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeCLIProtocolFrame(runCtx, conn, frame)
	}
	startAdmission := newProviderStartAdmission(cfg.ProtocolVersion, conn, writeFrame, metrics)
	var processAdmission core.ProcessStartAdmission
	if startAdmission != nil {
		processAdmission = startAdmission
	}
	processConfig := core.ProcessConfig{
		Command: core.ProcessCommand{
			Path:       cfg.ProviderCommand[0],
			Args:       cfg.ProviderCommand[1:],
			Stdin:      stdinReader,
			Stdout:     stdoutWriter,
			Stderr:     os.Stderr,
			Credential: cfg.ProviderCredential,
		},
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
		startAdmission.watchLifecycle(runCtx, cancel)
	}
	if err := publishRunControlCapability(runCtx, conn, cfg, writeFrame); err != nil {
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
			"version": "0.1.0",
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

	cwd, err := os.Getwd()
	if err != nil {
		cancel()
		return fmt.Errorf("get provider cwd: %w", err)
	}
	if err := writeACPRequest(stdinWriter, 2, "session/new", map[string]any{
		"cwd":        cwd,
		"mcpServers": []any{},
	}); err != nil {
		cancel()
		return err
	}
	sessionResult, err := readACPResponse(runCtx, scanner, 2)
	if err != nil {
		cancel()
		return err
	}
	providerSessionID := stringFieldFromAny(sessionResult["sessionId"])
	if providerSessionID == "" {
		cancel()
		return errors.New("acp session/new response missing sessionId")
	}
	if err := sendACPProviderReadyEvent(runCtx, conn, cfg, providerSessionID, masker, metrics); err != nil {
		cancel()
		return err
	}

	outputDone := make(chan error, 1)
	commandDone := make(chan error, 1)
	var permissionMu sync.Mutex
	pendingPermissions := make(map[string]acpPendingPermission)
	heartbeatDone, observePong := startAdapterHeartbeat(runCtx, cfg.Heartbeat, writeFrame)
	go func() {
		outputDone <- streamACPProviderOutput(runCtx, cfg, scanner, func(line []byte) {
			trackACPPermissionRequest(line, pendingPermissions, &permissionMu)
		}, func(event protocol.Event) error {
			masked, err := maskEvent(masker, event)
			if err != nil {
				return err
			}
			metrics.IncMaskedEvent()
			return writeFrame(&masked)
		})
	}()
	go func() {
		commandDone <- forwardHubCommandsToACPProvider(runCtx, conn, stdinWriter, writeFrame, observePong, providerSessionID, 3, pendingPermissions, &permissionMu, startAdmission, supervisor, cfg)
	}()

	processFinished := false
	outputFinished := false
	var processErr error
	var outputErr error
	for {
		if processFinished && outputFinished {
			cancel()
			_ = stdinWriter.Close()
			if processErr != nil {
				return processErr
			}
			return outputErr
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
	conn    *websocket.Conn
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

func newProviderStartAdmission(version int, conn *websocket.Conn, write func(protocol.Frame) error, metrics *core.AdapterMetrics) *providerStartAdmission {
	if version != protocol.ProtocolVersionV2 || conn == nil || write == nil {
		return nil
	}
	return &providerStartAdmission{
		conn: conn, write: write, metrics: metrics, direct: true,
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
		frame, err := readCLIProtocolFrame(ctx, a.conn)
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
		frame, err := readCLIProtocolFrame(ctx, a.conn)
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
	if a == nil || a.conn == nil {
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

func (a *providerStartAdmission) watchLifecycle(ctx context.Context, cancel context.CancelFunc) {
	if a == nil || cancel == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := a.VerifyRecoveryStart(ctx); err != nil {
					cancel()
					return
				}
				pingCtx, pingCancel := context.WithTimeout(ctx, 100*time.Millisecond)
				err := a.conn.Ping(pingCtx)
				pingCancel()
				if err != nil && (websocket.CloseStatus(err) != -1 || errors.Is(err, net.ErrClosed)) {
					a.invalidate()
					cancel()
					return
				}
			}
		}
	}()
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

func sendACPProviderReadyEvent(ctx context.Context, conn *websocket.Conn, cfg wrapConfig, providerSessionID string, masker *core.EventMasker, metrics *core.AdapterMetrics) error {
	payload, err := json.Marshal(map[string]any{
		"state":               "ready",
		"provider":            cfg.Provider,
		"provider_session_id": providerSessionID,
		"metadata":            map[string]any{},
		"source":              "acp",
	})
	if err != nil {
		return fmt.Errorf("marshal acp ready event: %w", err)
	}
	event := protocol.Event{
		Type:      "session.state",
		SessionID: cfg.SessionID,
		Time:      time.Now().UTC().UnixMilli(),
		Payload:   payload,
	}
	event, err = maskEvent(masker, event)
	if err != nil {
		return err
	}
	metrics.IncMaskedEvent()
	if err := writeCLIProtocolFrame(ctx, conn, &event); err != nil {
		return fmt.Errorf("send acp ready event: %w", err)
	}
	return nil
}

func publishRunControlCapability(ctx context.Context, conn *websocket.Conn, cfg wrapConfig, writeFrame func(protocol.Frame) error) error {
	if cfg.ProtocolVersion != protocol.ProtocolVersionV2 {
		return nil
	}
	proposalID, err := randomToken()
	if err != nil {
		return fmt.Errorf("generate run-control capability proposal: %w", err)
	}
	payload, err := json.Marshal(protocol.RunControlCapabilityPayload{
		SchemaVersion: 1, InterruptSupported: true, StopSupported: true,
	})
	if err != nil {
		return fmt.Errorf("marshal run-control capability: %w", err)
	}
	return writeFrame(&protocol.Event{
		Type: "session.run.capabilities", SessionID: cfg.SessionID,
		Time: time.Now().UTC().UnixMilli(), Payload: payload, ProposalID: proposalID,
	})
}

func handleProviderRunControl(ctx context.Context, command *protocol.Command, supervisor *core.ProcessSupervisor, writeFrame func(protocol.Frame) error, cfg wrapConfig, stop bool) error {
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
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		operationErr = supervisor.Stop(stopCtx)
		cancel()
	} else {
		operationErr = supervisor.Interrupt(ctx)
	}
	return acknowledgeRunControl(ctx, command, writeFrame, cfg, operation, completionState, operationErr)
}

func acknowledgeRunControl(ctx context.Context, command *protocol.Command, writeFrame func(protocol.Frame) error, cfg wrapConfig, operation, completionState string, operationErr error) error {
	if command == nil || writeFrame == nil {
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
	return nil
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

func forwardHubCommandsToProvider(ctx context.Context, conn *websocket.Conn, stdin io.WriteCloser, writeFrame func(protocol.Frame) error, observePong func(string), startAdmission *providerStartAdmission, supervisor *core.ProcessSupervisor, cfg wrapConfig) error {
	defer stdin.Close()
	for {
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			return ignoreContextError(err)
		}
		switch typed := frame.(type) {
		case *protocol.ProviderStartPrepare, *protocol.ProviderStartAck:
			if err := startAdmission.deliver(typed); err != nil {
				return err
			}
		case *protocol.Command:
			if typed.Type == protocol.CommandSessionInterrupt || typed.Type == protocol.CommandSessionStop {
				return handleProviderRunControl(ctx, typed, supervisor, writeFrame, cfg, typed.Type == protocol.CommandSessionStop)
			}
			if err := writeProviderCommand(stdin, typed); err != nil {
				return err
			}
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

func streamACPProviderOutput(ctx context.Context, cfg wrapConfig, scanner *bufio.Scanner, observe func([]byte), send func(protocol.Event) error) error {
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := append([]byte(nil), scanner.Bytes()...)
		if observe != nil {
			observe(line)
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

func forwardHubCommandsToACPProvider(ctx context.Context, conn *websocket.Conn, stdin io.WriteCloser, writeFrame func(protocol.Frame) error, observePong func(string), providerSessionID string, nextID int64, pendingPermissions map[string]acpPendingPermission, permissionMu *sync.Mutex, startAdmission *providerStartAdmission, supervisor *core.ProcessSupervisor, cfg wrapConfig) error {
	defer stdin.Close()
	for {
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			return ignoreContextError(err)
		}
		switch typed := frame.(type) {
		case *protocol.ProviderStartPrepare, *protocol.ProviderStartAck:
			if err := startAdmission.deliver(typed); err != nil {
				return err
			}
		case *protocol.Command:
			switch typed.Type {
			case protocol.CommandSessionInterrupt:
				if err := writeACPRequest(stdin, nextID, "session/cancel", map[string]any{"sessionId": providerSessionID}); err != nil {
					return fmt.Errorf("write acp provider interrupt %s: %w", typed.CommandID, err)
				}
				nextID++
				if err := acknowledgeRunControl(ctx, typed, writeFrame, cfg, "interrupt", "ready", nil); err != nil {
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
				ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}
				if err := writeFrame(&ack); err != nil {
					return fmt.Errorf("ack acp permission response %s: %w", typed.CommandID, err)
				}
			case protocol.CommandSessionStop:
				if err := handleProviderRunControl(ctx, typed, supervisor, writeFrame, cfg, true); err != nil {
					return err
				}
				return nil
			default:
				ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: "unsupported acp provider command"}
				if err := writeFrame(&ack); err != nil {
					return fmt.Errorf("reject acp provider command %s: %w", typed.CommandID, err)
				}
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
		var message map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			return nil, fmt.Errorf("decode acp response %d: %w", id, err)
		}
		if fmt.Sprint(message["id"]) != fmt.Sprint(id) {
			continue
		}
		if errValue, ok := message["error"]; ok {
			return nil, fmt.Errorf("acp request %d failed: %v", id, errValue)
		}
		result, ok := message["result"].(map[string]any)
		if !ok {
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

func startAdapterHeartbeat(ctx context.Context, cfg heartbeatConfig, writeFrame func(protocol.Frame) error) (<-chan error, func(string)) {
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
	authenticator := localSessionAuthenticator{Authenticator: baseAuthenticator, staticAdapterCredential: baseAuthenticator.AdapterCredential, sessionCredentialIssuer: issuer, sessionID: cfg.SessionID, provider: cfg.Provider}
	sessionStore := localSessionStore{Store: eventStore, sessionID: cfg.SessionID}
	handshake := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: authenticator,
		EventStore:    sessionStore,
	})
	webSocketHandler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: sessionStore, SessionCredentialIssuer: issuer, SessionCredentialLifecycle: issuer})
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
	staticAdapterCredential func(context.Context, string, auth.Principal, string) (int64, int64, bool, error)
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
