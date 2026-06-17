package main

import (
	"bufio"
	"context"
	"crypto/rand"
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
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/winghv/agentwharf/adapter/acp"
	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/adapter/fallback/jsonstream"
	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/auth/static"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store/sqlite"
	"nhooyr.io/websocket"
)

const (
	defaultServeAddr    = "127.0.0.1:8765"
	defaultSessionID    = "local"
	defaultProvider     = "claude-code"
	defaultControlToken = "local-control-token"
	defaultAdapterToken = "local-adapter-token"
	defaultWrapHubURL   = "ws://" + defaultServeAddr
)

var errUnsafeDefaultToken = errors.New("default local tokens require a loopback listen address")

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
		return errors.New("usage: agentwharf serve|wrap [options]")
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
		_, _ = fmt.Fprintf(stdout, "agentwharf serve listening on %s\n", running.wsURL)
		_, _ = fmt.Fprintf(stdout, "session_id=%s provider=%s\n", cfg.SessionID, cfg.Provider)
		return running.wait()
	case "wrap":
		cfg, err := parseWrapConfig(args[1:], stderr)
		if err != nil {
			return err
		}
		if err := runWrap(ctx, cfg, stdin); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "agentwharf wrap sent events for session_id=%s provider=%s\n", cfg.SessionID, cfg.Provider)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

type serveConfig struct {
	Addr         string
	DBPath       string
	SessionID    string
	Provider     string
	ControlToken string
	AdapterToken string
}

type wrapConfig struct {
	HubURL          string
	SessionID       string
	Agent           string
	Provider        string
	AdapterToken    string
	Format          string
	SecretDir       string
	ProviderCommand []string
}

func parseServeConfig(args []string, stderr io.Writer) (serveConfig, error) {
	cfg := serveConfig{
		Addr:         defaultServeAddr,
		DBPath:       defaultDBPath(),
		SessionID:    defaultSessionID,
		Provider:     defaultProvider,
		ControlToken: defaultControlToken,
		AdapterToken: defaultAdapterToken,
	}

	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.Addr, "addr", cfg.Addr, "listen address")
	flags.StringVar(&cfg.DBPath, "db", cfg.DBPath, "SQLite event store path")
	flags.StringVar(&cfg.SessionID, "session-id", cfg.SessionID, "local session id")
	flags.StringVar(&cfg.Provider, "provider", cfg.Provider, "provider name")
	flags.StringVar(&cfg.ControlToken, "control-token", cfg.ControlToken, "client control token")
	flags.StringVar(&cfg.AdapterToken, "adapter-token", cfg.AdapterToken, "adapter token")
	if err := flags.Parse(args); err != nil {
		return serveConfig{}, err
	}
	if flags.NArg() != 0 {
		return serveConfig{}, fmt.Errorf("unexpected serve arguments: %v", flags.Args())
	}
	return normalizeServeConfig(cfg)
}

func parseWrapConfig(args []string, stderr io.Writer) (wrapConfig, error) {
	cfg := wrapConfig{
		HubURL:       envOrDefault("AGENTWHARF_HUB_URL", defaultWrapHubURL),
		SessionID:    envOrDefault("AGENTWHARF_SESSION_ID", defaultSessionID),
		Agent:        envOrDefault("AGENTWHARF_AGENT", "claude"),
		Provider:     envOrDefault("AGENTWHARF_PROVIDER", ""),
		AdapterToken: envOrDefault("AGENTWHARF_ADAPTER_TOKEN", defaultAdapterToken),
		Format:       envOrDefault("AGENTWHARF_FORMAT", "jsonstream"),
		SecretDir:    envOrDefault("AGENTWHARF_SECRET_DIR", ""),
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
	flags.BoolVar(&useACP, "acp", false, "read ACP JSON frames from stdin")
	flags.BoolVar(&useJSONStream, "jsonstream", false, "read Claude stream-json lines from stdin")
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
	if !isLoopbackAddr(cfg.Addr) && usesDefaultToken(cfg) {
		return serveConfig{}, errUnsafeDefaultToken
	}
	return cfg, nil
}

func normalizeWrapConfig(cfg wrapConfig) (wrapConfig, error) {
	if cfg.HubURL == "" {
		cfg.HubURL = defaultWrapHubURL
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
	if cfg.AdapterToken == "" {
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
	if cfg.AdapterToken == defaultAdapterToken && !isLoopbackURL(cfg.HubURL) {
		return wrapConfig{}, errUnsafeDefaultToken
	}
	cfg.SecretDir = filepath.Clean(cfg.SecretDir)
	if cfg.SecretDir == "." {
		cfg.SecretDir = ""
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

func envOrDefault(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
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

func runWrap(ctx context.Context, cfg wrapConfig, stdin io.Reader) error {
	cfg, err := normalizeWrapConfig(cfg)
	if err != nil {
		return err
	}
	if stdin == nil {
		stdin = io.Reader(os.Stdin)
	}

	conn, _, err := websocket.Dial(ctx, cfg.HubURL, nil)
	if err != nil {
		return fmt.Errorf("connect hub: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	state, err := core.NewAdapterConnectionState(core.AdapterConnectionConfig{
		SessionID: cfg.SessionID,
		Provider:  cfg.Provider,
		Token:     cfg.AdapterToken,
	})
	if err != nil {
		return err
	}
	hello := state.Hello()
	if err := writeCLIProtocolFrame(ctx, conn, &hello); err != nil {
		return fmt.Errorf("send adapter hello: %w", err)
	}
	frame, err := readCLIProtocolFrame(ctx, conn)
	if err != nil {
		return fmt.Errorf("read hello ack: %w", err)
	}
	ack, ok := frame.(*protocol.HelloAck)
	if !ok {
		return fmt.Errorf("read hello ack: got %T", frame)
	}
	if _, err := state.MarkAccepted(*ack); err != nil {
		return err
	}
	masker, err := eventMaskerFromSecretDir(cfg.SecretDir)
	if err != nil {
		return err
	}

	if len(cfg.ProviderCommand) > 0 {
		return runWrapProvider(ctx, cfg, conn, masker)
	}

	events, err := translateWrapInput(ctx, cfg, stdin)
	if err != nil {
		return err
	}
	for _, ev := range events {
		event, err := maskEvent(masker, ev)
		if err != nil {
			return err
		}
		if err := writeCLIProtocolFrame(ctx, conn, &event); err != nil {
			return fmt.Errorf("send event %s: %w", event.Type, err)
		}
	}
	return nil
}

func runWrapProvider(ctx context.Context, cfg wrapConfig, conn *websocket.Conn, masker *core.EventMasker) error {
	if cfg.Format == "acp" {
		return runWrapACPProvider(ctx, cfg, conn, masker)
	}

	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create provider stdin pipe: %w", err)
	}
	defer stdinReader.Close()
	defer stdinWriter.Close()
	stdoutReader, stdoutWriter := io.Pipe()
	supervisor, err := core.NewProcessSupervisor(core.ProcessConfig{
		Command: core.ProcessCommand{
			Path:   cfg.ProviderCommand[0],
			Args:   cfg.ProviderCommand[1:],
			Stdin:  stdinReader,
			Stdout: stdoutWriter,
			Stderr: os.Stderr,
		},
	})
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	processDone := make(chan error, 1)
	outputDone := make(chan error, 1)
	commandDone := make(chan error, 1)
	var writeMu sync.Mutex
	writeFrame := func(frame protocol.Frame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeCLIProtocolFrame(runCtx, conn, frame)
	}
	go func() {
		err := supervisor.Run(runCtx)
		_ = stdoutWriter.Close()
		processDone <- err
	}()
	go func() {
		outputDone <- streamProviderOutput(runCtx, cfg, stdoutReader, func(event protocol.Event) error {
			masked, err := maskEvent(masker, event)
			if err != nil {
				return err
			}
			return writeFrame(&masked)
		})
	}()
	go func() {
		commandDone <- forwardHubCommandsToProvider(runCtx, conn, stdinWriter, writeFrame)
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
		case <-ctx.Done():
			cancel()
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = supervisor.Stop(stopCtx)
			stopCancel()
			return fmt.Errorf("wrap provider context done (process_done=%t output_done=%t): %w", processFinished, outputFinished, ctx.Err())
		}
	}
}

func runWrapACPProvider(ctx context.Context, cfg wrapConfig, conn *websocket.Conn, masker *core.EventMasker) error {
	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create provider stdin pipe: %w", err)
	}
	defer stdinReader.Close()
	defer stdinWriter.Close()
	stdoutReader, stdoutWriter := io.Pipe()
	supervisor, err := core.NewProcessSupervisor(core.ProcessConfig{
		Command: core.ProcessCommand{
			Path:   cfg.ProviderCommand[0],
			Args:   cfg.ProviderCommand[1:],
			Stdin:  stdinReader,
			Stdout: stdoutWriter,
			Stderr: os.Stderr,
		},
	})
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	processDone := make(chan error, 1)
	go func() {
		err := supervisor.Run(runCtx)
		_ = stdoutWriter.Close()
		processDone <- err
	}()

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
	if err := sendACPProviderReadyEvent(runCtx, conn, cfg, providerSessionID, masker); err != nil {
		cancel()
		return err
	}

	outputDone := make(chan error, 1)
	commandDone := make(chan error, 1)
	var writeMu sync.Mutex
	var permissionMu sync.Mutex
	pendingPermissions := make(map[string]acpPendingPermission)
	writeFrame := func(frame protocol.Frame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeCLIProtocolFrame(runCtx, conn, frame)
	}
	go func() {
		outputDone <- streamACPProviderOutput(runCtx, cfg, scanner, func(line []byte) {
			trackACPPermissionRequest(line, pendingPermissions, &permissionMu)
		}, func(event protocol.Event) error {
			masked, err := maskEvent(masker, event)
			if err != nil {
				return err
			}
			return writeFrame(&masked)
		})
	}()
	go func() {
		commandDone <- forwardHubCommandsToACPProvider(runCtx, conn, stdinWriter, writeFrame, providerSessionID, 3, pendingPermissions, &permissionMu)
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
		case <-ctx.Done():
			cancel()
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = supervisor.Stop(stopCtx)
			stopCancel()
			return fmt.Errorf("wrap acp provider context done (process_done=%t output_done=%t): %w", processFinished, outputFinished, ctx.Err())
		}
	}
}

func sendACPProviderReadyEvent(ctx context.Context, conn *websocket.Conn, cfg wrapConfig, providerSessionID string, masker *core.EventMasker) error {
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
	if err := writeCLIProtocolFrame(ctx, conn, &event); err != nil {
		return fmt.Errorf("send acp ready event: %w", err)
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

func forwardHubCommandsToProvider(ctx context.Context, conn *websocket.Conn, stdin io.WriteCloser, writeFrame func(protocol.Frame) error) error {
	defer stdin.Close()
	for {
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			return ignoreContextError(err)
		}
		switch typed := frame.(type) {
		case *protocol.Command:
			if err := writeProviderCommand(stdin, typed); err != nil {
				return err
			}
			ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}
			if err := writeFrame(&ack); err != nil {
				return fmt.Errorf("ack provider command %s: %w", typed.CommandID, err)
			}
			if typed.Type == protocol.CommandSessionStop {
				return nil
			}
		case *protocol.Ping:
			if err := writeFrame(&protocol.Pong{Nonce: typed.Nonce}); err != nil {
				return fmt.Errorf("send pong: %w", err)
			}
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

func forwardHubCommandsToACPProvider(ctx context.Context, conn *websocket.Conn, stdin io.WriteCloser, writeFrame func(protocol.Frame) error, providerSessionID string, nextID int64, pendingPermissions map[string]acpPendingPermission, permissionMu *sync.Mutex) error {
	defer stdin.Close()
	for {
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			return ignoreContextError(err)
		}
		switch typed := frame.(type) {
		case *protocol.Command:
			switch typed.Type {
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
				ack := protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}
				if err := writeFrame(&ack); err != nil {
					return fmt.Errorf("ack acp provider stop %s: %w", typed.CommandID, err)
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
	var decoded struct {
		Content []struct {
			Kind string `json:"kind"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("invalid session.send payload: %w", err)
	}
	prompt := make([]map[string]any, 0, len(decoded.Content))
	for _, part := range decoded.Content {
		if part.Kind != "text" || part.Text == "" {
			continue
		}
		prompt = append(prompt, map[string]any{
			"type": "text",
			"text": part.Text,
		})
	}
	if len(prompt) == 0 {
		return nil, errors.New("session.send payload has no text content")
	}
	return prompt, nil
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
	server *http.Server
	store  interface{ Close() error }
	done   chan error
	addr   string
	wsURL  string
}

func startServe(ctx context.Context, cfg serveConfig) (*runningServe, error) {
	cfg, err := normalizeServeConfig(cfg)
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

	authenticator := static.New([]static.Token{
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
	handshake := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: authenticator,
		EventStore:    eventStore,
		SessionLookup: singleSessionLookup{
			sessionID: cfg.SessionID,
			provider:  cfg.Provider,
			state:     "ready",
		},
	})
	server := &http.Server{
		Handler:           hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: eventStore}),
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

func (r *runningServe) wait() error {
	serveErr := <-r.done
	closeErr := r.store.Close()
	if serveErr != nil {
		return serveErr
	}
	if closeErr != nil {
		return fmt.Errorf("close sqlite store: %w", closeErr)
	}
	return nil
}

type singleSessionLookup struct {
	sessionID string
	provider  string
	state     string
}

func (s singleSessionLookup) LookupSession(_ context.Context, sessionID string) (hub.SessionInfo, error) {
	if sessionID != s.sessionID {
		return hub.SessionInfo{}, hub.ErrSessionNotFound
	}
	return hub.SessionInfo{State: s.state, Provider: s.provider}, nil
}
