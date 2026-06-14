package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	HubURL       string
	SessionID    string
	Agent        string
	Provider     string
	AdapterToken string
	Format       string
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
		HubURL:       defaultWrapHubURL,
		SessionID:    defaultSessionID,
		Agent:        "claude",
		Provider:     defaultProvider,
		AdapterToken: defaultAdapterToken,
		Format:       "jsonstream",
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
	flags.BoolVar(&useACP, "acp", false, "read ACP JSON frames from stdin")
	flags.BoolVar(&useJSONStream, "jsonstream", false, "read Claude stream-json lines from stdin")
	if err := flags.Parse(args); err != nil {
		return wrapConfig{}, err
	}
	if flags.NArg() != 0 {
		return wrapConfig{}, fmt.Errorf("unexpected wrap arguments: %v", flags.Args())
	}
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

	events, err := translateWrapInput(ctx, cfg, stdin)
	if err != nil {
		return err
	}
	for _, ev := range events {
		event := ev
		if err := writeCLIProtocolFrame(ctx, conn, &event); err != nil {
			return fmt.Errorf("send event %s: %w", event.Type, err)
		}
	}
	return nil
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
