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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/auth/static"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/store/sqlite"
)

const (
	defaultServeAddr    = "127.0.0.1:8765"
	defaultSessionID    = "local"
	defaultProvider     = "claude-code"
	defaultControlToken = "local-control-token"
	defaultAdapterToken = "local-adapter-token"
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
	if len(args) == 0 {
		return errors.New("usage: agentwharf serve [options]")
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
