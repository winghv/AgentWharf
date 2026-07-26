package core

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NewAdapterObservabilityHandler is intended for a host-owned diagnostic
// listener. It deliberately keeps the Provider-facing listener as next and
// exposes no diagnostic route to a sandbox or remote caller.
func NewAdapterObservabilityHandler(token string, metrics *AdapterMetrics, next http.Handler) http.Handler {
	return NewAdapterObservabilityHandlerAt(token, metrics, "", next)
}

// NewAdapterObservabilityHandlerAt mounts the Adapter diagnostic surface on a
// dedicated path prefix when Hub and Adapter share one host listener.
func NewAdapterObservabilityHandlerAt(token string, metrics *AdapterMetrics, prefix string, next http.Handler) http.Handler {
	prefix = strings.TrimSuffix("/"+strings.Trim(prefix, "/"), "/")
	if prefix == "/" {
		prefix = ""
	}
	return &adapterObservabilityHandler{token: token, metrics: metrics, prefix: prefix, next: next}
}

type adapterObservabilityHandler struct {
	token       string
	metrics     *AdapterMetrics
	prefix      string
	next        http.Handler
	unixPeer    bool
	profileSlot chan struct{}
}

func (h *adapterObservabilityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := h.diagnosticPath(r)
	if path == "/metrics" || strings.HasPrefix(path, "/debug/pprof") {
		if h.unixPeer {
			if err := validateDiagnosticRequest(r); err != nil {
				http.Error(w, "", http.StatusRequestEntityTooLarge)
				return
			}
			budget, _ := r.Context().Value(diagnosticBudgetKey{}).(*diagnosticBudget)
			if budget != nil && !budget.allow() {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		} else if !h.authorized(r) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if path == "/metrics" {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = (&diagnosticResponseWriter{ResponseWriter: w}).Write([]byte(h.metricsSnapshot().Prometheus()))
			return
		}
		h.servePprof(&diagnosticResponseWriter{ResponseWriter: w}, path, r)
		return
	}
	if h.next == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	h.next.ServeHTTP(w, r)
}

func (h *adapterObservabilityHandler) diagnosticPath(r *http.Request) string {
	if h == nil || r == nil || r.URL == nil {
		return ""
	}
	if h.prefix == "" {
		return r.URL.Path
	}
	if r.URL.Path == h.prefix {
		return "/"
	}
	if !strings.HasPrefix(r.URL.Path, h.prefix+"/") {
		return ""
	}
	return strings.TrimPrefix(r.URL.Path, h.prefix)
}

func (h *adapterObservabilityHandler) authorized(r *http.Request) bool {
	if h == nil || h.token == "" || r == nil {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return false
	}
	want := "Bearer " + h.token
	got := r.Header.Get("Authorization")
	return len(got) == len(want) && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (h *adapterObservabilityHandler) metricsSnapshot() AdapterMetricSnapshot {
	if h == nil || h.metrics == nil {
		return AdapterMetricSnapshot{}
	}
	return h.metrics.Snapshot()
}

func (h *adapterObservabilityHandler) servePprof(w http.ResponseWriter, path string, r *http.Request) {
	path = strings.TrimPrefix(path, "/debug/pprof")
	switch path {
	case "", "/", "/index.html":
		pprof.Index(w, r)
	case "/cmdline":
		// pprof.Cmdline returns os.Args verbatim. The wrap command accepts
		// bearer credentials, so exposing this route would turn diagnostics
		// into a credential disclosure channel.
		http.NotFound(w, r)
	case "/profile", "/trace":
		seconds, err := boundedProfileSeconds(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if h.profileSlot != nil {
			select {
			case h.profileSlot <- struct{}{}:
				defer func() { <-h.profileSlot }()
			default:
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		}
		query := r.URL.Query()
		query.Set("seconds", strconv.Itoa(seconds))
		r2 := r.Clone(r.Context())
		r2.URL.RawQuery = query.Encode()
		if path == "/profile" {
			pprof.Profile(w, r2)
		} else {
			pprof.Trace(w, r2)
		}
	case "/symbol":
		pprof.Symbol(w, r)
	default:
		name := strings.Trim(path, "/")
		if name == "" || strings.Contains(name, "/") {
			pprof.Index(w, r)
			return
		}
		if handler := pprof.Handler(name); handler != nil {
			if raw, present := r.URL.Query()["seconds"]; present {
				if len(raw) != 1 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				seconds, err := boundedProfileSeconds(r)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if h.profileSlot != nil {
					select {
					case h.profileSlot <- struct{}{}:
						defer func() { <-h.profileSlot }()
					default:
						w.WriteHeader(http.StatusTooManyRequests)
						return
					}
				}
				query := r.URL.Query()
				query.Set("seconds", strconv.Itoa(seconds))
				r2 := r.Clone(r.Context())
				r2.URL.RawQuery = query.Encode()
				handler.ServeHTTP(w, r2)
				return
			}
			handler.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}
}

func validateDiagnosticRequest(r *http.Request) error {
	if r == nil || r.Body == nil {
		return nil
	}
	if r.ContentLength > 0 || len(r.TransferEncoding) > 0 {
		return errors.New("diagnostic request body is forbidden")
	}
	return nil
}

func boundedProfileSeconds(r *http.Request) (int, error) {
	seconds := 30
	if raw := r.URL.Query().Get("seconds"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 30 {
			return 0, errors.New("profile seconds out of range")
		}
		seconds = value
	}
	return seconds, nil
}

type diagnosticResponseWriter struct {
	http.ResponseWriter
	written int64
}

func (w *diagnosticResponseWriter) Write(p []byte) (int, error) {
	const maxResponseBytes = 16 << 20
	if w.written >= maxResponseBytes {
		return 0, http.ErrAbortHandler
	}
	remaining := maxResponseBytes - w.written
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := w.ResponseWriter.Write(p)
	w.written += int64(n)
	if n < len(p) {
		return n, http.ErrAbortHandler
	}
	if int64(len(p)) < remaining {
		return n, err
	}
	return n, http.ErrAbortHandler
}

type diagnosticBudget struct {
	mu       sync.Mutex
	started  time.Time
	requests int
}

func (b *diagnosticBudget) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.started.IsZero() || now.Sub(b.started) >= time.Minute {
		b.started = now
		b.requests = 0
	}
	if b.requests >= 4 {
		return false
	}
	b.requests++
	return true
}

type diagnosticBudgetKey struct{}

// AdapterDiagnosticsServer is a fixed-entry-owned Unix diagnostic listener.
// It never falls back to TCP or loopback when the socket cannot be proven.
type AdapterDiagnosticsServer struct {
	server *http.Server
	ln     *peerUnixListener
	path   string
	cancel context.CancelFunc
	once   sync.Once
}

type AdapterDiagnosticsConfig struct {
	SocketPath    string
	RootPath      string
	MarkerPath    string
	Metrics       *AdapterMetrics
	FixedEntryUID uint32
	OperatorUID   uint32
	ProviderUID   uint32
}

func ValidateAdapterDiagnosticsRoot(rootPath, markerPath string, ownerUID uint32) error {
	if rootPath == "" || markerPath == "" || filepath.Dir(filepath.Clean(markerPath)) != filepath.Clean(rootPath) {
		return errors.New("diagnostic fixed-entry paths are invalid")
	}
	if err := validateFixedEntryPath(rootPath); err != nil {
		return err
	}
	if err := validateFixedEntryPath(markerPath); err != nil {
		return err
	}
	rootInfo, err := os.Lstat(rootPath)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode().Perm() != 0o700 {
		return errors.New("diagnostic fixed-entry root is unavailable")
	}
	if uid, ok := pathOwnerUID(rootInfo); !ok || uid != ownerUID {
		return errors.New("diagnostic fixed-entry root owner is untrusted")
	}
	markerInfo, err := os.Lstat(markerPath)
	if err != nil || !markerInfo.Mode().IsRegular() || markerInfo.Mode().Perm() != 0o600 {
		return errors.New("diagnostic health marker is unavailable")
	}
	if uid, ok := pathOwnerUID(markerInfo); !ok || uid != ownerUID {
		return errors.New("diagnostic health marker owner is untrusted")
	}
	return nil
}

func StartAdapterDiagnostics(ctx context.Context, socketPath string, metrics *AdapterMetrics) (*AdapterDiagnosticsServer, error) {
	return nil, errors.New("fixed-entry diagnostics configuration is required")
}

func StartAdapterDiagnosticsWithConfig(ctx context.Context, cfg AdapterDiagnosticsConfig) (*AdapterDiagnosticsServer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.SocketPath == "" || cfg.RootPath == "" || cfg.MarkerPath == "" || cfg.FixedEntryUID == 0 || cfg.OperatorUID == 0 || cfg.ProviderUID == 0 || cfg.OperatorUID == cfg.ProviderUID || cfg.FixedEntryUID == cfg.ProviderUID {
		return nil, errors.New("diagnostic identity and paths are required")
	}
	if filepath.Base(filepath.Clean(cfg.SocketPath)) != "diagnostics.sock" || filepath.Dir(filepath.Clean(cfg.SocketPath)) != filepath.Clean(cfg.RootPath) {
		return nil, errors.New("diagnostic socket must be directly under its fixed-entry root")
	}
	if err := ValidateAdapterDiagnosticsRoot(cfg.RootPath, cfg.MarkerPath, cfg.FixedEntryUID); err != nil {
		return nil, err
	}
	if err := validateFixedEntryPath(cfg.SocketPath); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(cfg.SocketPath); err == nil {
		// A stale or foreign entry is evidence that the fixed-entry proof is lost.
		return nil, errors.New("diagnostic socket path is occupied")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect diagnostic socket: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: cfg.SocketPath, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on diagnostic socket: %w", err)
	}
	if err := os.Chmod(cfg.SocketPath, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(cfg.SocketPath)
		return nil, fmt.Errorf("protect diagnostic socket: %w", err)
	}
	peer := &peerUnixListener{listener: listener, operatorUID: cfg.OperatorUID, slots: make(chan struct{}, 2)}
	handler := &adapterObservabilityHandler{metrics: cfg.Metrics, unixPeer: true, profileSlot: make(chan struct{}, 1)}
	budget := &diagnosticBudget{}
	serverCtx, cancel := context.WithCancel(ctx)
	server := &http.Server{
		Handler:        handler,
		MaxHeaderBytes: 8 << 10,
		ReadTimeout:    35 * time.Second,
		WriteTimeout:   35 * time.Second,
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, diagnosticBudgetKey{}, budget)
		},
		BaseContext: func(net.Listener) context.Context { return serverCtx },
	}
	result := &AdapterDiagnosticsServer{server: server, ln: peer, path: cfg.SocketPath, cancel: cancel}
	go func() {
		if ctx != nil {
			<-ctx.Done()
			result.Close()
		}
	}()
	go func() { _ = server.Serve(peer) }()
	return result, nil
}

func validateFixedEntryPath(path string) error {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return errors.New("diagnostic fixed-entry path must be absolute")
	}
	for current := clean; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && current == clean {
				return nil
			}
			return fmt.Errorf("inspect diagnostic fixed-entry path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("diagnostic fixed-entry path contains symlink")
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
	}
}

func (s *AdapterDiagnosticsServer) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *AdapterDiagnosticsServer) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		_ = s.ln.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = s.server.Shutdown(ctx)
		if err != nil && (errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")) {
			err = nil
		}
		if closeErr := s.server.Close(); err == nil && closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			err = closeErr
		}
		if removeErr := os.Remove(s.path); !errors.Is(removeErr, os.ErrNotExist) {
			if err == nil {
				err = removeErr
			}
		}
	})
	return err
}

type peerUnixListener struct {
	listener    *net.UnixListener
	operatorUID uint32
	slots       chan struct{}
}

func (l *peerUnixListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.listener.AcceptUnix()
		if err != nil {
			return nil, err
		}
		uid, err := unixPeerUID(conn)
		if err != nil || uid != l.operatorUID {
			_ = conn.Close()
			continue
		}
		select {
		case l.slots <- struct{}{}:
			return &limitedUnixConn{UnixConn: conn, release: func() { <-l.slots }}, nil
		default:
			_ = conn.Close()
		}
	}
}

func (l *peerUnixListener) Close() error   { return l.listener.Close() }
func (l *peerUnixListener) Addr() net.Addr { return l.listener.Addr() }

type limitedUnixConn struct {
	*net.UnixConn
	release func()
	once    sync.Once
}

func (c *limitedUnixConn) Close() error {
	err := c.UnixConn.Close()
	c.once.Do(c.release)
	return err
}

func unixPeerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var controlErr error
	var uid uint32
	if err := raw.Control(func(fd uintptr) {
		uid, controlErr = peerUID(int(fd))
	}); err != nil {
		return 0, err
	}
	if controlErr != nil {
		return 0, controlErr
	}
	return uid, nil
}
