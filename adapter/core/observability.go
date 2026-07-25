package core

import (
	"crypto/subtle"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
)

// NewAdapterObservabilityHandler is intended for a host-owned diagnostic
// listener. It deliberately keeps the Provider-facing listener as next and
// exposes no diagnostic route to a sandbox or remote caller.
func NewAdapterObservabilityHandler(token string, metrics *AdapterMetrics, next http.Handler) http.Handler {
	return &adapterObservabilityHandler{token: token, metrics: metrics, next: next}
}

type adapterObservabilityHandler struct {
	token   string
	metrics *AdapterMetrics
	next    http.Handler
}

func (h *adapterObservabilityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r != nil && (r.URL.Path == "/metrics" || strings.HasPrefix(r.URL.Path, "/debug/pprof")) {
		if !h.authorized(r) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte(h.metricsSnapshot().Prometheus()))
			return
		}
		h.servePprof(w, r)
		return
	}
	if h.next == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	h.next.ServeHTTP(w, r)
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

func (h *adapterObservabilityHandler) servePprof(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/debug/pprof")
	switch path {
	case "", "/", "/index.html":
		pprof.Index(w, r)
	case "/cmdline":
		pprof.Cmdline(w, r)
	case "/profile":
		pprof.Profile(w, r)
	case "/symbol":
		pprof.Symbol(w, r)
	case "/trace":
		pprof.Trace(w, r)
	default:
		name := strings.Trim(path, "/")
		if name == "" || strings.Contains(name, "/") {
			pprof.Index(w, r)
			return
		}
		if handler := pprof.Handler(name); handler != nil {
			handler.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}
}
