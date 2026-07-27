package hub

import (
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strings"
)

// NewObservabilityHandler adds the host-side diagnostic surface without
// exposing metrics or profiles to a client-facing WebSocket listener.
func NewObservabilityHandler(token string, next http.Handler) http.Handler {
	return &observabilityHandler{token: token, next: next}
}

type observabilityHandler struct {
	token string
	next  http.Handler
}

func (h *observabilityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" || strings.HasPrefix(r.URL.Path, "/debug/pprof") {
		if !h.authorized(r) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/metrics" {
			h.serveMetrics(w, r)
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

func (h *observabilityHandler) authorized(r *http.Request) bool {
	if h.token == "" || r == nil {
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

func (h *observabilityHandler) serveMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# HELP agentwharf_hub_info Static Hub build identity.\n# TYPE agentwharf_hub_info gauge\nagentwharf_hub_info{module=\"hub\",role=\"host\"} 1\n")
	_, _ = fmt.Fprintf(w, "# HELP agentwharf_hub_goroutines Current Go goroutine count.\n# TYPE agentwharf_hub_goroutines gauge\nagentwharf_hub_goroutines %d\n", runtime.NumGoroutine())
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	_, _ = fmt.Fprintf(w, "# HELP agentwharf_hub_heap_alloc_bytes Current heap allocation.\n# TYPE agentwharf_hub_heap_alloc_bytes gauge\nagentwharf_hub_heap_alloc_bytes %d\n", stats.HeapAlloc)
}

func (h *observabilityHandler) servePprof(w http.ResponseWriter, r *http.Request) {
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
