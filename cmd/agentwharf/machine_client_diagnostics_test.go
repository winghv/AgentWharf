package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

type recordedClientDiagnostic struct {
	Stage         string `json:"stage"`
	Reason        string `json:"reason"`
	SessionID     string `json:"session_id"`
	Attempt       int    `json:"attempt"`
	AgentVersion  string `json:"agent_version"`
	AgentPlatform string `json:"agent_platform"`
}

func diagnosticTestServer(t *testing.T) (*httptest.Server, func() []recordedClientDiagnostic) {
	t.Helper()
	var mu sync.Mutex
	var reports []recordedClientDiagnostic
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/machines/machine/client-diagnostics" {
			http.NotFound(w, r)
			return
		}
		var report recordedClientDiagnostic
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		reports = append(reports, report)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	return server, func() []recordedClientDiagnostic {
		mu.Lock()
		defer mu.Unlock()
		return append([]recordedClientDiagnostic(nil), reports...)
	}
}

func diagnosticTestCredential(server *httptest.Server) machineCredential {
	return machineCredential{CloudAPIURL: server.URL, MachineID: "machine", MachineToken: "test"}
}

// A machine that keeps failing the same step collapses into one report and
// counts its attempts, so a 2-second poll loop cannot become a row storm.
func TestMachineClientDiagnosticsCollapsesRepeatsAndCountsAttempts(t *testing.T) {
	t.Parallel()

	server, reports := diagnosticTestServer(t)
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	diagnostics := newMachineClientDiagnostics(server.Client(), diagnosticTestCredential(server))
	diagnostics.now = func() time.Time { return now }

	for i := 0; i < 5; i++ {
		diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "")
	}
	recorded := reports()
	if len(recorded) != 1 {
		t.Fatalf("reports = %d, want 1 collapsed report", len(recorded))
	}
	if recorded[0].Attempt != 1 || recorded[0].Stage != "claim_pickup" || recorded[0].Reason != "list_unavailable" {
		t.Fatalf("first report = %+v", recorded[0])
	}

	// The next window reports everything counted since the last one, so support
	// learns how often the machine actually hit this, not just that it did.
	now = now.Add(diagnosticReportInterval + time.Second)
	diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "")
	diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "")
	now = now.Add(diagnosticReportInterval + time.Second)
	diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "")

	recorded = reports()
	if len(recorded) != 3 {
		t.Fatalf("reports = %d, want 3", len(recorded))
	}
	if recorded[1].Attempt != 5 {
		t.Fatalf("second report attempt = %d, want the 5 failures since the first report", recorded[1].Attempt)
	}
	if recorded[2].Attempt != 2 {
		t.Fatalf("third report attempt = %d, want the 2 failures since the second report", recorded[2].Attempt)
	}
}

// The report always names the version the daemon is actually running, because
// the platform has no other authority for it.
func TestMachineClientDiagnosticsAlwaysCarryAgentVersion(t *testing.T) {
	t.Parallel()

	server, reports := diagnosticTestServer(t)
	diagnostics := newMachineClientDiagnostics(server.Client(), diagnosticTestCredential(server))
	diagnostics.record(context.Background(), "initialization_discovery", "runtime_unavailable", "")

	recorded := reports()
	if len(recorded) != 1 {
		t.Fatalf("reports = %d, want 1", len(recorded))
	}
	if recorded[0].AgentVersion == "" {
		t.Fatal("report carried no agent version")
	}
	if recorded[0].AgentPlatform == "" {
		t.Fatal("report carried no agent platform")
	}
}

// The stage/Session pairing is the platform's contract. A mispaired report is
// dropped locally rather than sent and rejected, so the ledger cannot fill with
// rows the platform must refuse.
func TestMachineClientDiagnosticsNeverSendsMispairedScope(t *testing.T) {
	t.Parallel()

	server, reports := diagnosticTestServer(t)
	diagnostics := newMachineClientDiagnostics(server.Client(), diagnosticTestCredential(server))

	diagnostics.record(context.Background(), "initialization_delivery", "runtime_unavailable", "")
	diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "session_1")
	if recorded := reports(); len(recorded) != 0 {
		t.Fatalf("mispaired reports = %+v, want none", recorded)
	}

	diagnostics.record(context.Background(), "initialization_delivery", "runtime_unavailable", "session_1")
	diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "")
	recorded := reports()
	if len(recorded) != 2 {
		t.Fatalf("paired reports = %d, want 2", len(recorded))
	}
}

// Reporting is an observation, never a dependency: an unreachable platform must
// not change what the machine does next.
func TestMachineClientDiagnosticsSurvivesUnreachablePlatform(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	diagnostics := newMachineClientDiagnostics(server.Client(), diagnosticTestCredential(server))
	diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "")
}

// The dedupe map is bounded so a long-lived daemon cannot accumulate one entry
// per Session it ever failed on.
func TestMachineClientDiagnosticsBoundTheirState(t *testing.T) {
	t.Parallel()

	server, _ := diagnosticTestServer(t)
	diagnostics := newMachineClientDiagnostics(server.Client(), diagnosticTestCredential(server))
	// Each Session fails once inside a single window, so the map fills with real
	// dedupe entries and the cap is what stops it growing without bound.
	for i := 0; i < diagnosticStateCap*2; i++ {
		diagnostics.record(context.Background(), "initialization_delivery", "runtime_unavailable", "session_"+strconv.Itoa(i))
	}
	diagnostics.mu.Lock()
	entries := len(diagnostics.state)
	diagnostics.mu.Unlock()
	if entries > diagnosticStateCap {
		t.Fatalf("dedupe entries = %d, want at most %d", entries, diagnosticStateCap)
	}

	// Entries for Sessions that stopped failing age out instead of accumulating.
	diagnostics.now = func() time.Time { return time.Now().Add(diagnosticStateTTL + time.Minute) }
	diagnostics.record(context.Background(), "claim_pickup", "list_unavailable", "")
	diagnostics.mu.Lock()
	entries = len(diagnostics.state)
	diagnostics.mu.Unlock()
	if entries > 2 {
		t.Fatalf("entries after TTL = %d, want the map to shrink", entries)
	}
}

func TestMachineClientDiagnosticsIgnoreEmptyInput(t *testing.T) {
	t.Parallel()

	server, reports := diagnosticTestServer(t)
	diagnostics := newMachineClientDiagnostics(server.Client(), diagnosticTestCredential(server))
	diagnostics.record(context.Background(), "", "runtime_unavailable", "")
	diagnostics.record(context.Background(), "claim_pickup", "", "")
	if recorded := reports(); len(recorded) != 0 {
		t.Fatalf("reports = %+v, want none", recorded)
	}
	// A nil reporter must be a no-op, not a panic: several call sites pass one.
	var missing *machineClientDiagnostics
	missing.record(context.Background(), "claim_pickup", "list_unavailable", "")
}
