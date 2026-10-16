package main

import (
	"context"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/winghv/agentwharf/internal/buildinfo"
)

// Client-side launch failures used to end as one local stderr line. The platform
// could see that a Task stopped moving but never why, so support had to ask the
// user to paste local logs. This reporter closes that gap: it names the machine
// launch step that stopped, using only the closed vocabulary the platform
// accepts.
//
// What it never sends: free text, paths, command lines, exit output, prompts or
// any user content. The platform stores the stage and reason class and nothing
// else, which is what makes the report safe to keep as durable triage evidence.
//
// Reporting is strictly best effort. A failed report must never change what the
// machine does next, so every call here swallows its own errors.
const (
	diagnosticStageInitializationDiscovery = "initialization_discovery"
	diagnosticStageInitializationDelivery  = "initialization_delivery"
	diagnosticStageClaimPickup             = "claim_pickup"
	diagnosticStageClaimExchange           = "claim_exchange"
	// diagnosticStageAdapterLaunch covers the provider launch itself, after the
	// claim was already exchanged. The four stages above stop at the claim
	// boundary, so a machine that took the claim and then refused to launch it --
	// an unavailable provider capability, a settings request the provider will
	// not accept, a readback that never matches -- was exactly the failure the
	// platform still could not see.
	diagnosticStageAdapterLaunch = "adapter_launch"
)

// diagnosticSessionStages must name a Session: they describe one Session's
// delivery or launch and cannot be attributed to the machine as a whole.
var diagnosticSessionStages = map[string]bool{
	diagnosticStageInitializationDelivery: true,
	diagnosticStageClaimExchange:          true,
	diagnosticStageAdapterLaunch:          true,
}

// diagnosticReportInterval is how long the same failure is collapsed into one
// report. Attempts are counted locally and sent with the next report, so a
// machine that fails every poll tells support how often it failed without
// writing a row per poll.
const diagnosticReportInterval = 5 * time.Minute

// diagnosticStateCap bounds the dedupe map. A daemon that runs for months would
// otherwise accumulate one entry per Session it ever failed on.
const diagnosticStateCap = 512

// diagnosticStateTTL retires dedupe entries for Sessions that stopped failing,
// so the map shrinks back on a healthy machine.
const diagnosticStateTTL = time.Hour

type machineClientDiagnosticState struct {
	attempts   int
	reportedAt time.Time
	updatedAt  time.Time
}

// machineClientDiagnostics reports bounded facts about this machine's own
// execution. It is safe for concurrent use: the daemon reports from its
// maintenance loop, its fast claim loop and per-Session workers at the same time.
type machineClientDiagnostics struct {
	client     *http.Client
	credential machineCredential
	interval   time.Duration
	now        func() time.Time

	mu    sync.Mutex
	state map[string]*machineClientDiagnosticState
}

func newMachineClientDiagnostics(client *http.Client, credential machineCredential) *machineClientDiagnostics {
	return &machineClientDiagnostics{
		client:     client,
		credential: credential,
		interval:   diagnosticReportInterval,
		now:        time.Now,
		state:      make(map[string]*machineClientDiagnosticState),
	}
}

// record notes a failure and reports it when the previous report for the same
// stage, reason and Session has aged out. attempt counts every observation
// since that report, so support sees both the class and the frequency.
func (d *machineClientDiagnostics) record(ctx context.Context, stage, reason, sessionID string) {
	if d == nil || d.client == nil || stage == "" || reason == "" {
		return
	}
	now := d.now()
	key := stage + "|" + reason + "|" + sessionID
	d.mu.Lock()
	d.pruneLocked(now)
	state, known := d.state[key]
	if !known {
		if len(d.state) >= diagnosticStateCap {
			// The cap wins over this one observation: dropping a report is
			// cheaper than unbounded memory on a daemon that runs for months.
			d.mu.Unlock()
			return
		}
		state = &machineClientDiagnosticState{}
		d.state[key] = state
	}
	state.updatedAt = now
	if known && now.Sub(state.reportedAt) < d.interval {
		// Inside the window: count the failure but stay quiet.
		state.attempts++
		d.mu.Unlock()
		return
	}
	// The window closed: report every failure since the last report, so support
	// sees both the class and how often the machine actually hit it.
	attempt := 1
	if known {
		attempt += state.attempts
	}
	state.attempts = 0
	state.reportedAt = now
	d.mu.Unlock()

	d.send(ctx, stage, reason, sessionID, attempt)
}

// pruneLocked keeps the dedupe map bounded. Healthy entries age out; when the
// cap is still reached the newest failure is skipped rather than evicting a
// live entry, because losing one report is cheaper than losing another.
func (d *machineClientDiagnostics) pruneLocked(now time.Time) {
	for key, state := range d.state {
		if now.Sub(state.updatedAt) > diagnosticStateTTL {
			delete(d.state, key)
		}
	}
}

func (d *machineClientDiagnostics) send(ctx context.Context, stage, reason, sessionID string, attempt int) {
	if sessionID == "" && diagnosticSessionStages[stage] {
		return
	}
	if sessionID != "" && !diagnosticSessionStages[stage] {
		return
	}
	endpoint, err := cloudAPIEndpoint(d.credential.CloudAPIURL, "/machines/"+url.PathEscape(d.credential.MachineID)+"/client-diagnostics")
	if err != nil {
		return
	}
	// A short budget: reporting is an observation, and a stalled platform must
	// never delay the machine's actual work.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	status, _, err := postCloudAPIJSON(ctx, d.client, endpoint, d.credential.MachineToken, map[string]any{
		"stage":          stage,
		"reason":         reason,
		"session_id":     sessionID,
		"attempt":        attempt,
		"agent_version":  machineDiagnosticAgentVersion(),
		"agent_platform": machineDiagnosticAgentPlatform(),
	})
	// Any status, including a rejection of an older vocabulary, means the report
	// did not land. There is nothing useful to do about it locally: the machine
	// keeps launching work either way.
	_ = status
	_ = err
}

// machineDiagnosticAgentVersion reports the daemon version actually running.
// The platform has no other authority for this: what pairing recorded is a
// browser-supplied claim, not a measurement.
func machineDiagnosticAgentVersion() string {
	version := strings.TrimSpace(buildinfo.Version)
	if version == "" {
		return "unknown"
	}
	if len(version) > 64 {
		return version[:64]
	}
	return version
}

func machineDiagnosticAgentPlatform() string {
	platform := runtime.GOOS + "-" + runtime.GOARCH
	if len(platform) > 32 {
		return platform[:32]
	}
	return platform
}

// reportMachineInitializationDiscovery records a machine-wide failure to even
// look for pending encrypted initializations.
func reportMachineInitializationDiscovery(ctx context.Context, diagnostics *machineClientDiagnostics, reason string) {
	if diagnostics != nil {
		diagnostics.record(ctx, diagnosticStageInitializationDiscovery, reason, "")
	}
}

// reportMachineInitializationDelivery records a failure to answer one Session's
// encrypted initialization.
func reportMachineInitializationDelivery(ctx context.Context, diagnostics *machineClientDiagnostics, sessionID, reason string) {
	if diagnostics != nil {
		diagnostics.record(ctx, diagnosticStageInitializationDelivery, reason, sessionID)
	}
}

// reportMachineClaimPickup records a failure of the machine's own claim poll.
func reportMachineClaimPickup(ctx context.Context, diagnostics *machineClientDiagnostics, reason string) {
	if diagnostics != nil {
		diagnostics.record(ctx, diagnosticStageClaimPickup, reason, "")
	}
}

// reportMachineClaimExchange records a failure to exchange a claim this machine
// was actually offered.
func reportMachineClaimExchange(ctx context.Context, diagnostics *machineClientDiagnostics, sessionID, reason string) {
	if diagnostics != nil {
		diagnostics.record(ctx, diagnosticStageClaimExchange, reason, sessionID)
	}
}

// reportMachineAdapterLaunch records why this machine could not launch a claim
// it had already taken. Reporting is an observation; ending the Session is a
// separate, explicit step the caller takes with the same bounded reason.
func reportMachineAdapterLaunch(ctx context.Context, diagnostics *machineClientDiagnostics, sessionID, reason string) {
	if diagnostics != nil {
		diagnostics.record(ctx, diagnosticStageAdapterLaunch, reason, sessionID)
	}
}
