package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// A provider that cannot load its native session has to say so in a way the
// daemon can act on.
//
// The reopen moves an ended Session to 'recovering' and clears the Adapter's
// terminal fence, and from that point the machine's daemon is the only party
// that can end it: the platform's own lifecycle never touches a Session it
// believes is running, because the machine holds the provider.
//
// The daemon could not. When an adapter gave up, it logged the failure and
// returned without telling the platform. The Session therefore stayed in
// 'recovering' forever: the console showed a conversation that was
// "reconnecting" and would never answer, the Continue button reopened it into
// the same state, and nothing the user could do moved it.
//
// This is the production failure. The provider's own transcript had been
// deleted, so the provider answered `session/load` with "Unknown sessionId",
// the adapter could not start, and the Session never left 'recovering'.
func TestEstablishACPSessionReportsAFailedLoad(t *testing.T) {
	provider := startUnloadableProvider(t)

	_, _, err := establishACPSession(context.Background(), provider.scanner, provider.stdin, "/tmp", "acp_ses_existing")
	if err == nil {
		t.Fatal("establishACPSession() error = nil, want the provider's load failure")
	}
	// The failure is distinguishable from a provider that merely failed to
	// start: a missing native transcript is not repairable by reconnecting.
	if !errors.Is(err, errProviderSessionNotResumable) {
		t.Fatalf("establishACPSession() error = %v, want it to wrap errProviderSessionNotResumable", err)
	}
	// The provider's own message survives, so the reason says which failure it
	// was rather than a generic "could not start".
	if !strings.Contains(err.Error(), "Invalid params") {
		t.Fatalf("establishACPSession() error = %v, want the provider's own message to survive", err)
	}
}

// Every failure that ends an adapter has to end the Session on the platform
// too, and it has to be reported.
//
// The machine is the only party that can end a Session the platform considers
// live, because the machine holds the provider. The branch that gave up on an
// adapter used to log the failure and return without reporting it, so the
// Session it belonged to stayed in 'recovering' forever.
func TestAnAdapterFailureIsReportedToThePlatform(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			// The production failure: the provider's transcript was gone.
			name:       "the provider cannot load its transcript",
			err:        fmt.Errorf("%w: %w", errProviderSessionNotResumable, errors.New("Invalid params")),
			wantReason: "provider_session_resume_failed",
		},
		{
			name:       "the provider is not installed",
			err:        fmt.Errorf("%w: %w", errProviderCommandNotFound, errors.New("executable file not found")),
			wantReason: "provider_command_unavailable",
		},
		{
			// The adapter ran out of restarts. The Session is still the
			// machine's to end.
			name:       "the adapter gave up",
			err:        errors.New("transport failed"),
			wantReason: "provider_start_failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reported []string
			var reportedMu sync.Mutex
			reportFailure := machineSessionFailureReporter(func(_ context.Context, sessionID, reason string) error {
				reportedMu.Lock()
				defer reportedMu.Unlock()
				reported = append(reported, sessionID+"|"+reason)
				return nil
			})

			handoff := &machineServeDispatch{
				ClaimID:          "recovery:ses_failure",
				SessionID:        "ses_failure",
				Provider:         "claude-code",
				AdapterExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			}
			var logs bytes.Buffer
			// The restart budget is spent, so the loop reaches its give-up
			// branch.
			outcome, restart := classifyAdapterFailure(context.Background(), handoff, tc.err,
				machineServeAdapterRestartLimit, &logs, reportFailure, nil, false)
			if restart {
				t.Fatal("the adapter was restarted after its budget was spent")
			}
			_ = outcome

			reportedMu.Lock()
			defer reportedMu.Unlock()
			if len(reported) == 0 {
				t.Fatalf("the failure was never reported to the platform; log:\n%s", logs.String())
			}
			if reported[0] != "ses_failure|"+tc.wantReason {
				t.Fatalf("reported %q, want the session and %q", reported[0], tc.wantReason)
			}
		})
	}
}

// The reason a failure is reported with has to be one the platform accepts, or
// the report is rejected and the Session is left in the state it was in.
func TestMachineAdapterFailureReasonIsAlwaysAcceptedByThePlatform(t *testing.T) {
	accepted := map[string]bool{
		"provider_command_unavailable":     true,
		"provider_config_unavailable":      true,
		"provider_credentials_unavailable": true,
		"provider_start_failed":            true,
		"working_directory_unavailable":    true,
		"provider_session_resume_failed":   true,
	}
	if reason := machineAdapterFailureReason(errors.New("some provider failure")); !accepted[reason] {
		t.Fatalf("machineAdapterFailureReason() = %q, want a reason the platform accepts", reason)
	}
	if reason := machineAdapterFailureReason(fmt.Errorf("wrapped: %w", errProviderSessionNotResumable)); reason != "provider_session_resume_failed" {
		t.Fatalf("machineAdapterFailureReason() = %q, want provider_session_resume_failed", reason)
	}
}

// acpTestProvider is a provider process that speaks just enough of the ACP
// protocol for the session exchange.
type acpTestProvider struct {
	scanner *bufio.Scanner
	stdin   io.Writer
}

// startUnloadableProvider starts a provider that refuses to load its native
// session, exactly as pi-acp does when the transcript is gone.
func startUnloadableProvider(t *testing.T) *acpTestProvider {
	t.Helper()
	provider := exec.Command(os.Args[0], "-test.run=TestACPProviderHelperProcess")
	provider.Env = append(os.Environ(), "AGENTWHARF_ACP_RECOVERY_HELPER=load-only")
	stdin, err := provider.StdinPipe()
	if err != nil {
		t.Fatalf("provider stdin: %v", err)
	}
	stdout, err := provider.StdoutPipe()
	if err != nil {
		t.Fatalf("provider stdout: %v", err)
	}
	if err := provider.Start(); err != nil {
		t.Fatalf("start provider: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Process.Kill()
		_ = provider.Wait()
	})
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return &acpTestProvider{scanner: scanner, stdin: stdin}
}

// TestACPProviderHelperProcess is the provider side of the tests above. It is
// not a test; it is the child process that speaks ACP.
func TestACPProviderHelperProcess(t *testing.T) {
	if os.Getenv("AGENTWHARF_ACP_RECOVERY_HELPER") == "" {
		t.Skip("not the provider helper process")
	}
	runWrapACPRecoveryProviderHelper()
}
