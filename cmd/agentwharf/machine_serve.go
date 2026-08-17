package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

const machineServeUsage = "usage: wharf machine serve [--poll-interval SECONDS] [--max-concurrent N] [--startup-smoke]"

const (
	machineServeDefaultPollInterval   = 10 * time.Second
	machineServeMinPollInterval       = time.Second
	machineServeDefaultMaxConcurrent  = 2
	machineServeSenderRetryInitial    = 2 * time.Second
	machineServeSenderRetryMax        = 30 * time.Second
	machineServeSenderAttemptWindow   = 10 * time.Second
	machineServeAdapterRestartInitial = 2 * time.Second
	machineServeAdapterRestartMax     = 30 * time.Second
	machineServeAdapterRestartLimit   = 5
	machineCredentialRefreshLeadTime  = 12 * time.Hour
)

var errNoInteractiveState = errors.New("session never reached an interactive state")

type machineServeConfig struct {
	PollInterval  time.Duration
	MaxConcurrent int
	StartupSmoke  bool
}

// machineServeDispatch is the crash-resume handoff persisted locally after a
// claim exchange. It carries only this machine's own session credentials and
// the instruction; it is never logged and never leaves the machine.
type machineServeDispatch struct {
	ClaimID           string `json:"claim_id"`
	TaskID            string `json:"task_id"`
	RunID             string `json:"run_id"`
	SessionID         string `json:"session_id"`
	Provider          string `json:"provider"`
	HubWSURL          string `json:"hub_ws_url"`
	AdapterToken      string `json:"adapter_token"`
	ClientToken       string `json:"client_token"`
	FirstInstruction  string `json:"first_instruction"`
	WorkingDirectory  string `json:"working_directory"`
	ModelID           string `json:"model_id"`
	ReasoningEffortID string `json:"reasoning_effort_id"`
	PermissionModeID  string `json:"permission_mode_id"`
	AdapterExpiresAt  string `json:"adapter_expires_at"`
	ClientExpiresAt   string `json:"client_expires_at"`
}

type machinePendingClaim struct {
	ClaimID   string `json:"claim_id"`
	TaskID    string `json:"task_id"`
	RunID     string `json:"run_id"`
	SessionID string `json:"session_id"`
	Provider  string `json:"provider"`
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at"`
}

type machinePendingClaimsResponse struct {
	Data []machinePendingClaim `json:"data"`
}

type machineAutoExchangeResponse struct {
	Data struct {
		SessionID         string `json:"session_id"`
		Provider          string `json:"provider"`
		HubWSURL          string `json:"hub_ws_url"`
		AdapterToken      string `json:"adapter_token"`
		ClientToken       string `json:"client_token"`
		FirstInstruction  string `json:"first_instruction"`
		WorkingDirectory  string `json:"working_directory"`
		ModelID           string `json:"model_id"`
		ReasoningEffortID string `json:"reasoning_effort_id"`
		PermissionModeID  string `json:"permission_mode_id"`
		Delivery          string `json:"delivery"`
		AdapterExpiresAt  string `json:"adapter_expires_at"`
		ClientExpiresAt   string `json:"client_expires_at"`
	} `json:"data"`
}

type machineTokenRefreshEnvelope struct {
	Data struct {
		Machine struct {
			ID string `json:"id"`
		} `json:"machine"`
		MachineToken string `json:"machine_token"`
		ExpiresAt    string `json:"expires_at"`
	} `json:"data"`
}

func runMachineServeCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cfg, err := parseMachineServeConfig(args, stderr)
	if err != nil {
		return err
	}
	return runMachineServe(ctx, cfg, stdout, stderr)
}

func parseMachineServeConfig(args []string, stderr io.Writer) (machineServeConfig, error) {
	cfg := machineServeConfig{
		PollInterval:  machineServeDefaultPollInterval,
		MaxConcurrent: machineServeDefaultMaxConcurrent,
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--poll-interval":
			i++
			if i >= len(args) {
				return machineServeConfig{}, errors.New(machineServeUsage)
			}
			seconds, err := strconv.Atoi(args[i])
			if err != nil || seconds <= 0 {
				return machineServeConfig{}, errors.New(machineServeUsage)
			}
			cfg.PollInterval = time.Duration(seconds) * time.Second
		case "--max-concurrent":
			i++
			if i >= len(args) {
				return machineServeConfig{}, errors.New(machineServeUsage)
			}
			concurrent, err := strconv.Atoi(args[i])
			if err != nil || concurrent < 1 {
				return machineServeConfig{}, errors.New(machineServeUsage)
			}
			cfg.MaxConcurrent = concurrent
		case "--startup-smoke":
			cfg.StartupSmoke = true
		case "-h", "--help":
			_, _ = fmt.Fprintln(stderr, machineServeUsage)
			return machineServeConfig{}, errors.New("help requested")
		default:
			return machineServeConfig{}, errors.New(machineServeUsage)
		}
	}
	if cfg.PollInterval < machineServeMinPollInterval {
		return machineServeConfig{}, errors.New("--poll-interval must be at least 5 seconds")
	}
	return cfg, nil
}

func runMachineServe(ctx context.Context, cfg machineServeConfig, stdout, stderr io.Writer) error {
	credential, err := loadMachineCredential()
	if err != nil {
		return fmt.Errorf("load machine credential: %w", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if refreshed, err := maybeRefreshMachineCredential(ctx, client, credential, machineCredentialRefreshLeadTime); err != nil {
		return err
	} else if refreshed {
		credential, err = loadMachineCredential()
		if err != nil {
			return fmt.Errorf("reload machine credential: %w", err)
		}
	}

	sem := make(chan struct{}, cfg.MaxConcurrent)
	var workers sync.WaitGroup
	var adapters sync.WaitGroup
	serveCtx, cancel := context.WithCancel(ctx)
	// Shutdown order: let the dispatch goroutines finish first (they deliver the
	// first instruction), then cancel to stop the keep-alive goroutines, then wait
	// for those keep-alive goroutines to exit.
	defer adapters.Wait()
	defer cancel()
	defer workers.Wait()

	// Crash-resume: re-run each persisted handoff with the stored credentials
	// instead of re-exchanging. The deterministic command ID makes the resend
	// safe; the Hub acknowledges a duplicate without a second delivery.
	handoffs, err := loadMachineDispatches()
	if err != nil {
		return err
	}
	for _, handoff := range handoffs {
		if !dispatchCredentialAlive(handoff) {
			_, _ = fmt.Fprintf(stderr, "wharf machine serve: dropping expired handoff for claim %s\n", handoff.ClaimID)
			_ = removeMachineDispatch(handoff.ClaimID)
			continue
		}
		workers.Add(1)
		go func(handoff machineServeDispatch) {
			defer workers.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			dispatchOutcome(serveCtx, cfg, &handoff, stdout, stderr, &adapters)
		}(handoff)
	}

	poll := time.NewTicker(cfg.PollInterval)
	defer poll.Stop()
	for {
		claims, retry, err := listPendingMachineClaims(ctx, client, credential)
		if err != nil {
			if retry {
				refreshed, refreshErr := maybeRefreshMachineCredential(ctx, client, credential, 0)
				if refreshErr != nil {
					return refreshErr
				}
				if refreshed {
					credential, err = loadMachineCredential()
					if err != nil {
						return fmt.Errorf("reload machine credential: %w", err)
					}
					claims, _, err = listPendingMachineClaims(ctx, client, credential)
				}
			}
			if err != nil {
				_, _ = fmt.Fprintln(stderr, machineServePollRetryMessage(err, cfg.PollInterval))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-poll.C:
				}
				continue
			}
		}
		if refreshed, refreshErr := maybeRefreshMachineCredential(ctx, client, credential, machineCredentialRefreshLeadTime); refreshErr != nil {
			return refreshErr
		} else if refreshed {
			credential, err = loadMachineCredential()
			if err != nil {
				return fmt.Errorf("reload machine credential: %w", err)
			}
		}
		for _, claim := range claims {
			if claim.ClaimID == "" || !claimActiveNow(claim) {
				continue
			}
			workers.Add(1)
			go func(claim machinePendingClaim) {
				defer workers.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				dispatchClaim(serveCtx, cfg, client, credential, claim, stdout, stderr, &adapters)
			}(claim)
		}
		if cfg.StartupSmoke {
			// One dispatch is enough for the smoke; the caller times the run.
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		}
	}
}

// isCloudTransportError reports a poll failure caused by the network rather
// than an application-level (auth/status/decode) rejection.
func isCloudTransportError(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "get cloud api request:") ||
		strings.Contains(text, "read cloud api response:") ||
		strings.Contains(text, "close cloud api response:")
}

// machineServePollRetryMessage renders a non-fatal poll failure with the retry
// interval, so a transient network error (e.g. unexpected EOF) reads as
// "cloud unreachable, will retry" rather than a daemon crash.
func machineServePollRetryMessage(err error, interval time.Duration) string {
	if isCloudTransportError(err) {
		return fmt.Sprintf("wharf machine serve: cloud unreachable (%s); retrying in %s", err, interval)
	}
	return fmt.Sprintf("wharf machine serve: %s; retrying in %s", err, interval)
}

// dispatchClaim exchanges one pending auto claim and runs the dispatch.
func dispatchClaim(ctx context.Context, cfg machineServeConfig, client *http.Client, credential machineCredential, claim machinePendingClaim, stdout, stderr io.Writer, adapters *sync.WaitGroup) {
	handoff, err := exchangeAutoMachineClaim(ctx, client, credential, claim)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "wharf machine serve: exchange claim %s: %v\n", claim.ClaimID, err)
		return
	}
	if err := saveMachineDispatch(*handoff); err != nil {
		_, _ = fmt.Fprintf(stderr, "wharf machine serve: persist dispatch %s: %v\n", claim.ClaimID, err)
		return
	}
	dispatchOutcome(ctx, cfg, handoff, stdout, stderr, adapters)
}

func dispatchOutcome(ctx context.Context, cfg machineServeConfig, handoff *machineServeDispatch, stdout, stderr io.Writer, adapters *sync.WaitGroup) {
	// Start the adapter keep-alive first so the Session can reach an interactive
	// state; the first-instruction sender retries until it does. The keep-alive
	// runs in the background so the semaphore only bounds the dispatch phase, not
	// the session lifetime.
	adapters.Add(1)
	go func() {
		defer adapters.Done()
		keepAdapterAlive(ctx, cfg, handoff, stdout, stderr)
	}()

	sendErr := sendFirstInstructionWithRetry(ctx, *handoff)
	switch {
	case sendErr == nil:
		_, _ = fmt.Fprintf(stdout, "auto_dispatch_ok: claim_id=%s session_id=%s\n", handoff.ClaimID, handoff.SessionID)
		if cfg.StartupSmoke {
			// Smoke is one-shot: the dispatch must not survive a daemon restart.
			_ = removeMachineDispatch(handoff.ClaimID)
		}
	case errors.Is(sendErr, context.Canceled):
		return
	default:
		_, _ = fmt.Fprintf(stderr, "wharf machine serve: dispatch %s: %v\n", handoff.ClaimID, sendErr)
		return
	}
}

// keepAdapterAlive re-runs the adapter until the daemon stops, the adapter
// credential expires, or a bounded number of restarts is exhausted. A crash
// (for example the machine slept and the provider or its Hub socket died) is
// restarted with backoff; the first instruction is not re-sent.
func keepAdapterAlive(ctx context.Context, cfg machineServeConfig, handoff *machineServeDispatch, stdout, stderr io.Writer) {
	adapterCfg := serveWrapConfig(*handoff, cfg.StartupSmoke)
	restarts := 0
	delay := machineServeAdapterRestartInitial
	for {
		done := runAdapter(ctx, adapterCfg, stderr)
		select {
		case <-ctx.Done():
			return
		case err := <-done:
			if ctx.Err() != nil {
				return
			}
			if cfg.StartupSmoke {
				_, _ = fmt.Fprintf(stderr, "wharf machine serve: adapter for %s ended: %v\n", handoff.SessionID, err)
				return
			}
			credentialAlive := dispatchCredentialAlive(*handoff)
			if !machineServeAdapterShouldRestart(err, credentialAlive, restarts) {
				_ = removeMachineDispatch(handoff.ClaimID)
				if err == nil {
					_, _ = fmt.Fprintf(stderr, "wharf machine serve: adapter for %s ended normally; not restarting\n", handoff.SessionID)
					return
				}
				_, _ = fmt.Fprintf(stderr, "wharf machine serve: adapter for %s ended (%v); giving up (restarts=%d)\n", handoff.SessionID, err, restarts)
				return
			}
			restarts++
			_, _ = fmt.Fprintf(stderr, "wharf machine serve: adapter for %s ended (%v); restarting in %s\n", handoff.SessionID, err, delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			if delay < machineServeAdapterRestartMax {
				delay *= 2
			}
		}
	}
}

func machineServeAdapterShouldRestart(err error, credentialAlive bool, restarts int) bool {
	return err != nil && credentialAlive && restarts < machineServeAdapterRestartLimit
}

func runAdapter(ctx context.Context, cfg wrapConfig, stderr io.Writer) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := runWrap(ctx, cfg, strings.NewReader(""), stderr)
		done <- err
	}()
	return done
}

// exchangeAutoMachineClaim consumes the claim with the machine bearer alone and
// returns the session-scoped credentials plus the first instruction.
func exchangeAutoMachineClaim(ctx context.Context, client *http.Client, credential machineCredential, claim machinePendingClaim) (*machineServeDispatch, error) {
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machine-task-claims/"+url.PathEscape(claim.ClaimID)+"/exchange")
	if err != nil {
		return nil, errors.New("claim unavailable")
	}
	status, body, err := postCloudAPIJSON(ctx, client, endpoint, credential.MachineToken, struct{}{})
	if err != nil || status < http.StatusOK || status >= http.StatusMultipleChoices {
		return nil, errors.New("claim unavailable")
	}
	var response machineAutoExchangeResponse
	if err := decodeCloudAPIJSON(body, &response); err != nil {
		return nil, errors.New("claim unavailable")
	}
	data := response.Data
	if data.SessionID == "" || data.HubWSURL == "" || data.AdapterToken == "" || data.ClientToken == "" ||
		data.FirstInstruction == "" || data.Delivery != "auto" || data.AdapterExpiresAt == "" || data.ClientExpiresAt == "" {
		return nil, errors.New("claim exchange response is incomplete")
	}
	adapterExpiresAt := data.AdapterExpiresAt
	clientExpiresAt := data.ClientExpiresAt
	return &machineServeDispatch{
		ClaimID:           claim.ClaimID,
		TaskID:            claim.TaskID,
		RunID:             claim.RunID,
		SessionID:         data.SessionID,
		Provider:          data.Provider,
		HubWSURL:          data.HubWSURL,
		AdapterToken:      data.AdapterToken,
		ClientToken:       data.ClientToken,
		FirstInstruction:  data.FirstInstruction,
		WorkingDirectory:  data.WorkingDirectory,
		ModelID:           data.ModelID,
		ReasoningEffortID: data.ReasoningEffortID,
		PermissionModeID:  data.PermissionModeID,
		AdapterExpiresAt:  adapterExpiresAt,
		ClientExpiresAt:   clientExpiresAt,
	}, nil
}

// listPendingMachineClaims polls the platform for this machine's active
// auto-delivery claims. A non-2xx response that indicates an invalid machine
// credential returns retry=true so the caller can refresh once.
func listPendingMachineClaims(ctx context.Context, client *http.Client, credential machineCredential) ([]machinePendingClaim, bool, error) {
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machine-task-claims/pending")
	if err != nil {
		return nil, false, err
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusUnauthorized {
		return nil, true, errors.New("machine credential rejected; refreshing once")
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return nil, false, newCloudStatusError("list machine task claims", status, body)
	}
	var response machinePendingClaimsResponse
	if err := decodeCloudAPIJSON(body, &response); err != nil {
		return nil, false, fmt.Errorf("decode pending claims: %w", err)
	}
	return response.Data, false, nil
}

// maybeRefreshMachineCredential refreshes the machine bearer when it is within
// the given lead time of expiry. It writes the refreshed credential back to the
// machine credential store.
func maybeRefreshMachineCredential(ctx context.Context, client *http.Client, credential machineCredential, leadTime time.Duration) (bool, error) {
	expiresAt, err := time.Parse(time.RFC3339, credential.ExpiresAt)
	if err == nil && leadTime > 0 && time.Until(expiresAt) > leadTime {
		return false, nil
	}
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machine-token/refresh")
	if err != nil {
		return false, err
	}
	status, body, err := postCloudAPIJSON(ctx, client, endpoint, credential.MachineToken, struct{}{})
	if err != nil {
		return false, err
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		// The 24-hour bearer expired while this machine was offline. Recover a
		// fresh one from the pairing-time refresh secret instead of forcing a
		// re-pair. recoverMachineCredential persists the new bearer.
		if _, err := recoverMachineCredential(ctx, client, credential); err != nil {
			return false, err
		}
		return true, nil
	}
	if status != http.StatusOK {
		return false, newCloudStatusError("refresh machine token", status, body)
	}
	var envelope machineTokenRefreshEnvelope
	if err := decodeCloudAPIJSON(body, &envelope); err != nil {
		return false, fmt.Errorf("decode refreshed machine token: %w", err)
	}
	if envelope.Data.MachineToken == "" || envelope.Data.ExpiresAt == "" || envelope.Data.Machine.ID != credential.MachineID {
		return false, errors.New("refresh machine token response is incomplete")
	}
	credential.MachineToken = envelope.Data.MachineToken
	credential.ExpiresAt = envelope.Data.ExpiresAt
	if err := saveMachineCredential(credential); err != nil {
		return false, fmt.Errorf("persist refreshed machine credential: %w", err)
	}
	return true, nil
}

// recoverMachineCredential exchanges the long-lived refresh secret for a fresh
// machine bearer. It is the offline-recovery path: a machine whose 24-hour
// bearer expired while it was offline can come back online without re-pairing,
// because the refresh secret is the pairing-time trust anchor and stays valid
// until the machine is released.
func recoverMachineCredential(ctx context.Context, client *http.Client, credential machineCredential) (machineCredential, error) {
	if strings.TrimSpace(credential.RefreshSecret) == "" {
		return credential, errors.New("machine refresh secret is unavailable")
	}
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machine-token/recover")
	if err != nil {
		return credential, err
	}
	status, body, err := postCloudAPIJSON(ctx, client, endpoint, "", machineTokenRecoverRequest{
		RefreshSecret: credential.RefreshSecret,
	})
	if err != nil {
		return credential, err
	}
	if status != http.StatusOK {
		return credential, newCloudStatusError("recover machine token", status, body)
	}
	var envelope machineTokenRefreshEnvelope
	if err := decodeCloudAPIJSON(body, &envelope); err != nil {
		return credential, fmt.Errorf("decode recovered machine token: %w", err)
	}
	if envelope.Data.MachineToken == "" || envelope.Data.ExpiresAt == "" || envelope.Data.Machine.ID != credential.MachineID {
		return credential, errors.New("recovered machine token response is incomplete")
	}
	credential.MachineToken = envelope.Data.MachineToken
	credential.ExpiresAt = envelope.Data.ExpiresAt
	if err := saveMachineCredential(credential); err != nil {
		return credential, fmt.Errorf("persist recovered machine credential: %w", err)
	}
	return credential, nil
}

type machineTokenRecoverRequest struct {
	RefreshSecret string `json:"refresh_secret"`
}

// serveWrapConfig builds the adapter configuration from the exchange handoff,
// mirroring the manual `wharf task claim` launch path.
func serveWrapConfig(handoff machineServeDispatch, startupSmoke bool) wrapConfig {
	agent := handoff.Provider
	if agent == "claude-code" {
		agent = "claude"
	}
	return wrapConfig{
		HubURL:           handoff.HubWSURL,
		SessionID:        handoff.SessionID,
		Agent:            agent,
		Provider:         handoff.Provider,
		AdapterToken:     handoff.AdapterToken,
		Format:           "acp",
		ProviderCommand:  defaultProviderCommand(agent),
		ProtocolVersion:  protocol.HubProtocolVersion,
		StartupSmoke:     startupSmoke,
		WorkingDirectory: handoff.WorkingDirectory,
		LaunchSettings:   wrapLaunchSettings{ModelID: handoff.ModelID, ReasoningEffortID: handoff.ReasoningEffortID, PermissionModeID: handoff.PermissionModeID},
	}
}

// sendFirstInstructionWithRetry delivers the instruction as the Session's
// client, retrying with bounded backoff while the client credential is valid.
// Each attempt is windowed: while the Session is still `starting`, the Hub
// admits the client hello as attach-only without an event subscription, so a
// fresh hello after the Adapter turned the Session interactive is what finally
// receives the ready state (via replay). The command ID is deterministic per
// claim, so a resend after a lost acknowledgement is a duplicate, never a
// second delivery.
func sendFirstInstructionWithRetry(ctx context.Context, handoff machineServeDispatch) error {
	commandID := handoff.ClaimID + ":command"
	delay := machineServeSenderRetryInitial
	for {
		err := sendFirstInstruction(ctx, handoff, commandID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errNoInteractiveState) {
			return err
		}
		expiresAt, parseErr := time.Parse(time.RFC3339, handoff.ClientExpiresAt)
		if parseErr != nil || !time.Now().UTC().Before(expiresAt) {
			return fmt.Errorf("client credential expired: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay < machineServeSenderRetryMax {
			delay *= 2
		}
	}
}

// sendFirstInstruction opens one client connection, waits for the Session to
// reach an interactive state, sends the instruction, and waits for the
// acknowledgement. A single attempt is bounded by the attempt window so a
// still-`starting` Session (attach-only admission, no event subscription)
// yields to a fresh hello on the next attempt.
func sendFirstInstruction(ctx context.Context, handoff machineServeDispatch, commandID string) error {
	ctx, cancel := context.WithTimeout(ctx, machineServeSenderAttemptWindow)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, handoff.HubWSURL, nil)
	if err != nil {
		return fmt.Errorf("dial hub: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if err := writeCLIProtocolFrame(ctx, conn, &protocol.Hello{
		ProtocolVersion: protocol.HubProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           handoff.ClientToken,
		Subscriptions:   []protocol.Subscription{{SessionID: handoff.SessionID, LastSeq: 0}},
	}); err != nil {
		return fmt.Errorf("send client hello: %w", err)
	}
	for {
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errNoInteractiveState
			}
			return fmt.Errorf("read hub frame: %w", err)
		}
		switch typed := frame.(type) {
		case *protocol.HelloAck:
			for _, summary := range typed.Sessions {
				if summary.SessionID == handoff.SessionID && serveInteractiveState(summary.State) {
					return sendClientCommand(ctx, conn, handoff.SessionID, handoff.FirstInstruction, commandID)
				}
			}
		case *protocol.Event:
			if typed.SessionID == handoff.SessionID && typed.Type == "session.state" {
				var payload struct {
					State string `json:"state"`
				}
				_ = json.Unmarshal(typed.Payload, &payload)
				if serveInteractiveState(payload.State) {
					return sendClientCommand(ctx, conn, handoff.SessionID, handoff.FirstInstruction, commandID)
				}
			}
		case *protocol.Ping:
			_ = writeCLIProtocolFrame(ctx, conn, &protocol.Pong{Nonce: typed.Nonce})
		case *protocol.Error:
			return fmt.Errorf("hub error: %s: %s", typed.Code, typed.Message)
		}
	}
}

func sendClientCommand(ctx context.Context, conn *websocket.Conn, sessionID, instruction, commandID string) error {
	payload, err := json.Marshal(map[string]any{
		"content": []map[string]string{{"kind": "text", "text": instruction}},
	})
	if err != nil {
		return fmt.Errorf("encode instruction: %w", err)
	}
	if err := writeCLIProtocolFrame(ctx, conn, &protocol.Command{
		CommandID: commandID, Type: protocol.CommandSessionSend, SessionID: sessionID, Payload: payload,
	}); err != nil {
		return fmt.Errorf("send instruction: %w", err)
	}
	for {
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			return fmt.Errorf("read hub frame after send: %w", err)
		}
		switch typed := frame.(type) {
		case *protocol.CommandAck:
			if typed.CommandID != commandID {
				continue
			}
			if typed.Status == protocol.AckAccepted || typed.Status == protocol.AckDuplicate {
				return nil
			}
			return fmt.Errorf("instruction rejected: %s", typed.Reason)
		case *protocol.Ping:
			_ = writeCLIProtocolFrame(ctx, conn, &protocol.Pong{Nonce: typed.Nonce})
		case *protocol.Error:
			return fmt.Errorf("hub error after send: %s: %s", typed.Code, typed.Message)
		}
	}
}

func serveInteractiveState(state string) bool {
	switch state {
	case "ready", "busy", "waiting_permission", "recovering":
		return true
	default:
		return false
	}
}

func dispatchCredentialAlive(handoff machineServeDispatch) bool {
	expiresAt, err := time.Parse(time.RFC3339, handoff.AdapterExpiresAt)
	if err != nil {
		return false
	}
	return time.Now().UTC().Before(expiresAt)
}

// claimActiveNow reports whether a pending claim is still exchangeable by the
// server clock; the server remains authoritative at exchange time.
func claimActiveNow(claim machinePendingClaim) bool {
	expiresAt, err := time.Parse(time.RFC3339, claim.ExpiresAt)
	if err != nil {
		return false
	}
	return time.Now().UTC().Before(expiresAt)
}

func machineDispatchDir() (string, error) {
	credentialPath, err := machineCredentialFile()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(credentialPath), "dispatch"), nil
}

func saveMachineDispatch(dispatch machineServeDispatch) error {
	dir, err := machineDispatchDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create dispatch directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure dispatch directory: %w", err)
	}
	data, err := json.MarshalIndent(dispatch, "", "  ")
	if err != nil {
		return fmt.Errorf("encode dispatch: %w", err)
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(dir, ".dispatch.*")
	if err != nil {
		return fmt.Errorf("create temporary dispatch: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secure temporary dispatch: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary dispatch: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary dispatch: %w", err)
	}
	path := filepath.Join(dir, dispatch.ClaimID+".json")
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace dispatch: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure dispatch: %w", err)
	}
	return nil
}

func loadMachineDispatches() ([]machineServeDispatch, error) {
	dir, err := machineDispatchDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read dispatch directory: %w", err)
	}
	var dispatches []machineServeDispatch
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read dispatch %s: %w", entry.Name(), err)
		}
		var dispatch machineServeDispatch
		if err := json.Unmarshal(data, &dispatch); err != nil {
			return nil, fmt.Errorf("decode dispatch %s: %w", entry.Name(), err)
		}
		if dispatch.ClaimID == "" || dispatch.SessionID == "" || dispatch.AdapterToken == "" || dispatch.ClientToken == "" {
			return nil, fmt.Errorf("dispatch %s is incomplete", entry.Name())
		}
		dispatches = append(dispatches, dispatch)
	}
	return dispatches, nil
}

func removeMachineDispatch(claimID string) error {
	dir, err := machineDispatchDir()
	if err != nil {
		return err
	}
	path := filepath.Join(dir, claimID+".json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete dispatch: %w", err)
	}
	return nil
}
