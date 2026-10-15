package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func pollSessionInitializations(ctx context.Context, client *http.Client, credential machineCredential, trusted bool, diagnostics *machineClientDiagnostics) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	account := machineLocalAccountBinding(credential)
	if account == "" {
		reportMachineInitializationDiscovery(ctx, diagnostics, "local_account_binding_unavailable")
		return errors.New("local initialization binding unavailable")
	}
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/e2ee-sessions/pending")
	if err != nil {
		reportMachineInitializationDiscovery(ctx, diagnostics, "discovery_unavailable")
		return errors.New("initialization discovery unavailable")
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil || status != http.StatusOK {
		reportMachineInitializationDiscovery(ctx, diagnostics, "discovery_unavailable")
		return errors.New("initialization discovery unavailable")
	}
	var response struct {
		Data []struct {
			Session string `json:"session_id"`
			Device  string `json:"device_id"`
		}
	}
	if decodeCloudAPIJSON(body, &response) != nil || response.Data == nil || len(response.Data) > 32 {
		reportMachineInitializationDiscovery(ctx, diagnostics, "discovery_response_invalid")
		return errors.New("invalid initialization discovery")
	}
	if len(response.Data) == 0 {
		return nil
	}
	// The two failures below are the ones that used to be invisible: the machine
	// is online and polling, the terminal is waiting for an answer, and nothing
	// anywhere said why no answer came.
	directory, err := machineEndpointDirectory(credential, account)
	if err != nil {
		reportMachineInitializationDiscovery(ctx, diagnostics, "endpoint_directory_unavailable")
		return errors.New("initialization storage unavailable")
	}
	runtime, err := openMachineE2EERuntime(ctx, directory, credential.MachineID, account)
	if err != nil {
		reportMachineInitializationDiscovery(ctx, diagnostics, "runtime_unavailable")
		return errors.New("initialization runtime unavailable")
	}
	defer runtime.database.Close()
	var failure error
	for _, item := range response.Data {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := deliverSessionInitialization(ctx, client, credential, runtime, item.Session, item.Device, trusted, diagnostics); err != nil {
			failure = errors.New("one or more initialization requests failed")
		}
	}
	return failure
}

// deliverSessionInitialization never treats machine bearer authentication as
// permission to initialize a session. The local runtime verifies paired trust.
func deliverSessionInitialization(ctx context.Context, client *http.Client, credential machineCredential, runtime *machineE2EERuntime, session, device string, trusted bool, diagnostics *machineClientDiagnostics) error {
	// Every exit below names the class it failed at, so the terminal's "machine
	// did not accept the setup" message can be backed by a platform-side reason.
	report := func(reason string) { reportMachineInitializationDelivery(ctx, diagnostics, session, reason) }
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/e2ee-sessions/"+url.PathEscape(session)+"/"+url.PathEscape(device)+"/endpoint")
	if err != nil {
		report("relay_unavailable")
		return errors.New("initialization relay unavailable")
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil || status != http.StatusOK {
		report("request_unavailable")
		return errors.New("initialization request unavailable")
	}
	var response struct {
		Data struct {
			Request   string    `json:"request"`
			ExpiresAt time.Time `json:"expires_at"`
			State     string    `json:"state"`
		}
	}
	if decodeCloudAPIJSON(body, &response) != nil || !response.Data.ExpiresAt.After(time.Now()) || response.Data.ExpiresAt.After(time.Now().Add(5*time.Minute)) {
		report("response_invalid")
		return errors.New("invalid initialization response")
	}
	control := strings.TrimSpace(os.Getenv("AGENTWHARF_TRUST_ACCOUNT_TERMINALS_VIEW_ONLY")) != "1"
	completed := response.Data.State == "completed"
	// Completed rows need no new response; never re-seal after response loss.
	// Require the local session, rather than trusting completion from the relay.
	var wrapped e2ee.WrappedKey
	if request, legacyErr := protocol.DecodeSessionInitialization([]byte(response.Data.Request), credential.MachineID, session); legacyErr == nil && request.Device == device {
		ctx, expire := context.WithDeadline(ctx, response.Data.ExpiresAt)
		defer expire()
		if completed {
			if runtime == nil || runtime.registry == nil {
				return errors.New("initialization runtime unavailable")
			}
			signed, err := e2ee.DecodeSessionInitialization([]byte(response.Data.Request))
			if err != nil {
				return errors.New("invalid completed initialization")
			}
			if err := runtime.executor.VerifyInitializedSession(ctx, runtime.registry, signed); err != nil {
				report("completed_signature_rejected")
				return errors.New("completed initialization signature rejected")
			}
			return runtime.requireSession(ctx, session)
		}
		if response.Data.State != "pending" {
			report("state_invalid")
			return errors.New("invalid initialization state")
		}
		wrapped, err = runtime.initializeSession(ctx, session, []byte(response.Data.Request))
		if err != nil {
			return err
		}
	} else if v2, trustedErr := e2ee.DecodeTrustedSessionKeyRequest([]byte(response.Data.Request)); trustedErr == nil && v2.Machine == credential.MachineID && v2.Session == session && v2.Device == device {
		// A terminal without a local machine offer creates the task through the v2
		// account-terminal-trust initialization.
		ctx, expire := context.WithDeadline(ctx, response.Data.ExpiresAt)
		defer expire()
		if completed {
			if runtime == nil || runtime.registry == nil {
				return errors.New("initialization runtime unavailable")
			}
			if err := runtime.executor.VerifyInitializedSessionTrusted(ctx, runtime.registry, v2); err != nil {
				report("completed_signature_rejected")
				return errors.New("completed initialization signature rejected")
			}
			return runtime.requireSession(ctx, session)
		}
		if response.Data.State != "pending" {
			report("state_invalid")
			return errors.New("invalid initialization state")
		}
		wrapped, err = runtime.initializeSessionTrusted(ctx, session, []byte(response.Data.Request), trusted, control)
		if err != nil {
			return err
		}
	} else {
		report("routing_mismatch")
		return errors.New("initialization routing mismatch")
	}
	// The creating terminal also receives the endpoint-signed session directory
	// so it can verify commands another terminal later authors in this session.
	journal, err := e2ee.NewCommandJournal(ctx, runtime.database)
	if err != nil {
		report("membership_unavailable")
		return errors.New("initialization response membership unavailable")
	}
	directory, err := runtime.vault.SignSessionMembershipDirectory(ctx, journal, session)
	if err != nil {
		report("membership_unavailable")
		return errors.New("initialization response membership unavailable")
	}
	payload := struct {
		Request    string                    `json:"request"`
		WrappedKey wrappedKeyResponsePayload `json:"wrapped_key"`
	}{response.Data.Request, wrappedKeyResponsePayload{Enc: wrapped.Enc, Ciphertext: wrapped.Ciphertext, Machine: runtime.public, Membership: &directory}}
	for {
		status, _, err = postCloudAPIJSON(ctx, client, endpoint, credential.MachineToken, payload)
		if err == nil && status == http.StatusNoContent {
			return nil
		}
		if err == nil && status < 500 {
			report("response_rejected")
			return errors.New("initialization response rejected")
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			report("deadline_exceeded")
			return ctx.Err()
		case <-timer.C:
		}
	}
}
