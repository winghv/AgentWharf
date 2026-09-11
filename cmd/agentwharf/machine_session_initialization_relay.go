package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func pollSessionInitializations(ctx context.Context, client *http.Client, credential machineCredential) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	account := machineLocalAccountBinding(credential)
	if account == "" {
		return errors.New("local initialization binding unavailable")
	}
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/e2ee-sessions/pending")
	if err != nil {
		return errors.New("initialization discovery unavailable")
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil || status != http.StatusOK {
		return errors.New("initialization discovery unavailable")
	}
	var response struct {
		Data []struct {
			Session string `json:"session_id"`
			Device  string `json:"device_id"`
		}
	}
	if decodeCloudAPIJSON(body, &response) != nil || response.Data == nil || len(response.Data) > 32 {
		return errors.New("invalid initialization discovery")
	}
	if len(response.Data) == 0 {
		return nil
	}
	directory, err := machineEndpointDirectory(credential, account)
	if err != nil {
		return errors.New("initialization storage unavailable")
	}
	runtime, err := openMachineE2EERuntime(ctx, directory, credential.MachineID, account)
	if err != nil {
		return errors.New("initialization runtime unavailable")
	}
	defer runtime.database.Close()
	var failure error
	for _, item := range response.Data {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := deliverSessionInitialization(ctx, client, credential, runtime, item.Session, item.Device); err != nil {
			failure = errors.New("one or more initialization requests failed")
		}
	}
	return failure
}

// deliverSessionInitialization never treats machine bearer authentication as
// permission to initialize a session. The local runtime verifies paired trust.
func deliverSessionInitialization(ctx context.Context, client *http.Client, credential machineCredential, runtime *machineE2EERuntime, session, device string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/e2ee-sessions/"+url.PathEscape(session)+"/"+url.PathEscape(device)+"/endpoint")
	if err != nil {
		return errors.New("initialization relay unavailable")
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil || status != http.StatusOK {
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
		return errors.New("invalid initialization response")
	}
	request, err := protocol.DecodeSessionInitialization([]byte(response.Data.Request), credential.MachineID, session)
	if err != nil || request.Device != device {
		return errors.New("initialization routing mismatch")
	}
	ctx, expire := context.WithDeadline(ctx, response.Data.ExpiresAt)
	defer expire()
	// Completed rows need no new response; never re-seal after response loss.
	// Require the local session, rather than trusting completion from the relay.
	if response.Data.State == "completed" {
		if runtime == nil || runtime.registry == nil {
			return errors.New("initialization runtime unavailable")
		}
		signed, err := e2ee.DecodeSessionInitialization([]byte(response.Data.Request))
		if err != nil {
			return errors.New("invalid completed initialization")
		}
		if err := runtime.executor.VerifyInitializedSession(ctx, runtime.registry, signed); err != nil {
			return errors.New("completed initialization signature rejected")
		}
		return runtime.requireSession(ctx, session)
	}
	if response.Data.State != "pending" {
		return errors.New("invalid initialization state")
	}
	wrapped, err := runtime.initializeSession(ctx, session, []byte(response.Data.Request))
	if err != nil {
		return err
	}
	// The creating terminal also receives the endpoint-signed session directory
	// so it can verify commands another terminal later authors in this session.
	journal, err := e2ee.NewCommandJournal(ctx, runtime.database)
	if err != nil {
		return errors.New("initialization response membership unavailable")
	}
	directory, err := runtime.vault.SignSessionMembershipDirectory(ctx, journal, session)
	if err != nil {
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
			return errors.New("initialization response rejected")
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
