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

func pollSessionKeyRequests(ctx context.Context, client *http.Client, credential machineCredential) error {
	account := strings.TrimSpace(os.Getenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING"))
	if account == "" {
		return errors.New("local key binding unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/e2ee-key-requests/pending")
	if err != nil {
		return err
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil || status != http.StatusOK {
		return errors.New("key request discovery unavailable")
	}
	var raw struct {
		Data []map[string]string `json:"data"`
	}
	if decodeCloudAPIJSON(body, &raw) != nil || len(raw.Data) > 32 {
		return errors.New("invalid key request discovery")
	}
	if len(raw.Data) == 0 {
		return nil
	}
	directory, err := machineEndpointDirectory(credential, account)
	if err != nil {
		return err
	}
	runtime, err := openMachineE2EERuntime(ctx, directory, credential.MachineID, account)
	if err != nil {
		return err
	}
	defer runtime.database.Close()
	var failure error
	for _, item := range raw.Data {
		session, device, keyID := item["session_id"], item["device_id"], item["key_id"]
		if session == "" || device == "" || keyID == "" {
			failure = errors.New("invalid key request identity")
			continue
		}
		if err := deliverSessionKeyRequest(ctx, client, credential, runtime, session, device, keyID); err != nil {
			failure = errors.New("one or more key requests failed")
		}
	}
	return failure
}

func deliverSessionKeyRequest(ctx context.Context, client *http.Client, credential machineCredential, runtime *machineE2EERuntime, session, device, keyID string) error {
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/e2ee-key-requests/"+url.PathEscape(session)+"/"+url.PathEscape(device)+"/"+url.PathEscape(keyID)+"/endpoint")
	if err != nil {
		return err
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil || status != http.StatusOK {
		return errors.New("key request unavailable")
	}
	var response struct {
		Data struct {
			Request   string    `json:"request"`
			State     string    `json:"state"`
			ExpiresAt time.Time `json:"expires_at"`
		} `json:"data"`
	}
	now := time.Now()
	if decodeCloudAPIJSON(body, &response) != nil || response.Data.Request == "" || !response.Data.ExpiresAt.After(now) || response.Data.ExpiresAt.After(now.Add(5*time.Minute)) {
		return errors.New("invalid key request")
	}
	signed, err := e2ee.DecodeSessionKeyRequest([]byte(response.Data.Request))
	if err != nil || signed.Machine != credential.MachineID || signed.Session != session || signed.Device != device || signed.KeyID != keyID {
		return errors.New("key request routing mismatch")
	}
	if response.Data.State == "completed" {
		return verifyCompletedSessionKeyRequest(ctx, runtime, signed)
	}
	if response.Data.State != "pending" {
		return errors.New("invalid key request state")
	}
	wrapped, err := runtime.executor.RecoverSessionKey(ctx, runtime.registry, e2ee.SessionKeyRequest(signed))
	if err != nil {
		return err
	}
	payload := struct {
		Request    string          `json:"request"`
		WrappedKey e2ee.WrappedKey `json:"wrapped_key"`
	}{response.Data.Request, wrapped}
	ctx, expire := context.WithDeadline(ctx, response.Data.ExpiresAt)
	defer expire()
	for {
		status, _, err = postCloudAPIJSON(ctx, client, endpoint, credential.MachineToken, payload)
		if err == nil && status == http.StatusNoContent {
			return nil
		}
		if err == nil && status < http.StatusInternalServerError {
			return errors.New("key response rejected")
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

func verifyCompletedSessionKeyRequest(ctx context.Context, runtime *machineE2EERuntime, request e2ee.SessionKeyRequest) error {
	if runtime == nil || runtime.executor == nil || runtime.registry == nil {
		return errors.New("key request runtime unavailable")
	}
	if _, err := runtime.executor.RecoverSessionKey(ctx, runtime.registry, request); err != nil {
		return errors.New("completed key request signature rejected")
	}
	return nil
}

var _ = protocol.ErrSessionKeyRequest
