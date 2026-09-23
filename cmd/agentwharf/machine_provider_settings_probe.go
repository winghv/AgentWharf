package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type machineProviderSettingsProbe struct {
	ID       string `json:"id"`
	Provider string `json:"provider"`
}

func pollMachineProviderSettingsProbe(ctx context.Context, client *http.Client, credential machineCredential, stderr interface{ Write([]byte) (int, error) }) error {
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/provider-settings-probes/pending")
	if err != nil {
		return err
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("provider settings probe poll returned status %d", status)
	}
	var response struct {
		Data []machineProviderSettingsProbe `json:"data"`
	}
	if err := decodeCloudAPIJSON(body, &response); err != nil {
		return err
	}
	for _, probe := range response.Data {
		if probe.ID == "" || !machineSupportsSettingsProbe(probe.Provider) {
			continue
		}
		var output bytes.Buffer
		probeCtx, cancel := context.WithTimeout(ctx, providerSettingsProbeTimeout+5*time.Second)
		probeErr := runProviderSettingsProbe(probeCtx, []string{"--provider", probe.Provider}, &output)
		cancel()
		payload := map[string]any{"failed": probeErr != nil}
		if probeErr == nil {
			var capability json.RawMessage
			if err := json.Unmarshal(output.Bytes(), &capability); err != nil {
				payload["failed"] = true
			} else {
				payload["capability"] = capability
			}
		}
		complete, err := cloudAPIEndpoint(credential.CloudAPIURL, "/provider-settings-probes/"+url.PathEscape(probe.ID)+"/complete")
		if err != nil {
			return err
		}
		status, _, err := postCloudAPIJSON(ctx, client, complete, credential.MachineToken, payload)
		if err != nil {
			return err
		}
		if status != http.StatusNoContent {
			return fmt.Errorf("provider settings probe completion returned status %d", status)
		}
	}
	return nil
}

func machineSupportsSettingsProbe(provider string) bool {
	switch provider {
	case "pi", "deepseek-harness", "codex", "claude-code":
		return true
	default:
		return false
	}
}
