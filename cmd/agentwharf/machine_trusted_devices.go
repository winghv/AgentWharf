package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

// reportTrustedDevices publishes the machine's current enrolled-terminal list to
// the platform so the owner can see it and later request revocation. The
// platform stores a mirror; the machine remains the authority.
func reportTrustedDevices(ctx context.Context, client *http.Client, credential machineCredential, registry *e2ee.DeviceRegistry) error {
	if registry == nil {
		return errors.New("device registry unavailable")
	}
	records, err := registry.ListDetailed(ctx)
	if err != nil {
		return err
	}
	type devicePayload struct {
		DeviceID string `json:"device_id"`
		Trusted  bool   `json:"trusted"`
	}
	devices := make([]devicePayload, 0, len(records))
	for _, record := range records {
		devices = append(devices, devicePayload{DeviceID: record.Identity.Device, Trusted: record.Trusted})
	}
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/trusted-terminals/endpoint/devices")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	status, _, err := postCloudAPIJSON(ctx, client, endpoint, credential.MachineToken, map[string]any{"devices": devices})
	if err != nil || status != http.StatusNoContent {
		return errors.New("trusted device report rejected")
	}
	return nil
}

// reportTrustedDevicesWithRuntime opens the endpoint runtime for callers that do
// not already hold one, such as daemon startup.
func reportTrustedDevicesWithRuntime(ctx context.Context, client *http.Client, credential machineCredential) error {
	account := machineLocalAccountBinding(credential)
	if account == "" {
		return errors.New("local key binding unavailable")
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
	return reportTrustedDevices(ctx, client, credential, runtime.registry)
}
