package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

func TestReportTrustedDevicesPublishesEnrollmentSet(t *testing.T) {
	ctx := context.Background()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()

	device, err := e2ee.NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public, err := device.Public()
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.registry.EnrollTrusted(ctx, public); err != nil {
		t.Fatal(err)
	}

	var path, authorization string
	var reported struct {
		Devices []struct {
			DeviceID string `json:"device_id"`
			Trusted  bool   `json:"trusted"`
		} `json:"devices"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, authorization = r.URL.Path, r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&reported)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	credential := machineCredential{MachineID: "machine", MachineToken: "machine-token", CloudAPIURL: server.URL}
	if err := reportTrustedDevices(ctx, server.Client(), credential, runtime.registry); err != nil {
		t.Fatal(err)
	}
	if path != "/machines/machine/trusted-terminals/endpoint/devices" {
		t.Fatalf("path = %q", path)
	}
	if authorization != "Bearer machine-token" {
		t.Fatalf("authorization = %q", authorization)
	}
	if len(reported.Devices) != 1 || reported.Devices[0].DeviceID != device.Device || !reported.Devices[0].Trusted {
		t.Fatalf("devices = %+v", reported.Devices)
	}
}
