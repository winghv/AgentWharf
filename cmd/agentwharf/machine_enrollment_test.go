package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

func TestMachineEnrollmentUsesPrivatePersistentIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	directory := filepath.Join(t.TempDir(), "endpoint")
	device, err := e2ee.NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public, err := device.Public()
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeTestJSON(w, 200, map[string]any{"data": map[string]string{"request": string(raw)}})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	var endpointIdentity e2ee.PairingIdentity
	err = enrollMachineEndpoint(ctx, server.Client(), machineCredential{MachineID: "machine", CloudAPIURL: server.URL, MachineToken: "test"}, directory, "local-account", func(offer e2ee.MachineOffer) error {
		if err := e2ee.VerifyMachineOffer(offer, time.Now()); err != nil {
			return err
		}
		endpointIdentity = offer.Identity
		request, err := e2ee.EncryptPairingIdentity(offer.Offer, public, ed25519.NewKeyFromSeed(device.SigningSeed), time.Now())
		if err != nil {
			return err
		}
		raw, err = json.Marshal(request)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := e2ee.LoadOrCreateIdentity(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	restoredPublic, err := restored.Public()
	if err != nil || restoredPublic != endpointIdentity {
		t.Fatal("identity changed on reopen")
	}
	database, err := e2ee.OpenLocalDatabase(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registry, err := e2ee.NewDeviceRegistry(ctx, database, "machine", "local-account")
	if err != nil {
		t.Fatal(err)
	}
	enrolled, err := registry.Device(ctx, device.Device)
	if err != nil || enrolled != public {
		t.Fatalf("durable enrollment: %v", err)
	}
}
