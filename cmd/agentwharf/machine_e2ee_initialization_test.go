package main

import (
	"bytes"
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

func TestMachineRuntimeSignedInitializationIngress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	directory := filepath.Join(t.TempDir(), "endpoint")
	runtime, err := openMachineE2EERuntime(ctx, directory, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.database.Close() }()
	client, err := e2ee.NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public, err := client.Public()
	if err != nil {
		t.Fatal(err)
	}
	invitation, offer, err := e2ee.NewPairingInvitation("machine", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pairing, err := e2ee.EncryptPairingIdentity(offer, public, ed25519.NewKeyFromSeed(client.SigningSeed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.registry.Enroll(ctx, invitation, pairing, time.Now()); err != nil {
		t.Fatal(err)
	}
	signed, err := e2ee.SignSessionInitialization(e2ee.SessionInitialization{Machine: "machine", Account: "account", Session: "session", KeyID: "key", Device: client.Device}, ed25519.NewKeyFromSeed(client.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.initializeSession(ctx, "other", wire); err == nil {
		t.Fatal("outer route substitution accepted")
	}
	if err := runtime.requireSession(ctx, "session"); err == nil {
		t.Fatal("rejected request created session")
	}
	var firstResponse []byte
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Error("missing bearer")
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/machines/machine/e2ee-sessions/pending" {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]string{{"session_id": "session", "device_id": client.Device}}})
			return
		}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"request": string(wire), "expires_at": time.Now().Add(time.Minute), "state": "pending"}})
			return
		}
		var body struct {
			Request    string          `json:"request"`
			WrappedKey json.RawMessage `json:"wrapped_key"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Request != string(wire) {
			t.Error("request changed")
			w.WriteHeader(400)
			return
		}
		if err := runtime.requireSession(ctx, "session"); err != nil {
			t.Error("response before local commit", err)
		}
		posts++
		if posts == 1 {
			firstResponse = append([]byte(nil), body.WrappedKey...)
			w.WriteHeader(503)
			return
		}
		if !bytes.Equal(firstResponse, body.WrappedKey) {
			t.Error("response was rewrapped on retry")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	if err := deliverSessionInitialization(ctx, server.Client(), machineCredential{CloudAPIURL: server.URL, MachineID: "machine", MachineToken: "test"}, runtime, "session", client.Device, false); err != nil {
		t.Fatal(err)
	}
	if posts != 2 {
		t.Fatal("missing fixed-response retry")
	}
	// Discover against a separate address without paired trust: no response
	// upload is allowed even though the machine bearer is accepted by HTTP.
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "account")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(t.TempDir(), "machine.json"))
	if err := pollSessionInitializations(ctx, server.Client(), machineCredential{CloudAPIURL: server.URL, MachineID: "machine", MachineToken: "test"}, false); err == nil {
		t.Fatal("unpaired discovery initialized session")
	}
	if posts != 2 {
		t.Fatal("unpaired discovery uploaded key")
	}
	credential := machineCredential{CloudAPIURL: server.URL, MachineID: "machine", MachineToken: "test"}
	pollDirectory, err := machineEndpointDirectory(credential, "account")
	if err != nil {
		t.Fatal(err)
	}
	pollRuntime, err := openMachineE2EERuntime(ctx, pollDirectory, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	invitation, offer, err = e2ee.NewPairingInvitation("machine", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pairing, err = e2ee.EncryptPairingIdentity(offer, public, ed25519.NewKeyFromSeed(client.SigningSeed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pollRuntime.registry.Enroll(ctx, invitation, pairing, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := pollRuntime.database.Close(); err != nil {
		t.Fatal(err)
	}
	posts = 0
	if err := pollSessionInitializations(ctx, server.Client(), credential, true); err != nil {
		t.Fatal(err)
	}
	if posts != 2 {
		t.Fatal("discovered request was not delivered")
	}

	if err := runtime.requireSession(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.database.Close(); err != nil {
		t.Fatal(err)
	}
	runtime, err = openMachineE2EERuntime(ctx, directory, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.initializeSession(ctx, "session", wire); err != nil {
		t.Fatalf("restart recovery failed: %v", err)
	}
	for _, scenario := range []string{"valid", "forged", "wrong_key", "revoked"} {
		request := signed
		if scenario == "wrong_key" {
			request.KeyID = "other-key"
			request, err = e2ee.SignSessionInitialization(request, ed25519.NewKeyFromSeed(client.SigningSeed))
			if err != nil {
				t.Fatal(err)
			}
		}
		if scenario == "revoked" {
			if err := runtime.executor.ReplaceGrants(ctx, "session", "rotated-key", 1, []e2ee.DeviceGrant{{DeviceID: client.Device, VerifyKey: ed25519.NewKeyFromSeed(client.SigningSeed).Public().(ed25519.PublicKey), Control: false}}); err != nil {
				t.Fatal(err)
			}
		}
		if scenario == "forged" {
			request.Signature = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		}
		requestBytes, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		completedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Error("completed initialization uploaded a fresh response")
				w.WriteHeader(400)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"request": string(requestBytes), "expires_at": time.Now().Add(time.Minute), "state": "completed"}})
		}))
		err = deliverSessionInitialization(ctx, completedServer.Client(), machineCredential{CloudAPIURL: completedServer.URL, MachineID: "machine", MachineToken: "test"}, runtime, "session", client.Device, false)
		completedServer.Close()
		if scenario != "valid" && err == nil {
			t.Fatal("completed relay flag bypassed signature authentication")
		}
		if scenario == "valid" && err != nil {
			t.Fatalf("valid completed recovery: %v", err)
		}
	}
	if err := runtime.requireSession(ctx, "session"); err != nil {
		t.Fatal(err)
	}
}
