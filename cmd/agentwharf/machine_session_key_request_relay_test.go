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

func TestSessionKeyRequestRelayVerifiesGrantAndRetriesFixedResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	clientIdentity, err := e2ee.NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public, err := clientIdentity.Public()
	if err != nil {
		t.Fatal(err)
	}
	invitation, offer, err := e2ee.NewPairingInvitation("machine", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pairing, err := e2ee.EncryptPairingIdentity(offer, public, ed25519.NewKeyFromSeed(clientIdentity.SigningSeed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.registry.Enroll(ctx, invitation, pairing, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: clientIdentity.Device, VerifyKey: ed25519.NewKeyFromSeed(clientIdentity.SigningSeed).Public().(ed25519.PublicKey), Control: false}}); err != nil {
		t.Fatal(err)
	}
	request, err := e2ee.SignSessionKeyRequest(e2ee.SessionKeyRequest{Machine: "machine", Account: "account", Session: "session", KeyID: "key", Device: clientIdentity.Device}, ed25519.NewKeyFromSeed(clientIdentity.SigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	posts := 0
	var firstResponse []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"request": string(requestBytes), "state": "pending", "expires_at": time.Now().Add(3 * time.Second)}})
			return
		}
		var body struct {
			Request    string          `json:"request"`
			WrappedKey json.RawMessage `json:"wrapped_key"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Request != string(requestBytes) {
			t.Error("changed key response request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		posts++
		if posts == 1 {
			firstResponse = append([]byte(nil), body.WrappedKey...)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if !bytes.Equal(firstResponse, body.WrappedKey) {
			t.Error("key response changed across retry")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	credential := machineCredential{CloudAPIURL: server.URL, MachineID: "machine", MachineToken: "test"}
	if err := deliverSessionKeyRequest(ctx, server.Client(), credential, runtime, "session", clientIdentity.Device, "key", false); err != nil {
		t.Fatal(err)
	}
	if posts != 2 {
		t.Fatalf("response attempts = %d", posts)
	}

	forged := request
	forged.Signature = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	forgedBytes, _ := json.Marshal(forged)
	completedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"request": string(forgedBytes), "state": "completed", "expires_at": time.Now().Add(time.Minute)}})
	}))
	defer completedServer.Close()
	credential.CloudAPIURL = completedServer.URL
	if err := deliverSessionKeyRequest(ctx, completedServer.Client(), credential, runtime, "session", clientIdentity.Device, "key", false); err == nil {
		t.Fatal("completed relay bypassed request signature verification")
	}
}
