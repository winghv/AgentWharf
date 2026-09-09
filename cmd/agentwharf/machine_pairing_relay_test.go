package main

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	_ "modernc.org/sqlite"
)

func TestMachinePairingRelayCommitsBeforeReceipt(t *testing.T) {
	testMachinePairingRelay(t, false)
}

func TestMachinePairingRelayRejectsSubstitutedInvitation(t *testing.T) {
	testMachinePairingRelay(t, true)
}

func testMachinePairingRelay(t *testing.T, substitute bool) {
	t.Helper()
	ctx := context.Background()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	registry, err := e2ee.NewDeviceRegistry(ctx, database, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	invitation, offer, err := e2ee.NewPairingInvitation("machine", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := e2ee.NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public, err := identity.Public()
	if err != nil {
		t.Fatal(err)
	}
	encryptionOffer := offer
	if substitute {
		_, encryptionOffer, err = e2ee.NewPairingInvitation("machine", time.Now())
		if err != nil {
			t.Fatal(err)
		}
	}
	request, err := e2ee.EncryptPairingIdentity(encryptionOffer, public, ed25519.NewKeyFromSeed(identity.SigningSeed), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	posted := false
	gets, posts := 0, 0
	var firstConfirmation string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-machine" {
			t.Error("missing machine authentication")
		}
		if r.Method == http.MethodGet {
			gets++
			writeTestJSON(w, 200, map[string]any{"data": map[string]string{"request": string(raw)}})
			return
		}
		var response struct{ Request, Confirmation string }
		if json.NewDecoder(r.Body).Decode(&response) != nil || response.Request != string(raw) || response.Confirmation == "" {
			t.Error("invalid confirmation response")
		}
		stored, err := registry.Device(ctx, identity.Device)
		if err != nil || stored != public {
			t.Error("receipt before durable enrollment")
		}
		posts++
		if posts == 1 {
			firstConfirmation = response.Confirmation
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if response.Confirmation != firstConfirmation {
			t.Error("receipt changed on retry")
		}
		posted = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	err = pollEncryptedPairing(ctx, server.Client(), machineCredential{CloudAPIURL: server.URL, MachineID: "machine", MachineToken: "test-machine"}, offer.ID, time.Now().Add(3*time.Second), invitation, registry)
	if substitute {
		if err == nil || posted || gets != 1 || posts != 0 {
			t.Fatalf("substituted pairing reached receipt: %v", err)
		}
		if _, err := registry.Device(ctx, identity.Device); err != e2ee.ErrUnauthorized {
			t.Fatalf("substituted identity enrolled: %v", err)
		}
		return
	}
	if err != nil || !posted || gets != 1 || posts != 2 {
		t.Fatalf("pairing: %v", err)
	}
}
