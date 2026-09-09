package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"github.com/winghv/agentwharf/experimental/e2ee"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnrollmentCommandPreservesExistingOfferFile(t *testing.T) {
	setupServeTestEnv(t)
	if err := saveMachineCredential(machineCredential{MachineID: "machine", MachineToken: "synthetic", CloudAPIURL: "https://api.example"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "offer.json")
	if err := os.WriteFile(path, []byte("existing-user-work"), 0600); err != nil {
		t.Fatal(err)
	}
	err := runPairCommand(context.Background(), []string{"--enroll", "local-account", path}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("existing offer overwritten")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "existing-user-work" {
		t.Fatal("existing file altered or removed")
	}
}

func TestEnrollmentCommandCompletesRealPairingAndRemovesOffer(t *testing.T) {
	setupServeTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "offer.json")
	device, err := e2ee.NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public, err := device.Public()
	if err != nil {
		t.Fatal(err)
	}
	var request []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Error("offer missing before polling")
				w.WriteHeader(500)
				return
			}
			stat, err := os.Stat(path)
			if err != nil || stat.Mode().Perm() != 0600 {
				t.Error("offer permissions")
			}
			var offer e2ee.MachineOffer
			if json.Unmarshal(data, &offer) != nil || e2ee.VerifyMachineOffer(offer, time.Now()) != nil {
				t.Error("invalid local offer")
				w.WriteHeader(500)
				return
			}
			wrapped, err := e2ee.EncryptPairingIdentity(offer.Offer, public, ed25519.NewKeyFromSeed(device.SigningSeed), time.Now())
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			request, err = json.Marshal(wrapped)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			writeTestJSON(w, 200, map[string]any{"data": map[string]string{"request": string(request)}})
			return
		}
		var receipt struct{ Request, Confirmation string }
		if json.NewDecoder(r.Body).Decode(&receipt) != nil || receipt.Request != string(request) || receipt.Confirmation == "" {
			t.Error("receipt invalid")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	if err := saveMachineCredential(machineCredential{MachineID: "machine", MachineToken: "synthetic", CloudAPIURL: server.URL}); err != nil {
		t.Fatal(err)
	}
	if err := runPairCommand(ctx, []string{"--enroll", "account", path}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("offer not removed: %v", err)
	}
}

func TestEnrollmentCommandRequiresExplicitLocalDestination(t *testing.T) {
	for _, args := range [][]string{{"--enroll"}, {"--enroll", "account", "relative"}, {"--enroll", "", "/tmp/offer"}} {
		if err := runPairCommand(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Fatal("invalid enrollment invocation accepted")
		}
	}
}
