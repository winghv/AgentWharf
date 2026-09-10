package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestEventSealerConcurrentLastReservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoint.sqlite")
	journal, db := openJournal(t, path)
	identity, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	grants := []DeviceGrant{{identity.Device, ed25519.NewKeyFromSeed(identity.SigningSeed).Public().(ed25519.PublicKey), true}}
	if err := vault.TransitionSession(ctx, journal, "session", "key", 0, grants); err != nil {
		t.Fatal(err)
	}
	_, otherDB := openJournal(t, path)
	other, err := NewSessionKeyVault(ctx, otherDB, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO e2ee_seal_budget VALUES(?,?,?,?)`, identity.Device, "session", "key", MaxEndpointSealsPerKey-1); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, v := range []*SessionKeyVault{vault, other} {
		wg.Add(1)
		go func(v *SessionKeyVault) {
			defer wg.Done()
			<-start
			_, err := v.SealEvent(ctx, Context{"event", "session", identity.Device, "key", "message", "session.state"}, json.RawMessage(`{"state":"busy"}`))
			results <- err
		}(v)
	}
	close(start)
	wg.Wait()
	close(results)
	successes, exhausted := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrCapacity) {
			exhausted++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || exhausted != 1 {
		t.Fatal("reservation overshoot", successes, exhausted)
	}
}
