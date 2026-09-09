package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestEventSealerBudgetSurvivesRestartAndDeniesHistoricalWrite(t *testing.T) {
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
	public := ed25519.NewKeyFromSeed(identity.SigningSeed).Public().(ed25519.PublicKey)
	grants := []DeviceGrant{{identity.Device, public, true}}
	if err := vault.TransitionSession(ctx, journal, "session", "key_1", 0, grants); err != nil {
		t.Fatal(err)
	}
	event := Context{"event", "session", identity.Device, "key_1", "message", "session.message"}
	payload := json.RawMessage(`{"role":"agent","content":[{"kind":"text","text":"synthetic private reply"}]}`)
	wire, err := vault.SealEventWire(ctx, event, payload)
	if err != nil {
		t.Fatal(err)
	}
	if wire.Version != 1 || wire.Scope != "event" || wire.Sender != event.Sender || wire.MessageID != event.MessageID || wire.KeyID != event.KeyID || wire.Type != event.Type {
		t.Fatal("event wire lost signing context")
	}
	packet := wire.Packet
	key, err := vault.Load(ctx, "session", "key_1")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenPacket(event, key, public, packet)
	if err != nil || string(opened) != string(payload) {
		t.Fatal("event roundtrip", err)
	}
	if _, err := db.Exec(`UPDATE e2ee_seal_budget SET used=?`, MaxEndpointSealsPerKey-1); err != nil {
		t.Fatal(err)
	}
	db.Close()
	journal, db = openJournal(t, path)
	vault, err = NewSessionKeyVault(ctx, db, identity)
	if err != nil {
		t.Fatal(err)
	}
	event.MessageID = "last"
	if _, err := vault.SealEvent(ctx, event, payload); err != nil {
		t.Fatal(err)
	}
	event.MessageID = "exhausted"
	if _, err := vault.SealEvent(ctx, event, payload); !errors.Is(err, ErrCapacity) {
		t.Fatal("budget reset", err)
	}
	if err := vault.TransitionSession(ctx, journal, "session", "key_2", 1, grants); err != nil {
		t.Fatal(err)
	}
	if _, err := vault.SealEvent(ctx, event, payload); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("historical encryption allowed", err)
	}
	event.KeyID = "key_2"
	if _, err := vault.SealEvent(ctx, event, payload); err != nil {
		t.Fatal("new epoch unavailable", err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_budget BEFORE UPDATE ON e2ee_seal_budget BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
		t.Fatal(err)
	}
	if packet, err := vault.SealEvent(ctx, event, payload); err == nil || packet != (ContentPacket{}) {
		t.Fatal("ciphertext escaped failed reservation")
	}
}
