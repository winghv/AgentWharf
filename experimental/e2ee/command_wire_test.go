package e2ee

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestCommandWireBindsRoutingToSignedContext(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	ctx := Context{Scope: "command", Session: "session", Sender: "client", KeyID: "key", MessageID: "command", Type: "session.send"}
	packet, err := SealPacket(ctx, key, private, PublicMetadata{}, json.RawMessage(`{"content":[{"kind":"text","text":"synthetic canary"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	wire := map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "command", "type": "session.send", "packet": packet}
	encoded, _ := json.Marshal(wire)
	decoded, p, err := DecodeCommandWire("session", "command", "session.send", encoded)
	if err != nil || decoded != ctx {
		t.Fatalf("context=%+v error=%v", decoded, err)
	}
	if _, err = OpenPacket(decoded, key, public, p); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"sender", "key_id", "message_id", "type", "scope"} {
		t.Run(field, func(t *testing.T) {
			changed := map[string]any{}
			for k, v := range wire {
				changed[k] = v
			}
			changed[field] = "substitute"
			raw, _ := json.Marshal(changed)
			c, p, err := DecodeCommandWire("session", "command", "session.send", raw)
			if err == nil {
				_, err = OpenPacket(c, key, public, p)
			}
			if err == nil {
				t.Fatal("substituted context accepted")
			}
		})
	}
	c, p, err := DecodeCommandWire("other-session", "command", "session.send", encoded)
	if err == nil {
		_, err = OpenPacket(c, key, public, p)
	}
	if err == nil {
		t.Fatal("cross session accepted")
	}
	for _, raw := range [][]byte{append(encoded, []byte(`{}`)...), append([]byte(`{"scope":"command",`), encoded[1:]...)} {
		if _, _, err := DecodeCommandWire("session", "command", "session.send", raw); err == nil {
			t.Fatal("malformed wire accepted")
		}
	}
}

// The machine may author a session.send only for a session whose current key
// grants it control; a terminal grant alone must never let it re-seal a launch.
func TestSealCommandAsMachineRequiresControlGrant(t *testing.T) {
	ctx := context.Background()
	journal, db := openJournal(t, filepath.Join(t.TempDir(), "endpoint.db"))
	machine, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewSessionKeyVault(ctx, db, machine)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewCommandExecutor(journal, vault)
	if err != nil {
		t.Fatal(err)
	}
	clientPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := vault.TransitionSession(ctx, journal, "session", "key_1", 0, []DeviceGrant{{DeviceID: "client", VerifyKey: clientPublic, Control: true}}); err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"launch":{"provider":"test-provider"}}`)
	if _, err := vault.SealCommandAsMachine(ctx, "session", "recovery:session", payload); err == nil {
		t.Fatal("machine sealed a command without a control grant")
	}
	machinePublic := ed25519.NewKeyFromSeed(machine.SigningSeed).Public().(ed25519.PublicKey)
	if err := vault.TransitionSession(ctx, journal, "session", "key_2", 1, []DeviceGrant{{DeviceID: machine.Device, VerifyKey: machinePublic, Control: true}}); err != nil {
		t.Fatal(err)
	}
	wire, err := vault.SealCommandAsMachine(ctx, "session", "recovery:session", payload)
	if err != nil {
		t.Fatal(err)
	}
	if wire.Sender != machine.Device || wire.Scope != "command" || wire.Type != "session.send" || wire.MessageID != "recovery:session" {
		t.Fatalf("machine wire context = %+v", wire)
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.VerifyWire(ctx, "session", "recovery:session", "session.send", encoded); err != nil {
		t.Fatal("machine-authored carrier failed current-grant verification", err)
	}
	command, packet, err := DecodeCommandWire("session", "recovery:session", "session.send", encoded)
	if err != nil {
		t.Fatal(err)
	}
	key, err := vault.Load(ctx, "session", "key_2")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	opened, err := OpenPacket(command, key, machinePublic, packet)
	if err != nil || string(opened) != string(payload) {
		t.Fatal("machine-authored payload roundtrip", err)
	}
}
