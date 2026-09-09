package sqlite_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestEncryptedEventsAndProposalsPreserveCiphertextOnSQLite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "encrypted.db")
	s := openStore(t, path)
	if _, err := s.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "session", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	connection, err := s.AcceptAdapterHello(ctx, "session", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := s.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: fence}
	authority := store.CommandAuthority{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal(err)
	}
	makeEvent := func(id, state string) store.PendingEvent {
		t.Helper()
		payload, _ := json.Marshal(map[string]string{"state": state, "reason": "private-sqlite-canary"})
		packet, err := e2ee.SealPacket(e2ee.Context{Scope: "event", Session: "session", Sender: "machine", KeyID: "key", MessageID: id, Type: "session.state"}, key, private, e2ee.PublicMetadata{State: state}, payload)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := json.Marshal(e2ee.EventWire{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: id, Type: "session.state", Packet: packet})
		if err != nil {
			t.Fatal(err)
		}
		return store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: wire}
	}
	first := makeEvent("first", "ready")
	if seq, err := s.AppendEncryptedAdapterEvents(ctx, "session", admission, []store.PendingEvent{first}); err != nil || seq != 1 {
		t.Fatalf("append %d %v", seq, err)
	}
	proposal := store.ProposedEventRequest{ProposalID: "proposal", Event: makeEvent("second", "busy")}
	receipt, err := s.CommitEncryptedProposedEvent(ctx, "session", authority, proposal)
	if err != nil || receipt.Seq != 2 {
		t.Fatalf("proposal %+v %v", receipt, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, path)
	duplicate, err := s.CommitEncryptedProposedEvent(ctx, "session", authority, proposal)
	if err != nil || duplicate.Seq != 2 {
		t.Fatalf("restart duplicate %+v %v", duplicate, err)
	}
	conflict := proposal
	conflict.Event = makeEvent("second", "busy")
	if _, err = s.CommitEncryptedProposedEvent(ctx, "session", authority, conflict); err == nil {
		t.Fatal("conflicting ciphertext accepted")
	}
	bad := first
	bad.Payload = []byte(`{"state":"ready"}`)
	if _, err = s.AppendEncryptedAdapterEvents(ctx, "session", admission, []store.PendingEvent{first, bad}); err == nil {
		t.Fatal("plaintext batch accepted")
	}
	count := 0
	if err = s.Replay(ctx, "session", 0, func(event store.Event) error {
		count++
		if bytes.Contains(event.Payload, []byte("private-sqlite-canary")) {
			t.Fatal("plaintext persisted")
		}
		carrier, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", event.Type, "")
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(carrier.Packet)
		packet, err := e2ee.DecodeContentPacket(event.Type, raw)
		if err != nil {
			return err
		}
		plaintext, err := e2ee.OpenPacket(e2ee.Context{Scope: "event", Session: "session", Sender: carrier.Sender, KeyID: carrier.KeyID, MessageID: carrier.MessageID, Type: event.Type}, key, public, packet)
		if err != nil || !bytes.Contains(plaintext, []byte("private-sqlite-canary")) {
			t.Fatal("replay decryption failed", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("replay count=%d", count)
	}
	snapshot, err := s.AttentionSnapshot(ctx, []string{"session"})
	if err != nil || len(snapshot) != 1 || snapshot[0].State != "busy" {
		t.Fatalf("snapshot %+v %v", snapshot, err)
	}
	terminal := makeEvent("terminal", "ended")
	if _, err = s.AppendEncryptedAdapterEvents(ctx, "session", admission, []store.PendingEvent{terminal, first}); err == nil {
		t.Fatal("terminal with tail accepted")
	}
	if _, err = s.AppendEncryptedAdapterEvents(ctx, "session", admission, []store.PendingEvent{terminal}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitEncryptedProposedEvent(ctx, "session", authority, proposal); err == nil {
		t.Fatal("terminal authority replay accepted")
	}
}
