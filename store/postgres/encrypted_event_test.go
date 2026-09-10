package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestEncryptedAdapterAppendPersistsCiphertextAndProjectsOnlyPublicState(t *testing.T) {
	h := newPostgresConnectionHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const session = "encrypted_session"
	if _, err := h.pool.Exec(ctx, `INSERT INTO agent_sessions(id,status) VALUES($1,'starting')`, session); err != nil {
		t.Fatal(err)
	}
	if _, err := h.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: session, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	connection, err := h.AcceptAdapterHello(ctx, session, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := h.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grant}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal(err)
	}
	command := e2ee.Context{Scope: "event", Session: session, Sender: "machine", KeyID: "key", MessageID: "event", Type: "session.state"}
	plaintext := json.RawMessage(`{"state":"ready","reason":"synthetic-private-canary"}`)
	packet, err := e2ee.SealPacket(command, key, private, e2ee.PublicMetadata{State: "ready"}, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(e2ee.EventWire{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: "event", Type: "session.state", Packet: packet})
	if err != nil {
		t.Fatal(err)
	}
	pending := store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: wire}
	seq, err := h.AppendEncryptedAdapterEvents(ctx, session, admission, []store.PendingEvent{pending})
	if err != nil || seq != 1 {
		t.Fatalf("append=%d %v", seq, err)
	}
	var status string
	if err = h.pool.QueryRow(ctx, `SELECT status FROM agent_sessions WHERE id=$1`, session).Scan(&status); err != nil || status != "ready" {
		t.Fatalf("projection=%q %v", status, err)
	}
	var stored []byte
	if err = h.pool.QueryRow(ctx, `SELECT payload FROM session_events WHERE session_id=$1 AND seq=1`, session).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte("synthetic-private-canary")) {
		t.Fatal("plaintext persisted")
	}
	carrier, err := protocol.DecodeEncryptedPacketCarrier(stored, "event", "session.state", "")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(carrier.Packet)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := e2ee.DecodeContentPacket("session.state", encoded)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := e2ee.OpenPacket(command, key, public, recovered)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatal("stored ciphertext authentication failed", err)
	}
	bad := pending
	bad.Payload = plaintext
	if _, err = h.AppendEncryptedAdapterEvents(ctx, session, admission, []store.PendingEvent{pending, bad}); err == nil {
		t.Fatal("plaintext accepted")
	}
	if latest, err := h.LatestSeq(ctx, session); err != nil || latest != 1 {
		t.Fatalf("invalid batch changed seq=%d %v", latest, err)
	}
	stale := admission
	stale.ConnectionEpoch++
	if _, err = h.AppendEncryptedAdapterEvents(ctx, session, stale, []store.PendingEvent{pending}); err == nil {
		t.Fatal("stale authority accepted")
	}
}
