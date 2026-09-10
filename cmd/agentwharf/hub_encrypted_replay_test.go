package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"net/http"
	"net/http/httptest"
	"nhooyr.io/websocket"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEncryptedHubConnectionReplaysExactCiphertext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "ses_reconnect", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	payloads := make(chan []byte, 2)
	var mu sync.Mutex
	connections := 0
	proposalIDs := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		frame, err := readFrameFromConn(ctx, conn)
		hello, ok := frame.(*protocol.Hello)
		if err != nil || !ok || hello.SessionID != "ses_reconnect" {
			t.Errorf("hello = %T %+v, %v", frame, frame, err)
			return
		}
		mu.Lock()
		connections++
		connectionNumber := connections
		mu.Unlock()
		if connectionNumber == 2 && !hello.Resume {
			t.Error("replacement hello did not request resume")
			return
		}
		ack := reconnectHelloAck("ses_reconnect", int64(connectionNumber), 1)
		ack.ContentMode = protocol.ContentModeRequired
		if hello.ContentMode != protocol.ContentModeRequired {
			t.Error("mode mismatch")
			return
		}
		if err := writeFrameToConn(ctx, conn, ack); err != nil {
			return
		}
		frame, err = readFrameFromConn(ctx, conn)
		event, ok := frame.(*protocol.Event)
		if err != nil || !ok || event.ProposalID == "" {
			t.Errorf("proposal = %T %+v, %v", frame, frame, err)
			return
		}
		proposalIDs <- event.ProposalID
		payloads <- event.Payload
		if connectionNumber == 1 {
			_ = conn.Close(websocket.StatusGoingAway, "temporary disconnect")
			return
		}
		_ = writeFrameToConn(ctx, conn, &protocol.EventReceipt{ProposalID: event.ProposalID, Seq: 1, Status: protocol.EventReceiptAccepted})
		frame, err = readFrameFromConn(ctx, conn)
		snapshot, ok := frame.(*protocol.Event)
		if err != nil || !ok || snapshot.Type != "session.run.capabilities" || snapshot.ProposalID == "" {
			t.Errorf("fresh snapshot missing: %T %v", frame, err)
			return
		}
		if bytes.Contains(snapshot.Payload, []byte("private-snapshot-canary")) {
			t.Error("fresh reconnect snapshot leaked plaintext")
			return
		}
		if _, err := protocol.DecodeEncryptedPacketCarrier(snapshot.Payload, "event", snapshot.Type, ""); err != nil {
			t.Errorf("snapshot is not encrypted: %v", err)
			return
		}
		var wire e2ee.EventWire
		if err := json.Unmarshal(snapshot.Payload, &wire); err != nil {
			t.Error(err)
			return
		}
		key, err := runtime.vault.Load(ctx, "ses_reconnect", wire.KeyID)
		if err != nil {
			t.Error(err)
			return
		}
		defer clear(key)
		verifyKey, err := base64.RawURLEncoding.DecodeString(runtime.public.SigningKey)
		if err != nil {
			t.Error(err)
			return
		}
		opened, err := e2ee.OpenPacket(e2ee.Context{Scope: wire.Scope, Session: "ses_reconnect", KeyID: wire.KeyID, Sender: wire.Sender, MessageID: wire.MessageID, Type: wire.Type}, key, ed25519.PublicKey(verifyKey), wire.Packet)
		if err != nil || !bytes.Contains(opened, []byte("private-snapshot-canary")) {
			t.Errorf("snapshot authentication failed: %v", err)
			return
		}
		_ = writeFrameToConn(ctx, conn, &protocol.EventReceipt{ProposalID: snapshot.ProposalID, Seq: 2, Status: protocol.EventReceiptAccepted})
		_ = writeFrameToConn(ctx, conn, &protocol.Ping{Nonce: "after-reconnect"})
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	initial, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrameToConn(ctx, initial, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleAdapter, Token: "token-1", SessionID: "ses_reconnect", Provider: "claude-code", ContentMode: protocol.ContentModeRequired}); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameFromConn(ctx, initial); err != nil {
		t.Fatal(err)
	}
	connection := newHubConnection(wrapConfig{HubURL: url, SessionID: "ses_reconnect", Provider: "claude-code", AdapterToken: "token-1", ProtocolVersion: protocol.ProtocolVersionV2, ContentMode: protocol.ContentModeRequired, e2eeRuntime: runtime}, initial, reconnectAuthority("ses_reconnect", 1, 1))
	defer connection.close()
	connection.setReconnectProposalFactory("capabilities", func() (*protocol.Event, error) {
		return &protocol.Event{Type: "session.run.capabilities", SessionID: "ses_reconnect", Time: time.Now().UnixMilli(), Payload: []byte(`{"schema_version":1,"interrupt_supported":true,"stop_supported":true,"detail":"private-snapshot-canary"}`)}, nil
	})
	event := &protocol.Event{Type: "session.message", SessionID: "ses_reconnect", Time: time.Now().UnixMilli(), Payload: []byte(`{"role":"agent","content":[{"kind":"text","text":"private-reconnect-canary"}]}`)}
	if err := connection.write(ctx, event); err != nil {
		t.Fatalf("write proposal: %v", err)
	}
	for {
		frame, err := connection.read(ctx)
		if err != nil {
			t.Fatalf("read after reconnect: %v", err)
		}
		if _, ok := frame.(*protocol.EventReceipt); ok {
			continue
		}
		if ping, ok := frame.(*protocol.Ping); !ok || ping.Nonce != "after-reconnect" {
			t.Fatalf("frame after reconnect = %#v", frame)
		}
		break
	}
	first, second := <-proposalIDs, <-proposalIDs
	if first != second {
		t.Fatalf("proposal ids = %q, %q; event=%q", first, second, event.ProposalID)
	}
	firstPayload, secondPayload := <-payloads, <-payloads
	if !bytes.Equal(firstPayload, secondPayload) || bytes.Contains(firstPayload, []byte("private-reconnect-canary")) {
		t.Fatal("invalid encrypted replay")
	}
	var used int
	if err := runtime.database.QueryRowContext(ctx, `SELECT used FROM e2ee_seal_budget WHERE session='ses_reconnect'`).Scan(&used); err != nil || used != 2 {
		t.Fatalf("resealed: %d %v", used, err)
	}
	if pending := connection.proposals(); len(pending) != 0 {
		t.Fatalf("pending proposals = %#v", pending)
	}
}
