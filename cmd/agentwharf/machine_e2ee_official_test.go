package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

// The interactive entrypoint must inject a Console instruction into the running
// official CLI. Before this path supported the encrypted lane it returned
// "required encrypted command executor is unavailable", so the Console command
// never received an acknowledgement.
func TestDeliverEncryptedOfficialCommandInjectsPrompt(t *testing.T) {
	ctx := context.Background()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	key, err := runtime.vault.Load(ctx, "session", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	payload := json.RawMessage(`{"content":[{"kind":"text","text":"console instruction"}]}`)
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "cmd", Type: "session.send"}, key, private, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "cmd", "type": "session.send", "packet": packet})
	if err != nil {
		t.Fatal(err)
	}
	command := &protocol.Command{SessionID: "session", CommandID: "cmd", Type: protocol.CommandSessionSend, Payload: wire}

	pty, err := os.CreateTemp(t.TempDir(), "pty")
	if err != nil {
		t.Fatal(err)
	}
	defer pty.Close()

	var acks []protocol.Frame
	writeFrame := func(frame protocol.Frame) error {
		acks = append(acks, frame)
		return nil
	}
	cfg := wrapConfig{SessionID: "session", ContentMode: protocol.ContentModeRequired, e2eeRuntime: runtime}
	if err := deliverEncryptedOfficialCommand(ctx, cfg, &hubConnection{}, command, writeFrame, pty, &sync.Mutex{}, nil, &atomic.Bool{}, nil, nil); err != nil {
		t.Fatalf("deliverEncryptedOfficialCommand() error = %v", err)
	}
	if len(acks) != 1 {
		t.Fatalf("acks = %d, want 1", len(acks))
	}
	ack, ok := acks[0].(*protocol.CommandAck)
	if !ok || ack.CommandID != "cmd" || ack.Status != protocol.AckAccepted {
		t.Fatalf("ack = %+v", acks[0])
	}
	if _, err := pty.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	written, err := io.ReadAll(pty)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(written), "console instruction") {
		t.Fatalf("pty = %q", written)
	}
}

// A second delivery of the same command id is an idempotent duplicate: the PTY
// must not receive the prompt twice and the Console receives a duplicate ack.
func TestDeliverEncryptedOfficialCommandAcksDuplicate(t *testing.T) {
	ctx := context.Background()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	key, err := runtime.vault.Load(ctx, "session", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	payload := json.RawMessage(`{"content":[{"kind":"text","text":"only once"}]}`)
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "dup", Type: "session.send"}, key, private, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "dup", "type": "session.send", "packet": packet})
	if err != nil {
		t.Fatal(err)
	}
	command := &protocol.Command{SessionID: "session", CommandID: "dup", Type: protocol.CommandSessionSend, Payload: wire}

	pty, err := os.CreateTemp(t.TempDir(), "pty")
	if err != nil {
		t.Fatal(err)
	}
	defer pty.Close()

	var second protocol.Frame
	deliver := func(writeFrame func(protocol.Frame) error) error {
		return deliverEncryptedOfficialCommand(ctx, wrapConfig{SessionID: "session", ContentMode: protocol.ContentModeRequired, e2eeRuntime: runtime}, &hubConnection{}, command, writeFrame, pty, &sync.Mutex{}, nil, &atomic.Bool{}, nil, nil)
	}
	if err := deliver(func(frame protocol.Frame) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := deliver(func(frame protocol.Frame) error { second = frame; return nil }); err != nil {
		t.Fatal(err)
	}
	ack, ok := second.(*protocol.CommandAck)
	if !ok || ack.Status != protocol.AckDuplicate {
		t.Fatalf("second ack = %+v", second)
	}
}

// The command loop is the boundary that rejected every encrypted command before
// this change. Drive it over a real WebSocket so a regression in the wiring, not
// only in the delivery helper, fails the test.
func TestOfficialCommandLoopAcksEncryptedSessionSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "ses_official", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	key, err := runtime.vault.Load(ctx, "ses_official", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	payload := json.RawMessage(`{"content":[{"kind":"text","text":"console loop instruction"}]}`)
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "ses_official", KeyID: "key", Sender: "client", MessageID: "cmd-loop", Type: "session.send"}, key, private, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "cmd-loop", "type": "session.send", "packet": packet})
	if err != nil {
		t.Fatal(err)
	}

	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if _, err := readFrameFromConn(ctx, conn); err != nil {
			serverErr <- fmt.Errorf("read hello: %w", err)
			return
		}
		if err := writeFrameToConn(ctx, conn, reconnectHelloAck("ses_official", 1, 1)); err != nil {
			serverErr <- err
			return
		}
		if err := writeFrameToConn(ctx, conn, &protocol.Command{SessionID: "ses_official", CommandID: "cmd-loop", Type: protocol.CommandSessionSend, Payload: wire}); err != nil {
			serverErr <- err
			return
		}
		frame, err := readFrameFromConn(ctx, conn)
		ack, ok := frame.(*protocol.CommandAck)
		if err != nil || !ok || ack.CommandID != "cmd-loop" || ack.Status != protocol.AckAccepted {
			serverErr <- fmt.Errorf("adapter ack = %T %+v, %v", frame, frame, err)
			return
		}
		serverErr <- nil
		<-r.Context().Done()
	}))
	defer server.Close()

	hubURL := "ws" + strings.TrimPrefix(server.URL, "http")
	initial, _, err := websocket.Dial(ctx, hubURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrameToConn(ctx, initial, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleAdapter,
		Token: "token", SessionID: "ses_official", Provider: "claude-code", ContentMode: protocol.ContentModeRequired,
	}); err != nil {
		t.Fatal(err)
	}
	frame, err := readFrameFromConn(ctx, initial)
	ack, ok := frame.(*protocol.HelloAck)
	if err != nil || !ok {
		t.Fatalf("initial ack = %T %+v, %v", frame, frame, err)
	}
	cfg := wrapConfig{
		HubURL: hubURL, SessionID: "ses_official", Provider: "claude-code",
		AdapterToken: "token", ProtocolVersion: protocol.ProtocolVersionV2,
		ContentMode: protocol.ContentModeRequired, e2eeRuntime: runtime,
	}
	connection := newHubConnection(cfg, initial, ack.ConnectionAuthority)
	defer connection.close()
	writeFrame := func(f protocol.Frame) error { return connection.write(ctx, f) }
	rotation := newCredentialRotationManager(ctx, ack.ConnectionAuthority, writeFrame, connection.credentials, connection.currentAuthority)

	pty, err := os.CreateTemp(t.TempDir(), "pty")
	if err != nil {
		t.Fatal(err)
	}
	defer pty.Close()

	commandDone := make(chan error, 1)
	go func() {
		commandDone <- forwardHubCommandsToOfficialCLI(ctx, cfg, connection, writeFrame, pty, &sync.Mutex{}, nil, &atomic.Bool{}, nil, nil, nil, rotation)
	}()
	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	select {
	case err := <-commandDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("official command loop did not stop")
	}
}
