package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

func TestRequiredHubOutputSealsBeforeNetwork(t *testing.T) {
	for _, kind := range []string{"session.message", "agent.activity", "log.tail", "resource.sample", "presence"} {
		t.Run(kind, func(t *testing.T) { testRequiredHubOutputSealsBeforeNetwork(t, kind) })
	}
}
func testRequiredHubOutputSealsBeforeNetwork(t *testing.T, kind string) {
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
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	observed := make(chan *protocol.Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		frame, err := readCLIProtocolFrame(ctx, conn)
		if err != nil {
			t.Error(err)
			return
		}
		event, ok := frame.(*protocol.Event)
		if !ok {
			t.Error("not event")
			return
		}
		observed <- event
	}))
	defer server.Close()
	socket, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.CloseNow()
	connection := newHubConnection(wrapConfig{SessionID: "session", ContentMode: protocol.ContentModeRequired, ProtocolVersion: 2, e2eeRuntime: runtime}, socket, nil)
	event := &protocol.Event{SessionID: "session", Type: kind, Time: 1, Payload: []byte(`{"role":"agent","content":[{"kind":"text","text":"secret-output-canary"}]}`)}
	if kind != "session.message" {
		event.Payload = []byte(`{"detail":"secret-output-canary"}`)
	}
	if err := connection.write(ctx, event); err != nil {
		t.Fatal(err)
	}
	select {
	case wire := <-observed:
		if strings.Contains(string(wire.Payload), "secret-output-canary") || (wire.ProposalID == "") != isCLIEventEphemeral(kind) {
			t.Fatal("unsealed network event")
		}
		if _, err := protocol.DecodeEncryptedPacketCarrier(wire.Payload, "event", kind, ""); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("no encrypted output")
	}
	if !strings.Contains(string(event.Payload), "secret-output-canary") {
		t.Fatal("caller payload mutated")
	}
}
