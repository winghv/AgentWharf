package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

func TestEncryptedLaunchRejectsNegotiationDowngrade(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame protocol.Frame
	}{
		{"missing mode", &protocol.HelloAck{ProtocolVersion: 2, Sessions: []protocol.SessionSummary{{SessionID: "ses", State: "ready"}}}},
		{"wrong session", &protocol.HelloAck{ProtocolVersion: 2, ContentMode: protocol.ContentModeRequired, Sessions: []protocol.SessionSummary{{SessionID: "other", State: "ready"}}}},
		{"event before ack", &protocol.Event{SessionID: "ses", Type: "session.state", Payload: []byte(`{"state":"ready"}`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			observed := make(chan bool, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				frame, err := readCLIProtocolFrame(ctx, conn)
				if err != nil {
					return
				}
				hello, ok := frame.(*protocol.Hello)
				if !ok || hello.ProtocolVersion != 2 || hello.ContentMode != protocol.ContentModeRequired {
					t.Error("missing required negotiation")
				}
				if err := writeCLIProtocolFrame(ctx, conn, tc.frame); err != nil {
					return
				}
				frame, err = readCLIProtocolFrame(ctx, conn)
				_, command := frame.(*protocol.Command)
				observed <- err == nil && command
			}))
			defer server.Close()
			err := sendFirstInstruction(ctx, machineServeDispatch{HubWSURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "ses", ClientToken: "test"}, "cmd")
			if err == nil || !strings.Contains(err.Error(), "encrypted launch") {
				t.Fatalf("error = %v", err)
			}
			select {
			case command := <-observed:
				if command {
					t.Fatal("command sent after invalid negotiation")
				}
			case <-ctx.Done():
				t.Fatal("server did not observe connection closure")
			}
		})
	}
}

func TestEncryptedLaunchSenderRejectsPlaintextBeforeWrite(t *testing.T) {
	for _, raw := range []string{"plaintext", `{"content":[]}`, `{}`} {
		if err := sendClientCommand(context.Background(), nil, "ses", raw, "cmd"); err == nil {
			t.Fatal("invalid carrier accepted")
		}
	}
}
