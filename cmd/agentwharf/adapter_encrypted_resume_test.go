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

func TestRequiredAdapterResumeModeOverWebSocket(t *testing.T) {
	for _, mode := range []string{"", protocol.ContentModeLegacy, protocol.ContentModeRequired} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			observed := make(chan *protocol.Hello, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				frame, err := readFrameFromConn(ctx, conn)
				if err != nil {
					return
				}
				hello, ok := frame.(*protocol.Hello)
				if !ok {
					return
				}
				observed <- hello
				ack := reconnectHelloAck("session", 2, 1)
				ack.ContentMode = mode
				if err := writeFrameToConn(ctx, conn, ack); err != nil {
					return
				}
				_, _, _ = conn.Read(ctx)
			}))
			defer server.Close()
			connection := &hubConnection{cfg: wrapConfig{HubURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "session", Provider: "claude-code", ProtocolVersion: 2, ContentMode: protocol.ContentModeRequired}}
			resumed, authority, _, err := connection.dialAndResume(ctx, "synthetic")
			if resumed != nil {
				defer resumed.CloseNow()
			}
			if mode == protocol.ContentModeRequired {
				if err != nil || resumed == nil || authority == nil {
					t.Fatalf("required resume failed: %v", err)
				}
			} else if err == nil || resumed != nil || authority != nil {
				t.Fatal("downgrade established a connection")
			}
			select {
			case hello := <-observed:
				if !hello.Resume || hello.ContentMode != protocol.ContentModeRequired || hello.SessionID != "session" {
					t.Fatal("incorrect resume hello")
				}
			case <-ctx.Done():
				t.Fatal("resume hello not observed")
			}
		})
	}
}
