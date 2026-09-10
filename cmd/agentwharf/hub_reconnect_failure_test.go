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

func TestReconnectPreparationFailureClosesInstalledSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closed := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, err := readFrameFromConn(ctx, conn); err != nil {
			return
		}
		ack := reconnectHelloAck("session", 1, 1)
		ack.ContentMode = protocol.ContentModeRequired
		if err := writeFrameToConn(ctx, conn, ack); err != nil {
			return
		}
		_, _, err = conn.Read(ctx)
		closed <- err
	}))
	defer server.Close()
	connection := newHubConnection(wrapConfig{HubURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "session", Provider: "claude-code", AdapterToken: "synthetic", ProtocolVersion: 2, ContentMode: protocol.ContentModeRequired}, nil, nil)
	defer connection.close()
	remove := connection.setReconnectProposalFactory("snapshot", func() (*protocol.Event, error) {
		return &protocol.Event{SessionID: "session", Type: "session.state", Time: 1, Payload: []byte(`{"state":"ready"}`)}, nil
	})
	defer remove()
	// No endpoint runtime: snapshot preparation must fail without leaving a
	// newly accepted socket available to concurrent writers.
	if err := connection.reconnect(ctx, nil); err == nil {
		t.Fatal("missing runtime accepted")
	}
	if connection.current() != nil {
		t.Fatal("failed reconnect retained transport")
	}
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("failed snapshot sent a frame")
		}
	case <-ctx.Done():
		t.Fatal("server did not observe closure")
	}
}
