package main

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

func TestServeStartsLocalHubWithSQLiteAndStaticAuth(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "events.db")
	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       dbPath,
		SessionID:    "ses_local",
		Provider:     "claude-code",
		ControlToken: "control-token",
		AdapterToken: "adapter-token",
	})
	if err != nil {
		t.Fatalf("startServe() error = %v", err)
	}
	defer func() {
		cancel()
		if err := running.wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve wait error = %v", err)
		}
	}()

	if running.addr == "" || running.wsURL == "" {
		t.Fatalf("running server missing addresses: %+v", running)
	}
	parsed, err := url.Parse(running.wsURL)
	if err != nil {
		t.Fatalf("parse ws url: %v", err)
	}
	if parsed.Scheme != "ws" || parsed.Host == "" {
		t.Fatalf("ws url = %q", running.wsURL)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat sqlite db: %v", err)
	}

	conn, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial hub: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	ack := readFrame(t, conn).(*protocol.HelloAck)
	if ack.ProtocolVersion != protocol.ProtocolVersion || len(ack.Sessions) != 1 {
		t.Fatalf("hello ack = %+v", ack)
	}
	if got := ack.Sessions[0]; got.SessionID != "ses_local" || got.Provider != "claude-code" ||
		got.State != "ready" || got.LatestSeq != 0 || got.ReplayFrom != 1 {
		t.Fatalf("session summary = %+v", got)
	}
}

func TestRunServeRejectsNonLocalDefaultToken(t *testing.T) {
	t.Parallel()

	err := run(context.Background(), []string{
		"serve",
		"--addr", "0.0.0.0:0",
		"--db", filepath.Join(t.TempDir(), "events.db"),
	}, io.Discard, io.Discard)
	if err == nil || !errors.Is(err, errUnsafeDefaultToken) {
		t.Fatalf("run serve error = %v, want errUnsafeDefaultToken", err)
	}
}

func writeFrame(t *testing.T, conn *websocket.Conn, frame protocol.Frame) {
	t.Helper()

	data, err := protocol.Encode(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) protocol.Frame {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("websocket message type = %v, want text", typ)
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		t.Fatalf("decode frame %s: %v", string(data), err)
	}
	return frame
}
