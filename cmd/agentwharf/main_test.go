package main

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

func TestParseWrapConfigAcceptsClaudeACPFlag(t *testing.T) {
	t.Parallel()

	cfg, err := parseWrapConfig([]string{
		"--agent", "claude",
		"--acp",
		"--hub", "ws://127.0.0.1:8765",
		"--session-id", "ses_1",
		"--adapter-token", "adapter-token",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseWrapConfig() error = %v", err)
	}
	if cfg.Agent != "claude" || cfg.Provider != "claude-code" || cfg.Format != "acp" {
		t.Fatalf("wrap config = %+v", cfg)
	}
}

func TestRunWrapJSONStreamPublishesEventsToHub(t *testing.T) {
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

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	stdin := strings.NewReader(strings.Join([]string{
		`{"type":"system","subtype":"init","session_id":"provider_ses"}`,
		`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"pong"}]}}`,
	}, "\n"))
	if err := runWithInput(ctx, []string{
		"wrap",
		"--hub", running.wsURL,
		"--session-id", "ses_local",
		"--adapter-token", "adapter-token",
		"--agent", "claude",
		"--jsonstream",
	}, stdin, io.Discard, io.Discard); err != nil {
		t.Fatalf("run wrap error = %v", err)
	}

	first := readFrame(t, client).(*protocol.Event)
	second := readFrame(t, client).(*protocol.Event)
	if first.Type != "session.state" || first.Seq == nil || *first.Seq != 1 {
		t.Fatalf("first event = %+v", first)
	}
	if second.Type != "session.message" || second.Seq == nil || *second.Seq != 2 || string(second.Payload) == "" {
		t.Fatalf("second event = %+v", second)
	}
}

func TestRunWrapACPPublishesEventsToHub(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	running, err := startServe(ctx, serveConfig{
		Addr:         "127.0.0.1:0",
		DBPath:       filepath.Join(t.TempDir(), "events.db"),
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

	client, _, err := websocket.Dial(ctx, running.wsURL, nil)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "control-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_local"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)

	stdin := strings.NewReader(`{"method":"session/update","params":{"session_id":"acp_ses","update":{"type":"agent_message_chunk","text":"hello"}}}`)
	if err := runWithInput(ctx, []string{
		"wrap",
		"--hub", running.wsURL,
		"--session-id", "ses_local",
		"--adapter-token", "adapter-token",
		"--agent", "claude",
		"--acp",
	}, stdin, io.Discard, io.Discard); err != nil {
		t.Fatalf("run wrap error = %v", err)
	}

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Type != "session.message" || ev.Seq == nil || *ev.Seq != 1 {
		t.Fatalf("event = %+v", ev)
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
