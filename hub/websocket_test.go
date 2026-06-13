package hub_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"nhooyr.io/websocket"
)

func TestWebSocketServerAcceptsHelloAndPing(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1", LastSeq: 2}},
	})
	ack := readFrame(t, conn).(*protocol.HelloAck)
	if len(ack.Sessions) != 1 || ack.Sessions[0].SessionID != "ses_1" ||
		ack.Sessions[0].LatestSeq != 7 || ack.Sessions[0].ReplayFrom != 3 {
		t.Fatalf("hello ack = %+v", ack)
	}

	writeFrame(t, conn, &protocol.Ping{Nonce: "n1"})
	pong := readFrame(t, conn).(*protocol.Pong)
	if pong.Nonce != "n1" {
		t.Fatalf("pong nonce = %q", pong.Nonce)
	}
}

func TestWebSocketServerReplaysEventsAfterHelloAck(t *testing.T) {
	t.Parallel()

	events := fakeEventStore{
		latest: map[string]int64{"ses_1": 4},
		events: map[string][]store.Event{
			"ses_1": {
				{SessionID: "ses_1", Seq: 1, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)},
				{SessionID: "ses_1", Seq: 3, Type: "session.message", Time: time.UnixMilli(1003), Payload: json.RawMessage(`{"n":3}`)},
				{SessionID: "ses_1", Seq: 4, Type: "session.state", Time: time.UnixMilli(1004), Payload: json.RawMessage(`{"state":"ready"}`)},
			},
		},
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1", LastSeq: 2}},
	})
	if ack := readFrame(t, conn).(*protocol.HelloAck); ack.Sessions[0].ReplayFrom != 3 {
		t.Fatalf("hello ack = %+v", ack)
	}
	first := readFrame(t, conn).(*protocol.Event)
	second := readFrame(t, conn).(*protocol.Event)
	if first.SessionID != "ses_1" || first.Seq == nil || *first.Seq != 3 ||
		first.Type != "session.message" || first.Time != 1003 || string(first.Payload) != `{"n":3}` {
		t.Fatalf("first replay event = %+v payload=%s", first, string(first.Payload))
	}
	if second.SessionID != "ses_1" || second.Seq == nil || *second.Seq != 4 ||
		second.Type != "session.state" || second.Time != 1004 {
		t.Fatalf("second replay event = %+v", second)
	}
}

func TestWebSocketServerRejectsFirstFrameThatIsNotHello(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Ping{Nonce: "early"})
	errFrame := readFrame(t, conn).(*protocol.Error)
	if errFrame.Code != "invalid_hello" || !errFrame.Fatal {
		t.Fatalf("error frame = %+v", errFrame)
	}
}

func TestWebSocketServerRejectsUnauthorizedHello(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "bad-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
	})
	errFrame := readFrame(t, conn).(*protocol.Error)
	if errFrame.Code != "unauthorized" || !errFrame.Fatal {
		t.Fatalf("error frame = %+v", errFrame)
	}
}

func TestWebSocketServerTimesOutWaitingForHello(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake(), func(cfg *hub.WebSocketConfig) {
		cfg.HandshakeTimeout = 20 * time.Millisecond
	})
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	errFrame := readFrame(t, conn).(*protocol.Error)
	if errFrame.Code != "timeout" || !errFrame.Fatal {
		t.Fatalf("error frame = %+v", errFrame)
	}
}

func newWebSocketTestServer(t *testing.T, handshake *hub.Handshake, options ...func(*hub.WebSocketConfig)) *httptest.Server {
	t.Helper()

	cfg := hub.WebSocketConfig{Handshake: handshake}
	for _, option := range options {
		option(&cfg)
	}
	srv := httptest.NewServer(hub.NewWebSocketHandler(cfg))
	t.Cleanup(srv.Close)
	return srv
}

func dialWebSocket(t *testing.T, httpURL string) *websocket.Conn {
	t.Helper()

	wsURL := "ws" + strings.TrimPrefix(httpURL, "http")
	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	return conn
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
		t.Fatalf("message type = %v, want text", typ)
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		t.Fatalf("decode frame %s: %v", string(data), err)
	}
	return frame
}

func testHandshake() *hub.Handshake {
	return testHandshakeWithStore(fakeEventStore{latest: map[string]int64{"ses_1": 7}})
}

func testHandshakeWithStore(events fakeEventStore) *hub.Handshake {
	return hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{
			token: "client-token",
			principal: auth.Principal{
				Subject: "client",
				Scopes:  []auth.Scope{auth.SessionView("ses_1")},
			},
		},
		EventStore:    events,
		SessionLookup: fakeSessions{"ses_1": {State: "ready", Provider: "claude-code"}},
	})
}

type fakeEventStore struct {
	latest map[string]int64
	events map[string][]store.Event
}

func (f fakeEventStore) Append(context.Context, string, []store.PendingEvent) (int64, error) {
	return 0, errors.New("append is not implemented by fakeEventStore")
}

func (f fakeEventStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	return f.latest[sessionID], nil
}

func (f fakeEventStore) Replay(_ context.Context, sessionID string, afterSeq int64, fn func(store.Event) error) error {
	for _, ev := range f.events[sessionID] {
		if ev.Seq <= afterSeq {
			continue
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}
