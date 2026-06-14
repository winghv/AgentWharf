package hub_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
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

	events := newFakeEventStore(map[string]int64{"ses_1": 4}, map[string][]store.Event{
		"ses_1": {
			{SessionID: "ses_1", Seq: 1, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)},
			{SessionID: "ses_1", Seq: 3, Type: "session.message", Time: time.UnixMilli(1003), Payload: json.RawMessage(`{"n":3}`)},
			{SessionID: "ses_1", Seq: 4, Type: "session.state", Time: time.UnixMilli(1004), Payload: json.RawMessage(`{"state":"ready"}`)},
		},
	})
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

func TestWebSocketServerPersistsAdapterDurableEventBeforeFanout(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2001,
		Payload:   json.RawMessage(`{"role":"agent"}`),
	})

	ev := readFrame(t, client).(*protocol.Event)
	if ev.SessionID != "ses_1" || ev.Type != "session.message" || ev.Seq == nil || *ev.Seq != 1 ||
		ev.Time != 2001 || string(ev.Payload) != `{"role":"agent"}` {
		t.Fatalf("fanout event = %+v payload=%s", ev, string(ev.Payload))
	}
	calls := events.appended()
	if len(calls) != 1 || calls[0].sessionID != "ses_1" || len(calls[0].events) != 1 ||
		calls[0].events[0].Type != "session.message" {
		t.Fatalf("append calls = %+v", calls)
	}
}

func TestWebSocketServerDoesNotFanoutDurableEventWhenPersistenceFails(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	events.setAppendError(errors.New("disk full"))
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2001,
		Payload:   json.RawMessage(`{"role":"agent"}`),
	})

	errFrame := readFrame(t, adapter).(*protocol.Error)
	if errFrame.Code != "persist_failed" || errFrame.Fatal {
		t.Fatalf("adapter error = %+v", errFrame)
	}
	if frame, err := readFrameWithin(client, 80*time.Millisecond); err == nil {
		t.Fatalf("client unexpectedly received frame %+v", frame)
	}
}

func TestWebSocketServerBroadcastsEphemeralEventWithoutPersistence(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "log.tail",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"line":"hello"}`),
	})

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Seq != nil || ev.Type != "log.tail" || ev.Time != 2002 || string(ev.Payload) != `{"line":"hello"}` {
		t.Fatalf("ephemeral fanout event = %+v payload=%s", ev, string(ev.Payload))
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("ephemeral event was persisted: %+v", calls)
	}
}

func TestWebSocketServerBroadcastsEphemeralEventWithoutEventStore(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events))
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "log.tail",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"line":"hello"}`),
	})

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Type != "log.tail" || ev.Seq != nil {
		t.Fatalf("ephemeral fanout event = %+v", ev)
	}
}

func TestWebSocketServerPersistsAndRoutesClientSessionSend(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	observer := dialWebSocket(t, server.URL)
	defer observer.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeClientHello(t, observer, "client-token", 0)
	_ = readFrame(t, observer).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_send_1",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"Continue"}]}`),
	})

	observerEvent := readFrame(t, observer).(*protocol.Event)
	if observerEvent.Type != "session.message" || observerEvent.Seq == nil || *observerEvent.Seq != 1 {
		t.Fatalf("observer event = %+v", observerEvent)
	}
	var message struct {
		MessageID string `json:"message_id"`
		Role      string `json:"role"`
		Content   []struct {
			Kind string `json:"kind"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(observerEvent.Payload, &message); err != nil {
		t.Fatalf("decode session.message payload: %v", err)
	}
	if message.MessageID != "cmd_send_1" || message.Role != "user" ||
		len(message.Content) != 1 || message.Content[0].Text != "Continue" {
		t.Fatalf("session.message payload = %+v", message)
	}

	routed := readFrame(t, adapter).(*protocol.Command)
	if routed.CommandID != "cmd_send_1" || routed.Type != protocol.CommandSessionSend ||
		string(routed.Payload) != `{"content":[{"kind":"text","text":"Continue"}]}` {
		t.Fatalf("routed command = %+v payload=%s", routed, string(routed.Payload))
	}

	ack := readCommandAckFor(t, client, "cmd_send_1")
	if ack.Status != protocol.AckAccepted || ack.Reason != "" {
		t.Fatalf("client ack = %+v", ack)
	}
	calls := events.appended()
	if len(calls) != 1 || calls[0].sessionID != "ses_1" ||
		calls[0].events[0].Type != "session.message" {
		t.Fatalf("append calls = %+v", calls)
	}
}

func TestWebSocketServerRejectsUnauthorizedClientCommand(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "view-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_unauthorized",
		Type:      protocol.CommandSessionInterrupt,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{}`),
	})

	ack := readFrame(t, client).(*protocol.CommandAck)
	if ack.CommandID != "cmd_unauthorized" || ack.Status != protocol.AckRejected || ack.Reason != "unauthorized" {
		t.Fatalf("client ack = %+v", ack)
	}
}

func TestWebSocketServerPersistsPermissionDecisionAndDeduplicatesRequest(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	observer := dialWebSocket(t, server.URL)
	defer observer.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeClientHello(t, observer, "client-token", 0)
	_ = readFrame(t, observer).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	decisionPayload := json.RawMessage(`{"request_id":"pr_1","decision":"approve","decided_by":"usr_1","note":""}`)
	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_decide_1",
		Type:      protocol.CommandPermissionRespond,
		SessionID: "ses_1",
		Payload:   decisionPayload,
	})

	decision := readFrame(t, observer).(*protocol.Event)
	if decision.Type != "permission.decision" || decision.Seq == nil || *decision.Seq != 1 ||
		string(decision.Payload) != string(decisionPayload) {
		t.Fatalf("permission decision event = %+v payload=%s", decision, string(decision.Payload))
	}
	routed := readFrame(t, adapter).(*protocol.Command)
	if routed.CommandID != "cmd_decide_1" || routed.Type != protocol.CommandPermissionRespond ||
		string(routed.Payload) != string(decisionPayload) {
		t.Fatalf("routed permission command = %+v payload=%s", routed, string(routed.Payload))
	}
	if ack := readCommandAckFor(t, client, "cmd_decide_1"); ack.Status != protocol.AckAccepted {
		t.Fatalf("permission ack = %+v", ack)
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_decide_2",
		Type:      protocol.CommandPermissionRespond,
		SessionID: "ses_1",
		Payload:   decisionPayload,
	})
	duplicate := readFrame(t, client).(*protocol.CommandAck)
	if duplicate.CommandID != "cmd_decide_2" || duplicate.Status != protocol.AckDuplicate {
		t.Fatalf("duplicate decision ack = %+v", duplicate)
	}
	if frame, err := readFrameWithin(adapter, 80*time.Millisecond); err == nil {
		t.Fatalf("adapter unexpectedly received duplicate decision %+v", frame)
	}
	calls := events.appended()
	if len(calls) != 1 || calls[0].events[0].Type != "permission.decision" {
		t.Fatalf("append calls = %+v", calls)
	}
}

func TestWebSocketServerDeduplicatesAcceptedClientCommands(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	command := &protocol.Command{
		CommandID: "cmd_duplicate",
		Type:      protocol.CommandSessionInterrupt,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{}`),
	}
	writeFrame(t, client, command)
	first := readFrame(t, adapter).(*protocol.Command)
	if first.CommandID != "cmd_duplicate" {
		t.Fatalf("first routed command = %+v", first)
	}
	if ack := readFrame(t, client).(*protocol.CommandAck); ack.Status != protocol.AckAccepted {
		t.Fatalf("first ack = %+v", ack)
	}

	writeFrame(t, client, command)
	duplicate := readFrame(t, client).(*protocol.CommandAck)
	if duplicate.CommandID != "cmd_duplicate" || duplicate.Status != protocol.AckDuplicate {
		t.Fatalf("duplicate ack = %+v", duplicate)
	}
	if frame, err := readFrameWithin(adapter, 80*time.Millisecond); err == nil {
		t.Fatalf("adapter unexpectedly received duplicate frame %+v", frame)
	}
}

func TestWebSocketServerBuffersSessionSendUntilAdapterReconnects(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_buffered",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"Buffered"}]}`),
	})

	ack := readCommandAckFor(t, client, "cmd_buffered")
	if ack.Status != protocol.AckAccepted {
		t.Fatalf("client ack = %+v", ack)
	}

	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	routed := readFrame(t, adapter).(*protocol.Command)
	if routed.CommandID != "cmd_buffered" || routed.Type != protocol.CommandSessionSend {
		t.Fatalf("buffered command = %+v", routed)
	}
}

func TestWebSocketServerBuffersLiveEventsUntilReplayCompletes(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 1}, map[string][]store.Event{
		"ses_1": {
			{SessionID: "ses_1", Seq: 1, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)},
		},
	})
	replayStarted := make(chan struct{})
	releaseReplay := make(chan struct{})
	events.onReplayEvent = func() {
		close(replayStarted)
		<-releaseReplay
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	replayed := readFrame(t, client).(*protocol.Event)
	if replayed.Seq == nil || *replayed.Seq != 1 {
		t.Fatalf("replayed event = %+v", replayed)
	}
	<-replayStarted

	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"n":2}`),
	})
	close(releaseReplay)

	live := readFrame(t, client).(*protocol.Event)
	if live.Seq == nil || *live.Seq != 2 || string(live.Payload) != `{"n":2}` {
		t.Fatalf("live event after replay = %+v payload=%s", live, string(live.Payload))
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

func writeClientHello(t *testing.T, conn *websocket.Conn, token string, lastSeq int64) {
	t.Helper()

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           token,
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1", LastSeq: lastSeq}},
	})
}

func writeAdapterHello(t *testing.T, conn *websocket.Conn, token string) {
	t.Helper()

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleAdapter,
		Token:           token,
		SessionID:       "ses_1",
		Provider:        "claude-code",
		Resume:          true,
	})
}

func readFrame(t *testing.T, conn *websocket.Conn) protocol.Frame {
	t.Helper()

	frame, err := readFrameWithin(conn, time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame
}

func readFrameWithin(conn *websocket.Conn, timeout time.Duration) (protocol.Frame, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, errors.New("websocket message type is not text")
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		return nil, err
	}
	return frame, nil
}

func readCommandAckFor(t *testing.T, conn *websocket.Conn, commandID string) *protocol.CommandAck {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for command ack %s", commandID)
		default:
		}
		frame := readFrame(t, conn)
		if ack, ok := frame.(*protocol.CommandAck); ok && ack.CommandID == commandID {
			return ack
		}
	}
}

func testHandshake() *hub.Handshake {
	return testHandshakeWithStore(newFakeEventStore(map[string]int64{"ses_1": 7}, nil))
}

func testHandshakeWithStore(events store.EventStore) *hub.Handshake {
	return hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: websocketTestAuth{
			principals: map[string]auth.Principal{
				"client-token": {
					Subject: "client",
					Scopes:  []auth.Scope{auth.SessionControl("ses_1")},
				},
				"view-token": {
					Subject: "viewer",
					Scopes:  []auth.Scope{auth.SessionView("ses_1")},
				},
				"adapter-token": {
					Subject: "adapter",
					Scopes:  []auth.Scope{auth.SessionAdapter("ses_1")},
				},
			},
		},
		EventStore:    events,
		SessionLookup: fakeSessions{"ses_1": {State: "ready", Provider: "claude-code"}},
	})
}

type websocketTestAuth struct {
	principals map[string]auth.Principal
}

func (a websocketTestAuth) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	principal, ok := a.principals[token]
	if !ok {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return principal, nil
}

func (a websocketTestAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}

type fakeEventStore struct {
	mu            sync.Mutex
	latest        map[string]int64
	events        map[string][]store.Event
	appendErr     error
	appendCalls   []appendCall
	onReplayEvent func()
}

type appendCall struct {
	sessionID string
	events    []store.PendingEvent
}

func newFakeEventStore(latest map[string]int64, events map[string][]store.Event) *fakeEventStore {
	if latest == nil {
		latest = make(map[string]int64)
	}
	if events == nil {
		events = make(map[string][]store.Event)
	}
	return &fakeEventStore{latest: latest, events: events}
}

func (f *fakeEventStore) setAppendError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appendErr = err
}

func (f *fakeEventStore) appended() []appendCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := make([]appendCall, len(f.appendCalls))
	copy(copied, f.appendCalls)
	return copied
}

func (f *fakeEventStore) Append(_ context.Context, sessionID string, evs []store.PendingEvent) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	firstSeq := f.latest[sessionID] + 1
	pending := make([]store.PendingEvent, len(evs))
	copy(pending, evs)
	f.appendCalls = append(f.appendCalls, appendCall{sessionID: sessionID, events: pending})
	for i, ev := range evs {
		seq := firstSeq + int64(i)
		f.events[sessionID] = append(f.events[sessionID], store.Event{
			SessionID: sessionID,
			Seq:       seq,
			Type:      ev.Type,
			Time:      ev.Time,
			Payload:   append(json.RawMessage(nil), ev.Payload...),
		})
		f.latest[sessionID] = seq
	}
	return firstSeq, nil
}

func (f *fakeEventStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.latest[sessionID], nil
}

func (f *fakeEventStore) Replay(_ context.Context, sessionID string, afterSeq int64, fn func(store.Event) error) error {
	f.mu.Lock()
	events := append([]store.Event(nil), f.events[sessionID]...)
	onReplayEvent := f.onReplayEvent
	f.mu.Unlock()
	for _, ev := range events {
		if ev.Seq <= afterSeq {
			continue
		}
		if err := fn(ev); err != nil {
			return err
		}
		if onReplayEvent != nil {
			onReplayEvent()
		}
	}
	return nil
}
