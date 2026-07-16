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

func TestWebSocketServerNegotiatesV2WithoutHistoryCapability(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
	})
	ack := readFrame(t, conn).(*protocol.HelloAck)
	if ack.ProtocolVersion != protocol.ProtocolVersionV2 || ack.Capabilities != nil {
		t.Fatalf("v2 hello ack = %+v, want v2 with no capabilities", ack)
	}
	writeFrame(t, conn, &protocol.HistoryPageRequest{RequestID: "hist_unready", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, conn).(*protocol.Error); got.Code != "history_unsupported" {
		t.Fatalf("unready history error = %+v", got)
	}
}

func TestWebSocketServerAdvertisesAndServesAuthorizedHistory(t *testing.T) {
	t.Parallel()

	before := int64(58)
	next := int64(52)
	events := &fakeHistoryStore{
		fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 57}, nil),
		page: store.HistoryPage{
			Events:    []store.Event{{SessionID: "ses_1", Seq: 52, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)}},
			LatestSeq: 57, NextBeforeSeq: &next, RetentionState: store.RetentionGap,
		},
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "view-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
	})
	ack := readFrame(t, client).(*protocol.HelloAck)
	if ack.Capabilities == nil || ack.Capabilities.HistoryPage == nil || ack.Capabilities.HistoryPage.MaxLimit != 100 {
		t.Fatalf("history capability = %+v", ack.Capabilities)
	}
	writeFrame(t, client, &protocol.HistoryPageRequest{
		RequestID: "hist_1", SessionID: "ses_1", BeforeSeq: &before, Limit: 1,
	})
	response := readFrame(t, client).(*protocol.HistoryPageResponse)
	if response.RequestID != "hist_1" || response.SessionID != "ses_1" || response.LatestSeq != 57 ||
		response.NextBeforeSeq == nil || *response.NextBeforeSeq != next || len(response.Events) != 1 ||
		response.Events[0].Frame != protocol.FrameEvent || response.Events[0].Seq != 52 || response.RetentionState != store.RetentionGap {
		t.Fatalf("history response = %+v", response)
	}
	call := events.historyCall(t)
	if call.sessionID != "ses_1" || call.beforeSeq == nil || *call.beforeSeq != before || call.limit != 1 {
		t.Fatalf("history call = %+v", call)
	}

	control := dialWebSocket(t, server.URL)
	defer control.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, control, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
	})
	_ = readFrame(t, control).(*protocol.HelloAck)
	writeFrame(t, control, &protocol.HistoryPageRequest{RequestID: "hist_control", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, control).(*protocol.HistoryPageResponse); got.RequestID != "hist_control" {
		t.Fatalf("control history response = %+v", got)
	}
	if calls := events.historyCalls(); calls != 2 {
		t.Fatalf("history calls = %d, want 2", calls)
	}
}

func TestWebSocketServerHistoryAuthorizationAndAvailability(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		version    int
		token      string
		sessionID  string
		storeError error
		wantCode   string
	}{
		{name: "v1 unsupported", version: 1, token: "view-token", sessionID: "ses_1", wantCode: "history_unsupported"},
		{name: "api wildcard denied", version: 2, token: "api-token", sessionID: "ses_1", wantCode: "history_unavailable"},
		{name: "cross session denied", version: 2, token: "view-token", sessionID: "ses_2", wantCode: "history_unavailable"},
		{name: "store failure", version: 2, token: "view-token", sessionID: "ses_1", storeError: errors.New("private store failure"), wantCode: "history_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 0}, nil), err: test.storeError}
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
				cfg.EventStore = events
			})
			client := dialWebSocket(t, server.URL)
			defer client.Close(websocket.StatusNormalClosure, "")
			writeFrame(t, client, &protocol.Hello{
				ProtocolVersion: test.version, Role: protocol.RoleClient, Token: test.token,
				Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
			})
			_ = readFrame(t, client).(*protocol.HelloAck)
			writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_denied", SessionID: test.sessionID, Limit: 1})
			got := readFrame(t, client).(*protocol.Error)
			if got.Code != test.wantCode || got.Message == "private store failure" {
				t.Fatalf("history error = %+v", got)
			}
			if test.name != "store failure" && events.historyCalls() != 0 {
				t.Fatalf("denied history reached store")
			}
			writeFrame(t, client, &protocol.Ping{Nonce: "after-history-error"})
			if pong := readFrame(t, client).(*protocol.Pong); pong.Nonce != "after-history-error" {
				t.Fatalf("pong after history error = %+v", pong)
			}
		})
	}
}

func TestWebSocketServerRejectsUntrustedHistoryPages(t *testing.T) {
	t.Parallel()
	event := func(sessionID string, seq int64) store.Event {
		return store.Event{SessionID: sessionID, Seq: seq, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)}
	}
	nextTwo := int64(2)
	for _, test := range []struct {
		name   string
		page   store.HistoryPage
		before *int64
	}{
		{name: "cross session", page: store.HistoryPage{Events: []store.Event{event("ses_2", 1)}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "oversized", page: store.HistoryPage{Events: []store.Event{event("ses_1", 1), event("ses_1", 2)}, LatestSeq: 2, RetentionState: store.RetentionComplete}},
		{name: "nonpositive sequence", page: store.HistoryPage{Events: []store.Event{event("ses_1", 0)}, RetentionState: store.RetentionComplete}},
		{name: "descending sequence", page: store.HistoryPage{Events: []store.Event{event("ses_1", 2), event("ses_1", 1)}, LatestSeq: 2, RetentionState: store.RetentionComplete}},
		{name: "cursor violation", page: store.HistoryPage{Events: []store.Event{event("ses_1", 2)}, LatestSeq: 2, RetentionState: store.RetentionComplete}, before: &nextTwo},
		{name: "latest below event", page: store.HistoryPage{Events: []store.Event{event("ses_1", 2)}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "inconsistent next", page: store.HistoryPage{Events: []store.Event{event("ses_1", 1)}, LatestSeq: 2, NextBeforeSeq: &nextTwo, RetentionState: store.RetentionComplete}},
		{name: "invalid retention", page: store.HistoryPage{LatestSeq: 1, RetentionState: "unknown"}},
		{name: "invalid payload", page: store.HistoryPage{Events: []store.Event{{SessionID: "ses_1", Seq: 1, Type: "x", Payload: json.RawMessage(`{`)}}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 2}, nil), page: test.page}
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
			client := dialWebSocket(t, server.URL)
			defer client.Close(websocket.StatusNormalClosure, "")
			writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "view-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
			_ = readFrame(t, client).(*protocol.HelloAck)
			writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_bad", SessionID: "ses_1", BeforeSeq: test.before, Limit: 1})
			if got := readFrame(t, client).(*protocol.Error); got.Code != "history_unavailable" {
				t.Fatalf("history error = %+v", got)
			}
		})
	}
}

func TestWebSocketServerReauthenticatesHistoryBeforeAndAfterStore(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		validCalls int
		storeCalls int
	}{
		{name: "expired before request", validCalls: 1, storeCalls: 0},
		{name: "revoked during read", validCalls: 2, storeCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := &fakeHistoryStore{
				fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 1}, nil),
				page:           store.HistoryPage{Events: []store.Event{{SessionID: "ses_1", Seq: 1, Type: "x", Payload: json.RawMessage(`{}`)}}, LatestSeq: 1, RetentionState: store.RetentionComplete},
			}
			authenticator := &boundedWebsocketAuth{validCalls: test.validCalls, principal: auth.Principal{Subject: "viewer", Scopes: []auth.Scope{auth.SessionView("ses_1")}}}
			handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: authenticator, EventStore: events, SessionLookup: fakeSessions{"ses_1": {State: "ready"}}})
			server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
			client := dialWebSocket(t, server.URL)
			defer client.Close(websocket.StatusNormalClosure, "")
			writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "expiring-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
			_ = readFrame(t, client).(*protocol.HelloAck)
			writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_expired", SessionID: "ses_1", Limit: 1})
			if got := readFrame(t, client).(*protocol.Error); got.Code != "history_unavailable" || events.historyCalls() != test.storeCalls {
				t.Fatalf("history error = %+v, store calls = %d", got, events.historyCalls())
			}
		})
	}
}

func TestWebSocketServerRejectsAdapterHistoryRequest(t *testing.T) {
	t.Parallel()

	events := &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 0}, nil)}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	ack := readFrame(t, adapter).(*protocol.HelloAck)
	if ack.Capabilities != nil {
		t.Fatalf("adapter capabilities = %+v", ack.Capabilities)
	}
	writeFrame(t, adapter, &protocol.HistoryPageRequest{RequestID: "hist_adapter", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, adapter).(*protocol.Error); got.Code != "history_unsupported" {
		t.Fatalf("adapter history error = %+v", got)
	}
	if events.historyCalls() != 0 {
		t.Fatal("adapter history request reached store")
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

func TestWebSocketServerDoesNotReplayV2IdleWarning(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 2}, map[string][]store.Event{
		"ses_1": {
			{SessionID: "ses_1", Seq: 1, Type: "x.vm.idle_warning", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{}`)},
			{SessionID: "ses_1", Seq: 2, Type: "session.message", Time: time.UnixMilli(1002), Payload: json.RawMessage(`{}`)},
		},
	})
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)
	if event := readFrame(t, client).(*protocol.Event); event.Type != "session.message" || event.Seq == nil || *event.Seq != 2 {
		t.Fatalf("replayed event = %+v, want session.message seq=2", event)
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

func TestWebSocketServerBatchesAdapterDurableEvents(t *testing.T) {
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
		Payload:   json.RawMessage(`{"n":1}`),
	})
	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"n":2}`),
	})

	first := readFrame(t, client).(*protocol.Event)
	second := readFrame(t, client).(*protocol.Event)
	if first.Seq == nil || *first.Seq != 1 || string(first.Payload) != `{"n":1}` {
		t.Fatalf("first fanout event = %+v payload=%s", first, string(first.Payload))
	}
	if second.Seq == nil || *second.Seq != 2 || string(second.Payload) != `{"n":2}` {
		t.Fatalf("second fanout event = %+v payload=%s", second, string(second.Payload))
	}

	calls := events.appended()
	if len(calls) != 1 || calls[0].sessionID != "ses_1" || len(calls[0].events) != 2 {
		t.Fatalf("append calls = %+v, want one batch of two events", calls)
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

func TestWebSocketServerEmitsIdleWarningFromHostWithoutPersistence(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{
		Handshake:  testHandshakeWithStore(events),
		EventStore: events,
	})
	broadcaster, ok := handler.(hub.EphemeralBroadcaster)
	if !ok {
		t.Fatalf("handler does not implement EphemeralBroadcaster")
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)

	payload := json.RawMessage(`{"vm_id":"vm_1","seconds_until_suspend":600,"suspend_at":"2026-06-21T12:30:00Z","message":"This VM will pause soon because it has been idle."}`)
	if err := broadcaster.EmitEphemeralEvent(context.Background(), protocol.Event{
		Type:      "session.idle_warning",
		SessionID: "ses_1",
		Time:      2003,
		Payload:   payload,
	}); err != nil {
		t.Fatalf("emit idle warning: %v", err)
	}

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Seq != nil || ev.Type != "session.idle_warning" || ev.Time != 2003 ||
		string(ev.Payload) != string(payload) {
		t.Fatalf("idle warning event = %+v payload=%s", ev, string(ev.Payload))
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("idle warning event was persisted: %+v", calls)
	}
}

func TestWebSocketServerDoesNotSendV1IdleWarningToV2Client(t *testing.T) {
	t.Parallel()

	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: testHandshake()})
	broadcaster := handler.(hub.EphemeralBroadcaster)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	v1Client := dialWebSocket(t, server.URL)
	defer v1Client.Close(websocket.StatusNormalClosure, "")
	v2Client := dialWebSocket(t, server.URL)
	defer v2Client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, v1Client, "client-token", 0)
	_ = readFrame(t, v1Client).(*protocol.HelloAck)
	writeFrame(t, v2Client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
	})
	_ = readFrame(t, v2Client).(*protocol.HelloAck)

	if err := broadcaster.EmitEphemeralEvent(context.Background(), protocol.Event{
		Type: "session.idle_warning", SessionID: "ses_1", Time: 2003,
		Payload: json.RawMessage(`{"message":"legacy warning"}`),
	}); err != nil {
		t.Fatalf("emit v1 idle warning: %v", err)
	}
	if frame := readFrame(t, v1Client).(*protocol.Event); frame.Type != "session.idle_warning" {
		t.Fatalf("v1 client event type = %q, want session.idle_warning", frame.Type)
	}
	if frame, err := readFrameWithin(v2Client, 100*time.Millisecond); err == nil {
		t.Fatalf("v2 client received v1-only event: %+v", frame)
	}
}

func TestWebSocketServerRejectsVersionOwnedWarningFromAdapter(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type: "x.vm.idle_warning", SessionID: "ses_1", Time: 2003, Payload: json.RawMessage(`{}`),
	})
	if frame := readFrame(t, adapter).(*protocol.Error); frame.Code != "invalid_event" {
		t.Fatalf("adapter error = %+v, want invalid_event", frame)
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("version-owned warning was persisted: %+v", calls)
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

func TestWebSocketServerReportsAcceptedClientCommandActivity(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	observer := &recordingCommandActivityObserver{}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
		cfg.CommandActivityObserver = observer
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_activity",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"hello"}]}`),
	})
	ack := readCommandAckFor(t, client, "cmd_activity")
	if ack.Status != protocol.AckAccepted {
		t.Fatalf("ack = %+v", ack)
	}
	if len(observer.got) != 1 {
		t.Fatalf("observed activity count = %d, want 1", len(observer.got))
	}
	got := observer.got[0]
	if got.SessionID != "ses_1" || got.CommandID != "cmd_activity" || got.Type != protocol.CommandSessionSend ||
		got.DurableSeq == nil || *got.DurableSeq != 1 || got.At.IsZero() {
		t.Fatalf("observed activity = %+v", got)
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_rejected",
		Type:      protocol.CommandSessionInterrupt,
		SessionID: "ses_other",
		Payload:   json.RawMessage(`{}`),
	})
	rejected := readCommandAckFor(t, client, "cmd_rejected")
	if rejected.Status != protocol.AckRejected || rejected.Reason != "unauthorized" {
		t.Fatalf("rejected ack = %+v", rejected)
	}
	if len(observer.got) != 1 {
		t.Fatalf("observed rejected command activity = %+v", observer.got)
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_activity",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"hello"}]}`),
	})
	duplicate := readCommandAckFor(t, client, "cmd_activity")
	if duplicate.Status != protocol.AckDuplicate {
		t.Fatalf("duplicate ack = %+v", duplicate)
	}
	if len(observer.got) != 1 {
		t.Fatalf("observed duplicate command activity = %+v", observer.got)
	}
}

func TestWebSocketServerReportsAdapterActivity(t *testing.T) {
	t.Parallel()

	observer := &recordingAdapterActivityObserver{}
	server := newWebSocketTestServer(t, testHandshake(), func(cfg *hub.WebSocketConfig) {
		cfg.AdapterActivityObserver = observer
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	if got := waitAdapterActivityCount(t, observer, 1); len(got) != 1 || got[0].SessionID != "ses_1" || got[0].At.IsZero() {
		t.Fatalf("hello adapter activity = %+v", got)
	}

	writeFrame(t, adapter, &protocol.Ping{Nonce: "adapter-ping"})
	pong := readFrame(t, adapter).(*protocol.Pong)
	if pong.Nonce != "adapter-ping" {
		t.Fatalf("pong nonce = %q", pong.Nonce)
	}
	if got := waitAdapterActivityCount(t, observer, 2); len(got) != 2 || got[1].SessionID != "ses_1" || got[1].At.IsZero() {
		t.Fatalf("ping adapter activity = %+v", got)
	}

	writeFrame(t, adapter, &protocol.Event{
		Type:      "log.tail",
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"line":"ready"}`),
	})
	if got := waitAdapterActivityCount(t, observer, 3); got[2].SessionID != "ses_1" || got[2].At.IsZero() {
		t.Fatalf("event adapter activity = %+v", got)
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
				"api-token": {
					Subject: "api",
					Scopes:  []auth.Scope{auth.API()},
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

type boundedWebsocketAuth struct {
	mu         sync.Mutex
	validCalls int
	calls      int
	principal  auth.Principal
}

func (a *boundedWebsocketAuth) Authenticate(_ context.Context, _ string) (auth.Principal, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls > a.validCalls {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return a.principal, nil
}

func (a *boundedWebsocketAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}

type recordingCommandActivityObserver struct {
	got []hub.CommandActivity
}

func (o *recordingCommandActivityObserver) ObserveCommandActivity(_ context.Context, activity hub.CommandActivity) {
	o.got = append(o.got, activity)
}

type recordingAdapterActivityObserver struct {
	mu  sync.Mutex
	got []hub.AdapterActivity
}

func (o *recordingAdapterActivityObserver) ObserveAdapterActivity(_ context.Context, activity hub.AdapterActivity) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.got = append(o.got, activity)
}

func (o *recordingAdapterActivityObserver) activities() []hub.AdapterActivity {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]hub.AdapterActivity(nil), o.got...)
}

func waitAdapterActivityCount(t *testing.T, observer *recordingAdapterActivityObserver, want int) []hub.AdapterActivity {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got := observer.activities()
		if len(got) >= want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return observer.activities()
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

type historyCall struct {
	sessionID string
	beforeSeq *int64
	limit     int
}

type fakeHistoryStore struct {
	*fakeEventStore
	mu    sync.Mutex
	page  store.HistoryPage
	err   error
	calls []historyCall
}

func (f *fakeHistoryStore) History(_ context.Context, sessionID string, beforeSeq *int64, limit int) (store.HistoryPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := historyCall{sessionID: sessionID, limit: limit}
	if beforeSeq != nil {
		value := *beforeSeq
		call.beforeSeq = &value
	}
	f.calls = append(f.calls, call)
	return f.page, f.err
}

func (f *fakeHistoryStore) historyCall(t *testing.T) historyCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 1 {
		t.Fatalf("history calls = %d, want 1", len(f.calls))
	}
	return f.calls[0]
}

func (f *fakeHistoryStore) historyCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
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
