package hub_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"nhooyr.io/websocket"
)

type bootstrapTestStore struct {
	*fakeHistoryStore
	replays atomic.Int32
	started chan int64
	resume  chan struct{}
}

func (s *bootstrapTestStore) Replay(ctx context.Context, id string, after int64, fn func(store.Event) error) error {
	s.replays.Add(1)
	return s.fakeEventStore.Replay(ctx, id, after, fn)
}

func (s *bootstrapTestStore) Bootstrap(ctx context.Context, id string, through int64) ([]store.Event, error) {
	s.started <- through
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.resume:
	}
	s.fakeEventStore.mu.Lock()
	defer s.fakeEventStore.mu.Unlock()
	var latestState *store.Event
	for _, event := range s.events[id] {
		if event.Seq <= through && event.Type == "session.state" {
			copy := event
			latestState = &copy
		}
	}
	if latestState == nil {
		return nil, nil
	}
	return []store.Event{*latestState}, nil
}

type advancingBootstrapStore struct {
	*bootstrapTestStore
	reads atomic.Int32
}

func (s *advancingBootstrapStore) LatestSeq(ctx context.Context, id string) (int64, error) {
	latest, err := s.fakeEventStore.LatestSeq(ctx, id)
	if s.reads.Add(1) == 1 && err == nil {
		_, err = s.Append(ctx, id, []store.PendingEvent{{Type: "session.state", Time: time.UnixMilli(latest + 1), Payload: json.RawMessage(`{"state":"busy"}`)}})
	}
	return latest, err
}

func TestTailSubscriptionCapturesControlUpdateBeforeRegistration(t *testing.T) {
	bootstrap := &bootstrapTestStore{fakeHistoryStore: &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 10_000}, nil)}, started: make(chan int64, 1), resume: make(chan struct{})}
	events := &advancingBootstrapStore{bootstrapTestStore: bootstrap}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1", SkipReplay: true}}})
	ack := readFrame(t, client).(*protocol.HelloAck)
	if ack.Sessions[0].LatestSeq != 10_001 || ack.Sessions[0].ReplayFrom != 10_002 {
		t.Fatalf("final watermark = %+v", ack.Sessions[0])
	}
	close(bootstrap.resume)
	event := readFrame(t, client).(*protocol.Event)
	if event.Type != "session.state" || *event.Seq != 10_001 {
		t.Fatalf("updated control state = %+v", event)
	}
	if bootstrap.replays.Load() != 0 {
		t.Fatal("tail bootstrap replayed full history")
	}
}

func TestTailSubscriptionSkipsReplayAndPreservesBufferedLiveEvents(t *testing.T) {
	events := &bootstrapTestStore{
		fakeHistoryStore: &fakeHistoryStore{
			fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 10_000}, map[string][]store.Event{"ses_1": {{SessionID: "ses_1", Seq: 2, Type: "session.state", Time: time.UnixMilli(2), Payload: json.RawMessage(`{"state":"ready"}`)}}}),
			page: store.HistoryPage{LatestSeq: 10_000, RetentionState: store.RetentionComplete, Events: []store.Event{
				{SessionID: "ses_1", Seq: 10_000, Type: "session.message", Time: time.UnixMilli(10_000), Payload: json.RawMessage(`{"role":"agent"}`)},
			}},
		},
		started: make(chan int64, 1), resume: make(chan struct{}),
	}
	next := int64(10_000)
	events.page.NextBeforeSeq = &next
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1", SkipReplay: true}}})
	ack := readFrame(t, client).(*protocol.HelloAck)
	if ack.Sessions[0].ReplayFrom != 10_001 || ack.Capabilities.HistoryPage == nil {
		t.Fatalf("hello = %+v", ack)
	}
	select {
	case through := <-events.started:
		if through != 10_000 {
			t.Fatalf("bootstrap watermark = %d", through)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap did not start")
	}
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.Event{Type: "session.message", SessionID: "ses_1", Time: 20_001, Payload: json.RawMessage(`{"role":"agent"}`)})
	close(events.resume)
	state := readFrame(t, client).(*protocol.Event)
	if state.Type != "session.state" || *state.Seq != 2 {
		t.Fatalf("bootstrap event = %+v", state)
	}
	live := readFrame(t, client).(*protocol.Event)
	if live.Type != "session.message" || *live.Seq != 10_001 {
		t.Fatalf("live event = %+v", live)
	}
	writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "tail", SessionID: "ses_1", Limit: 1})
	page := readFrame(t, client).(*protocol.HistoryPageResponse)
	if len(page.Events) != 1 || page.Events[0].Seq != 10_000 || events.replays.Load() != 0 {
		t.Fatalf("page = %+v, replay calls = %d", page, events.replays.Load())
	}
}

func TestTailSubscriptionRejectsStoreWithoutBoundedBootstrap(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 1}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1", SkipReplay: true}}})
	frame := readFrame(t, client).(*protocol.Error)
	if frame.Code != "history_unsupported" {
		t.Fatalf("error = %+v", frame)
	}
}
