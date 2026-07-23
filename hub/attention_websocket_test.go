package hub_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
	"nhooyr.io/websocket"
)

func TestWebSocketAttentionSubscriptionReadsOnlyStoreSummary(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_attention", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_attention"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}}, EventStore: events})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	ack, ok := readFrame(t, conn).(*protocol.HelloAck)
	if !ok || len(ack.Sessions) != 0 || ack.Capabilities == nil || ack.Capabilities.AttentionSummary == nil || ack.Capabilities.AttentionSummary.MaxSessions != 64 {
		t.Fatalf("attention hello.ack = %+v", ack)
	}
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_1"})
	snapshot, ok := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if !ok || snapshot.Kind != "snapshot" || len(snapshot.Summaries) != 1 || snapshot.Summaries[0].SessionID != "ses_attention" || snapshot.Summaries[0].LatestSeq != 1 {
		t.Fatalf("attention snapshot = %+v", snapshot)
	}
	writeFrame(t, conn, &protocol.HistoryPageRequest{RequestID: "history_1", SessionID: "ses_attention", Limit: 1})
	if frame, ok := readFrame(t, conn).(*protocol.Error); !ok || frame.Code != "unsupported_frame" {
		t.Fatalf("attention history response = %#v", frame)
	}
}

func TestWebSocketAttentionSubscriptionClosesAtGrantExpiry(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-expiry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_attention"}, MaxSessions: 1, ExpiresAt: time.Now().Add(40 * time.Millisecond)}}, EventStore: events})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_expiry"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if _, err := readFrameWithin(conn, time.Second); err == nil {
		t.Fatal("expired attention subscription remained open")
	}
}

func TestWebSocketAttentionUpdateIsScopedAndGrantChangeClosesMembership(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-membership.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_one", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	authorizer := &mutableAttentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_one", "ses_missing"}, MaxSessions: 2, ExpiresAt: time.Now().Add(time.Minute)}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: authorizer, EventStore: events})
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: events})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_membership"})
	snapshot := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if snapshot.SubscriptionState != "incomplete" || len(snapshot.Summaries) != 2 {
		t.Fatalf("initial incomplete snapshot = %+v", snapshot)
	}
	if _, err := events.Append(ctx, "ses_one", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"busy"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := handler.EmitEphemeralEvent(ctx, protocol.Event{Type: "agent.activity", SessionID: "ses_one", Time: time.Now().UTC().UnixMilli(), Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	update := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if update.Kind != "update" || len(update.Summaries) != 1 || update.Summaries[0].SessionID != "ses_one" || update.Summaries[0].LatestSeq != 2 {
		t.Fatalf("scoped update = %+v", update)
	}
	authorizer.setGrant(auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_missing"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)})
	if err := handler.EmitEphemeralEvent(ctx, protocol.Event{Type: "agent.activity", SessionID: "ses_one", Time: time.Now().UTC().UnixMilli(), Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameWithin(conn, time.Second); err == nil {
		t.Fatal("changed grant left old attention membership connected")
	}
}

func TestWebSocketAttentionExpiryChangeClosesMembership(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-expiry-change.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_one", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	authorizer := &mutableAttentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_one"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: authorizer, EventStore: events})
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: events})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_expiry_change"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)

	authorizer.setGrant(auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_one"}, MaxSessions: 1, ExpiresAt: time.Now().Add(2 * time.Minute)})
	if err := handler.EmitEphemeralEvent(ctx, protocol.Event{Type: "agent.activity", SessionID: "ses_one", Time: time.Now().UTC().UnixMilli(), Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameWithin(conn, time.Second); err == nil {
		t.Fatal("expiry-only authorization change left old membership connected")
	}
}

func TestWebSocketAttentionConcurrentFanoutDeduplicatesWithoutRace(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-concurrent-fanout.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_one", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_one"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}}, EventStore: events})
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: events})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_concurrent"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if _, err := events.Append(ctx, "ses_one", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"busy"}`)}}); err != nil {
		t.Fatal(err)
	}

	const fanouts = 32
	start := make(chan struct{})
	errCh := make(chan error, fanouts)
	var wait sync.WaitGroup
	for range fanouts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errCh <- handler.EmitEphemeralEvent(ctx, protocol.Event{Type: "agent.activity", SessionID: "ses_one", Time: time.Now().UTC().UnixMilli(), Payload: []byte(`{}`)})
		}()
	}
	close(start)
	wait.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent attention fanout: %v", err)
		}
	}
	update := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if update.Kind != "update" || len(update.Summaries) != 1 || update.Summaries[0].LatestSeq != 2 {
		t.Fatalf("concurrent attention update = %+v", update)
	}
	if frame, err := readFrameWithin(conn, 50*time.Millisecond); err == nil {
		t.Fatalf("duplicate concurrent attention update = %+v", frame)
	}
}

func TestWebSocketAttentionResubscribeWaitsForInFlightFanout(t *testing.T) {
	ctx := context.Background()
	events := newAttentionActivityStore(store.SessionAttentionSummary{SessionID: "ses_one", State: "ready", SummaryVersion: 1, StateOfProjection: store.AttentionProjectionComplete})
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_one"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}}, EventStore: events})
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: events})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_old"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)

	reason := "capacity"
	events.setSummary(store.SessionAttentionSummary{SessionID: "ses_one", State: "ready", SummaryVersion: 2, StateOfProjection: store.AttentionProjectionComplete, Blocker: &store.AttentionBlocker{Kind: store.AttentionBlockerQueued, Reason: &reason}})
	calls := events.observeSnapshots()
	started, release := events.blockNextSnapshot()
	fanoutDone := make(chan error, 1)
	go func() {
		fanoutDone <- handler.EmitEphemeralEvent(ctx, protocol.Event{Type: "agent.activity", SessionID: "ses_one", Time: time.Now().UTC().UnixMilli(), Payload: []byte(`{}`)})
	}()
	<-calls
	<-started
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_new"})
	<-calls
	close(release)
	if err := <-fanoutDone; err != nil {
		t.Fatalf("blocked fanout: %v", err)
	}
	oldUpdate := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if oldUpdate.Kind != "update" || oldUpdate.RequestID != "attn_old" || len(oldUpdate.Summaries) != 1 || oldUpdate.Summaries[0].SummaryVersion != 2 {
		t.Fatalf("old update must precede replacement snapshot: %+v", oldUpdate)
	}
	newSnapshot := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if newSnapshot.Kind != "snapshot" || newSnapshot.RequestID != "attn_new" || len(newSnapshot.Summaries) != 1 || newSnapshot.Summaries[0].SummaryVersion != 2 {
		t.Fatalf("replacement snapshot = %+v", newSnapshot)
	}
	if frame, err := readFrameWithin(conn, 50*time.Millisecond); err == nil {
		t.Fatalf("stale fanout after replacement snapshot = %+v", frame)
	}
}

func TestWebSocketAttentionRenewalSurvivesOldExpiryTimer(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-renewal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_one", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	authorizer := &mutableAttentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_one"}, MaxSessions: 1, ExpiresAt: time.Now().Add(60 * time.Millisecond)}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: authorizer, EventStore: events})
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: events})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_old_expiry"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	time.Sleep(20 * time.Millisecond)
	authorizer.setGrant(auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_one"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)})
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_renewed"})
	if snapshot := readFrame(t, conn).(*protocol.AttentionSummaryFrame); snapshot.RequestID != "attn_renewed" {
		t.Fatalf("renewed snapshot = %+v", snapshot)
	}
	time.Sleep(80 * time.Millisecond)
	writeFrame(t, conn, &protocol.Ping{Nonce: "renewal-survived"})
	if pong := readFrame(t, conn).(*protocol.Pong); pong.Nonce != "renewal-survived" {
		t.Fatalf("old expiry timer closed renewed subscription: %+v", pong)
	}
}

func TestWebSocketAttentionActivityRefreshDeliversLedgerOnlyUpdate(t *testing.T) {
	ctx := context.Background()
	events := newAttentionActivityStore(store.SessionAttentionSummary{
		SessionID: "ses_ledger", State: "ready", SummaryVersion: 1, StateOfProjection: store.AttentionProjectionComplete,
	})
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_ledger"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}}, EventStore: events})
	var published []hub.ActivitySummary
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{
		Handshake:  handshake,
		EventStore: events,
		ActivitySink: hub.ActivitySinkFunc(func(_ context.Context, summary hub.ActivitySummary) error {
			published = append(published, summary)
			return nil
		}),
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_ledger"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)

	reason := "capacity"
	events.setSummary(store.SessionAttentionSummary{
		SessionID: "ses_ledger", State: "ready", SummaryVersion: 2, StateOfProjection: store.AttentionProjectionComplete,
		Blocker: &store.AttentionBlocker{Kind: store.AttentionBlockerQueued, Reason: &reason},
	})
	if err := handler.RequestActivityRefresh(ctx); err != nil {
		t.Fatalf("ledger activity refresh: %v", err)
	}
	update := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if update.Kind != "update" || len(update.Summaries) != 1 || update.Summaries[0].SummaryVersion != 2 || update.Summaries[0].Blocker == nil || update.Summaries[0].Blocker.Kind != "queued" {
		t.Fatalf("ledger-only attention update = %+v", update)
	}
	if len(published) != 1 || published[0].LedgerVersion != 2 || published[0].BlockerKind != "queued" {
		t.Fatalf("activity sink lost ledger update = %+v", published)
	}
}

func TestWebSocketAttentionTerminalSummaryLeavesLiveMembership(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-terminal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_terminal", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(ctx, "ses_live", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_terminal", "ses_live"}, MaxSessions: 2, ExpiresAt: time.Now().Add(time.Minute)}}, EventStore: events})
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake, EventStore: events})
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_terminal"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if _, err := events.Append(ctx, "ses_terminal", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := handler.EmitEphemeralEvent(ctx, protocol.Event{Type: "agent.activity", SessionID: "ses_terminal", Time: time.Now().UTC().UnixMilli(), Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	terminal := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if terminal.Kind != "update" || len(terminal.Summaries) != 1 || terminal.Summaries[0].TerminalOutcome == nil || *terminal.Summaries[0].TerminalOutcome != "ended" {
		t.Fatalf("terminal attention update = %+v", terminal)
	}
	if _, err := events.Append(ctx, "ses_live", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"busy"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := handler.EmitEphemeralEvent(ctx, protocol.Event{Type: "agent.activity", SessionID: "ses_live", Time: time.Now().UTC().UnixMilli(), Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	live := readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	if live.Kind != "update" || len(live.Summaries) != 1 || live.Summaries[0].SessionID != "ses_live" || live.Summaries[0].LatestSeq != 2 {
		t.Fatalf("terminal Session removed other live membership: %+v", live)
	}
}

func TestWebSocketAttentionRejectsCommandsAndCredentialRotationWithoutMutation(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-deny.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_attention", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_attention"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}}, EventStore: events})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, conn).(*protocol.HelloAck)
	writeFrame(t, conn, &protocol.AttentionSubscribe{RequestID: "attn_deny"})
	_ = readFrame(t, conn).(*protocol.AttentionSummaryFrame)
	for _, command := range []*protocol.Command{
		{CommandID: "cmd_send", Type: protocol.CommandSessionSend, SessionID: "ses_attention", Payload: []byte(`{"content":"must-not-persist"}`)},
		{CommandID: "cmd_permission", Type: protocol.CommandPermissionRespond, SessionID: "ses_attention", Payload: []byte(`{"request_id":"pr_1","decision":"approve"}`)},
	} {
		writeFrame(t, conn, command)
		ack := readFrame(t, conn).(*protocol.CommandAck)
		if ack.CommandID != command.CommandID || ack.Status != protocol.AckRejected || ack.Reason != "unauthorized" {
			t.Fatalf("attention command acknowledgement = %+v", ack)
		}
	}
	writeFrame(t, conn, &protocol.CredentialRotationRequest{RotationID: "rotate_1"})
	if response := readFrame(t, conn).(*protocol.Error); response.Code != "unsupported_frame" {
		t.Fatalf("attention credential rotation response = %+v", response)
	}
	if latest, err := events.LatestSeq(ctx, "ses_attention"); err != nil || latest != 1 {
		t.Fatalf("attention forbidden frames mutated Store latest=%d err=%v", latest, err)
	}
}

func TestWebSocketAttentionReconnectRequiresFreshSubscribe(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "attention-reconnect.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if _, err := events.Append(ctx, "ses_attention", []store.PendingEvent{{Type: "session.state", Time: time.Now().UTC(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionSocketAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_attention"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}}, EventStore: events})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	first := dialWebSocket(t, server.URL)
	writeFrame(t, first, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, first).(*protocol.HelloAck)
	writeFrame(t, first, &protocol.AttentionSubscribe{RequestID: "attn_first"})
	_ = readFrame(t, first).(*protocol.AttentionSummaryFrame)
	if err := first.Close(websocket.StatusNormalClosure, "reconnect"); err != nil {
		t.Fatal(err)
	}
	second := dialWebSocket(t, server.URL)
	defer second.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, second, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	_ = readFrame(t, second).(*protocol.HelloAck)
	writeFrame(t, second, &protocol.Ping{Nonce: "no-old-membership"})
	if pong := readFrame(t, second).(*protocol.Pong); pong.Nonce != "no-old-membership" {
		t.Fatalf("reconnect received a summary before fresh subscribe: %+v", pong)
	}
	writeFrame(t, second, &protocol.AttentionSubscribe{RequestID: "attn_second"})
	if snapshot := readFrame(t, second).(*protocol.AttentionSummaryFrame); snapshot.RequestID != "attn_second" || len(snapshot.Summaries) != 1 {
		t.Fatalf("reconnect fresh snapshot = %+v", snapshot)
	}
}

type attentionSocketAuth struct {
	principal auth.Principal
	grant     auth.AttentionGrant
}

type attentionActivityStore struct {
	mu      sync.RWMutex
	summary store.SessionAttentionSummary

	snapshotMu      sync.Mutex
	snapshotCalls   chan struct{}
	blockNext       bool
	blockedSnapshot chan struct{}
	releaseSnapshot chan struct{}
}

func newAttentionActivityStore(summary store.SessionAttentionSummary) *attentionActivityStore {
	return &attentionActivityStore{summary: summary}
}

func (s *attentionActivityStore) Append(context.Context, string, []store.PendingEvent) (int64, error) {
	return 0, nil
}
func (s *attentionActivityStore) Replay(context.Context, string, int64, func(store.Event) error) error {
	return nil
}
func (s *attentionActivityStore) History(context.Context, string, *int64, int) (store.HistoryPage, error) {
	return store.HistoryPage{}, nil
}
func (s *attentionActivityStore) LatestSeq(context.Context, string) (int64, error) { return 0, nil }
func (s *attentionActivityStore) AttentionSnapshot(_ context.Context, sessionIDs []string) ([]store.SessionAttentionSummary, error) {
	s.mu.RLock()
	summary := s.summary
	s.mu.RUnlock()
	s.snapshotMu.Lock()
	if s.snapshotCalls != nil {
		select {
		case s.snapshotCalls <- struct{}{}:
		default:
		}
	}
	block := s.blockNext
	started, release := s.blockedSnapshot, s.releaseSnapshot
	if block {
		s.blockNext = false
	}
	s.snapshotMu.Unlock()
	if block {
		close(started)
		<-release
	}
	for _, sessionID := range sessionIDs {
		if sessionID == summary.SessionID {
			return []store.SessionAttentionSummary{summary}, nil
		}
	}
	return nil, nil
}
func (s *attentionActivityStore) AttentionSummaryPage(context.Context, store.AttentionSummaryPageRequest) (store.AttentionSummaryPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return store.AttentionSummaryPage{SnapshotAt: time.Now().UTC(), Summaries: []store.SessionAttentionSummary{s.summary}}, nil
}
func (s *attentionActivityStore) setSummary(summary store.SessionAttentionSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.summary = summary
}

func (s *attentionActivityStore) observeSnapshots() <-chan struct{} {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	s.snapshotCalls = make(chan struct{}, 4)
	return s.snapshotCalls
}

func (s *attentionActivityStore) blockNextSnapshot() (<-chan struct{}, chan<- struct{}) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	s.blockNext = true
	s.blockedSnapshot = make(chan struct{})
	s.releaseSnapshot = make(chan struct{})
	return s.blockedSnapshot, s.releaseSnapshot
}

type mutableAttentionSocketAuth struct {
	mu        sync.RWMutex
	principal auth.Principal
	grant     auth.AttentionGrant
}

func (a *mutableAttentionSocketAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return a.principal, nil
}
func (a *mutableAttentionSocketAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}
func (a *mutableAttentionSocketAuth) AuthorizeAttention(context.Context, auth.Principal) (auth.AttentionGrant, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	grant := a.grant
	grant.SessionIDs = append([]string(nil), a.grant.SessionIDs...)
	return grant, nil
}
func (a *mutableAttentionSocketAuth) setGrant(grant auth.AttentionGrant) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.grant = grant
}

func (a attentionSocketAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return a.principal, nil
}
func (a attentionSocketAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}
func (a attentionSocketAuth) AuthorizeAttention(context.Context, auth.Principal) (auth.AttentionGrant, error) {
	return a.grant, nil
}
