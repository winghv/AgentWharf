package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

func TestHubConnectionReconnectsSameSessionAndReplaysUnconfirmedProposal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	connections := 0
	proposalIDs := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		frame, err := readFrameFromConn(ctx, conn)
		hello, ok := frame.(*protocol.Hello)
		if err != nil || !ok || hello.SessionID != "ses_reconnect" {
			t.Errorf("hello = %T %+v, %v", frame, frame, err)
			return
		}
		mu.Lock()
		connections++
		connectionNumber := connections
		mu.Unlock()
		if connectionNumber == 2 && !hello.Resume {
			t.Error("replacement hello did not request resume")
			return
		}
		if err := writeFrameToConn(ctx, conn, reconnectHelloAck("ses_reconnect", int64(connectionNumber), 1)); err != nil {
			return
		}
		frame, err = readFrameFromConn(ctx, conn)
		event, ok := frame.(*protocol.Event)
		if err != nil || !ok || event.ProposalID == "" {
			t.Errorf("proposal = %T %+v, %v", frame, frame, err)
			return
		}
		proposalIDs <- event.ProposalID
		if connectionNumber == 1 {
			_ = conn.Close(websocket.StatusGoingAway, "temporary disconnect")
			return
		}
		_ = writeFrameToConn(ctx, conn, &protocol.EventReceipt{ProposalID: event.ProposalID, Seq: 1, Status: protocol.EventReceiptAccepted})
		_ = writeFrameToConn(ctx, conn, &protocol.Ping{Nonce: "after-reconnect"})
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	initial, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrameToConn(ctx, initial, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleAdapter, Token: "token-1", SessionID: "ses_reconnect", Provider: "claude-code"}); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameFromConn(ctx, initial); err != nil {
		t.Fatal(err)
	}
	connection := newHubConnection(wrapConfig{HubURL: url, SessionID: "ses_reconnect", Provider: "claude-code", AdapterToken: "token-1", ProtocolVersion: protocol.ProtocolVersionV2}, initial, reconnectAuthority("ses_reconnect", 1, 1))
	defer connection.close()
	event := &protocol.Event{Type: "session.message", SessionID: "ses_reconnect", Time: time.Now().UnixMilli(), Payload: []byte(`{"role":"agent","text":"once"}`)}
	if err := connection.write(ctx, event); err != nil {
		t.Fatalf("write proposal: %v", err)
	}
	for {
		frame, err := connection.read(ctx)
		if err != nil {
			t.Fatalf("read after reconnect: %v", err)
		}
		if _, ok := frame.(*protocol.EventReceipt); ok {
			continue
		}
		if ping, ok := frame.(*protocol.Ping); !ok || ping.Nonce != "after-reconnect" {
			t.Fatalf("frame after reconnect = %#v", frame)
		}
		break
	}
	first, second := <-proposalIDs, <-proposalIDs
	if first != second || first != event.ProposalID {
		t.Fatalf("proposal ids = %q, %q; event=%q", first, second, event.ProposalID)
	}
	if pending := connection.proposals(); len(pending) != 0 {
		t.Fatalf("pending proposals = %#v", pending)
	}
}

func TestHubConnectionPublishesFreshRegisteredCapabilityAfterReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	connections := 0
	proposalIDs := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		frame, err := readFrameFromConn(ctx, conn)
		hello, ok := frame.(*protocol.Hello)
		if err != nil || !ok || hello.SessionID != "ses_capability_reconnect" {
			t.Errorf("hello = %T %+v, %v", frame, frame, err)
			return
		}
		mu.Lock()
		connections++
		connectionNumber := connections
		mu.Unlock()
		if err := writeFrameToConn(ctx, conn, reconnectHelloAck("ses_capability_reconnect", int64(connectionNumber), 1)); err != nil {
			return
		}
		frame, err = readFrameFromConn(ctx, conn)
		event, ok := frame.(*protocol.Event)
		if err != nil || !ok || event.Type != "session.run.capabilities" || event.ProposalID == "" {
			t.Errorf("capability = %T %+v, %v", frame, frame, err)
			return
		}
		proposalIDs <- event.ProposalID
		if err := writeFrameToConn(ctx, conn, &protocol.EventReceipt{ProposalID: event.ProposalID, Seq: int64(connectionNumber), Status: protocol.EventReceiptAccepted}); err != nil {
			return
		}
		if connectionNumber == 1 {
			_ = conn.Close(websocket.StatusGoingAway, "replace writer")
			return
		}
		_ = writeFrameToConn(ctx, conn, &protocol.Ping{Nonce: "capability-restored"})
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	initial, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrameToConn(ctx, initial, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleAdapter, Token: "token-1", SessionID: "ses_capability_reconnect", Provider: "claude-code"}); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameFromConn(ctx, initial); err != nil {
		t.Fatal(err)
	}
	connection := newHubConnection(wrapConfig{HubURL: url, SessionID: "ses_capability_reconnect", Provider: "claude-code", AdapterToken: "token-1", ProtocolVersion: protocol.ProtocolVersionV2}, initial, reconnectAuthority("ses_capability_reconnect", 1, 1))
	defer connection.close()
	reconnectCount := 0
	remove := connection.setReconnectProposalFactory("run-control", func() (*protocol.Event, error) {
		reconnectCount++
		return &protocol.Event{
			Type: "session.run.capabilities", SessionID: "ses_capability_reconnect",
			Time: time.Now().UnixMilli(), ProposalID: fmt.Sprintf("capability-reconnect-%d", reconnectCount),
			Payload: []byte(`{"schema_version":1,"interrupt_supported":true,"stop_supported":true}`),
		}, nil
	})
	defer remove()
	if err := connection.write(ctx, &protocol.Event{
		Type: "session.run.capabilities", SessionID: "ses_capability_reconnect",
		Time: time.Now().UnixMilli(), ProposalID: "capability-initial",
		Payload: []byte(`{"schema_version":1,"interrupt_supported":true,"stop_supported":true}`),
	}); err != nil {
		t.Fatalf("write initial capability: %v", err)
	}
	for {
		frame, err := connection.read(ctx)
		if err != nil {
			t.Fatalf("read after capability reconnect: %v", err)
		}
		if _, ok := frame.(*protocol.EventReceipt); ok {
			continue
		}
		if ping, ok := frame.(*protocol.Ping); !ok || ping.Nonce != "capability-restored" {
			t.Fatalf("frame after capability reconnect = %#v", frame)
		}
		break
	}
	first, second := <-proposalIDs, <-proposalIDs
	if first != "capability-initial" || second == first || second != "capability-reconnect-1" {
		t.Fatalf("capability proposal ids = %q, %q", first, second)
	}
}

func TestHubConnectionConvergesOnPendingRotatedCredentialAfterLostActivation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	acceptedTokens := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		frame, err := readFrameFromConn(ctx, conn)
		hello, ok := frame.(*protocol.Hello)
		if err != nil || !ok {
			return
		}
		acceptedTokens <- hello.Token
		if hello.Token != "new-token" {
			_ = writeFrameToConn(ctx, conn, &protocol.Error{Code: "unauthorized", Message: "credential replaced", Fatal: true})
			return
		}
		_ = writeFrameToConn(ctx, conn, reconnectHelloAck("ses_rotation", 9, 2))
		_ = writeFrameToConn(ctx, conn, &protocol.Ping{Nonce: "rotated"})
	}))
	defer server.Close()

	connection := newHubConnection(wrapConfig{HubURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "ses_rotation", Provider: "claude-code", AdapterToken: "old-token", ProtocolVersion: protocol.ProtocolVersionV2}, nil, reconnectAuthority("ses_rotation", 8, 1))
	defer connection.close()
	connection.credentials.recordPending("new-token", 2)
	frame, err := connection.read(ctx)
	if err != nil {
		t.Fatalf("read with pending credential: %v", err)
	}
	if ping, ok := frame.(*protocol.Ping); !ok || ping.Nonce != "rotated" {
		t.Fatalf("frame = %#v", frame)
	}
	if got := <-acceptedTokens; got != "new-token" {
		t.Fatalf("first reconnect token = %q", got)
	}
	if candidates := connection.credentials.candidates(); len(candidates) == 0 || candidates[0] != "new-token" {
		t.Fatalf("converged candidates = %#v", candidates)
	}
	if authority := connection.currentAuthority(); authority == nil || authority.ConnectionEpoch != 9 || authority.CredentialGeneration != 2 {
		t.Fatalf("authority = %#v", authority)
	}
}

func TestCredentialSetDropsUnactivatedPendingCredentialWhenOldTokenReconnects(t *testing.T) {
	credentials := newAdapterCredentialSet("old-token", reconnectAuthority("ses_rotation", 8, 1))
	credentials.recordPending("new-token", 2)
	credentials.converge("old-token", 1)
	if candidates := credentials.candidates(); len(candidates) != 1 || candidates[0] != "old-token" {
		t.Fatalf("credential candidates = %#v", candidates)
	}
	if generation := credentials.pendingGeneration(); generation != 0 {
		t.Fatalf("pending generation = %d, want 0", generation)
	}
}

func TestHubConnectionStopsWhenEveryCredentialIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusPolicyViolation, "rejected")
		if _, err := readFrameFromConn(ctx, conn); err == nil {
			_ = writeFrameToConn(ctx, conn, &protocol.Error{Code: "revoked", Message: "session authority revoked", Fatal: true})
		}
	}))
	defer server.Close()

	connection := newHubConnection(wrapConfig{HubURL: "ws" + strings.TrimPrefix(server.URL, "http"), SessionID: "ses_revoked", Provider: "claude-code", AdapterToken: "revoked-token", ProtocolVersion: protocol.ProtocolVersionV2}, nil, reconnectAuthority("ses_revoked", 1, 1))
	defer connection.close()
	if _, err := connection.read(ctx); !errors.Is(err, errClaimAuthRejection) {
		t.Fatalf("read error = %v, want credential rejection", err)
	}
}

func TestHubConnectionCancellationInterruptsReconnectBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	connection := newHubConnection(wrapConfig{HubURL: "ws://127.0.0.1:1", SessionID: "ses_cancel", Provider: "claude-code", AdapterToken: "token", ProtocolVersion: protocol.ProtocolVersionV2}, nil, reconnectAuthority("ses_cancel", 1, 1))
	done := make(chan error, 1)
	go func() {
		_, err := connection.read(ctx)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read error = %v, want context canceled", err)
		}
		if time.Since(started) > 500*time.Millisecond {
			t.Fatalf("cancellation took %v", time.Since(started))
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect backoff ignored cancellation")
	}
}

func reconnectHelloAck(sessionID string, epoch, generation int64) *protocol.HelloAck {
	return &protocol.HelloAck{
		ProtocolVersion:     protocol.ProtocolVersionV2,
		Sessions:            []protocol.SessionSummary{{SessionID: sessionID, Provider: "claude-code"}},
		ConnectionAuthority: reconnectAuthority(sessionID, epoch, generation),
	}
}

func reconnectAuthority(sessionID string, epoch, generation int64) *protocol.ConnectionAuthorityReceipt {
	return &protocol.ConnectionAuthorityReceipt{
		SessionID: sessionID, ConnectionEpoch: epoch, CredentialGeneration: generation,
		AcceptedFence: epoch, WriterLeaseID: "lease-reconnect", ExpiresAt: time.Now().Add(15 * time.Minute).UnixMilli(),
	}
}
