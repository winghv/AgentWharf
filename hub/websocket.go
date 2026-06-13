package hub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"nhooyr.io/websocket"
)

const defaultHandshakeTimeout = 10 * time.Second
const maxReplayBufferedEvents = 1024

var errReplayBufferOverflow = errors.New("replay buffer overflow")

type WebSocketConfig struct {
	Handshake        *Handshake
	EventStore       store.EventStore
	HandshakeTimeout time.Duration
}

func NewWebSocketHandler(cfg WebSocketConfig) http.Handler {
	timeout := cfg.HandshakeTimeout
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	return &webSocketHandler{
		handshake:        cfg.Handshake,
		events:           cfg.EventStore,
		handshakeTimeout: timeout,
		subscribers:      make(map[string]map[*clientConnection]struct{}),
	}
}

type webSocketHandler struct {
	handshake        *Handshake
	events           store.EventStore
	handshakeTimeout time.Duration

	mu          sync.Mutex
	subscribers map[string]map[*clientConnection]struct{}
}

func (h *webSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	accepted, err := h.acceptPeer(ctx, conn)
	if err != nil {
		return
	}
	peer := h.registerPeer(conn, accepted)
	if peer != nil {
		defer h.unregisterClient(peer)
	}
	if err := h.replayAccepted(ctx, peer, accepted); err != nil {
		return
	}
	h.readLoop(ctx, conn, accepted)
}

func (h *webSocketHandler) acceptPeer(ctx context.Context, conn *websocket.Conn) (AcceptedPeer, error) {
	frame, err := h.readHelloFrame(ctx, conn)
	if err != nil {
		_ = writeProtocolError(context.Background(), conn, "timeout", "waiting for hello", true)
		_ = conn.Close(websocket.StatusPolicyViolation, "hello timeout")
		return AcceptedPeer{}, err
	}
	hello, ok := frame.(*protocol.Hello)
	if !ok {
		_ = writeProtocolError(ctx, conn, "invalid_hello", "first frame must be hello", true)
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid hello")
		return AcceptedPeer{}, ErrInvalidHello
	}
	if h.handshake == nil {
		err := errors.New("websocket handshake is not configured")
		_ = writeProtocolError(ctx, conn, "internal_error", err.Error(), true)
		_ = conn.Close(websocket.StatusInternalError, "handshake not configured")
		return AcceptedPeer{}, err
	}
	ack, accepted, err := h.handshake.HandleHello(ctx, hello)
	if err != nil {
		code := protocolErrorCode(err)
		_ = writeProtocolError(ctx, conn, code, err.Error(), true)
		_ = conn.Close(websocket.StatusPolicyViolation, code)
		return AcceptedPeer{}, err
	}
	if err := writeProtocolFrame(ctx, conn, &ack); err != nil {
		return AcceptedPeer{}, err
	}
	return accepted, nil
}

func (h *webSocketHandler) readHelloFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	type result struct {
		frame protocol.Frame
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		frame, err := readProtocolFrame(ctx, conn)
		resultCh <- result{frame: frame, err: err}
	}()

	timer := time.NewTimer(h.handshakeTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.frame, result.err
	case <-timer.C:
		return nil, context.DeadlineExceeded
	}
}

func (h *webSocketHandler) readLoop(ctx context.Context, conn *websocket.Conn, accepted AcceptedPeer) {
	for {
		frame, err := readProtocolFrame(ctx, conn)
		if err != nil {
			return
		}
		switch typed := frame.(type) {
		case *protocol.Ping:
			if err := writeProtocolFrame(ctx, conn, &protocol.Pong{Nonce: typed.Nonce}); err != nil {
				return
			}
		case *protocol.Pong:
			continue
		case *protocol.Event:
			if accepted.Role != protocol.RoleAdapter {
				_ = writeProtocolError(ctx, conn, "unsupported_frame", "client event frames are not accepted", false)
				continue
			}
			if err := h.handleAdapterEvent(ctx, conn, accepted, typed); err != nil {
				continue
			}
		default:
			_ = writeProtocolError(ctx, conn, "unsupported_frame", fmt.Sprintf("unsupported frame %s", typed.FrameName()), false)
		}
	}
}

func (h *webSocketHandler) replayAccepted(ctx context.Context, peer *clientConnection, accepted AcceptedPeer) error {
	if h.events == nil || accepted.Role != protocol.RoleClient || peer == nil {
		return nil
	}
	for _, sub := range accepted.Subscribed {
		if err := h.events.Replay(ctx, sub.SessionID, sub.LastSeq, func(ev store.Event) error {
			seq := ev.Seq
			return peer.writeReplayEvent(ctx, protocol.Event{
				Type:      ev.Type,
				SessionID: ev.SessionID,
				Seq:       &seq,
				Time:      ev.Time.UnixMilli(),
				Payload:   ev.Payload,
			})
		}); err != nil {
			_ = peer.writeFrame(ctx, &protocol.Error{Code: "replay_failed", Message: err.Error(), Fatal: true})
			_ = peer.conn.Close(websocket.StatusInternalError, "replay failed")
			return err
		}
		if err := peer.finishReplay(ctx, sub.SessionID); err != nil {
			return err
		}
	}
	return nil
}

func (h *webSocketHandler) registerPeer(conn *websocket.Conn, accepted AcceptedPeer) *clientConnection {
	if accepted.Role != protocol.RoleClient {
		return nil
	}
	peer := newClientConnection(conn, accepted.Subscribed, h.events != nil)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range accepted.Subscribed {
		if h.subscribers[sub.SessionID] == nil {
			h.subscribers[sub.SessionID] = make(map[*clientConnection]struct{})
		}
		h.subscribers[sub.SessionID][peer] = struct{}{}
	}
	return peer
}

func (h *webSocketHandler) unregisterClient(peer *clientConnection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sessionID := range peer.subscriptions {
		delete(h.subscribers[sessionID], peer)
		if len(h.subscribers[sessionID]) == 0 {
			delete(h.subscribers, sessionID)
		}
	}
}

func (h *webSocketHandler) handleAdapterEvent(ctx context.Context, conn *websocket.Conn, accepted AcceptedPeer, ev *protocol.Event) error {
	if ev == nil || ev.Type == "" || ev.SessionID == "" {
		err := errors.New("event type and session_id are required")
		_ = writeProtocolError(ctx, conn, "invalid_event", err.Error(), false)
		return err
	}
	if ev.SessionID != accepted.SessionID {
		err := fmt.Errorf("adapter is not authorized for session %s", ev.SessionID)
		_ = writeProtocolError(ctx, conn, "unauthorized", err.Error(), false)
		return err
	}
	if ev.Seq != nil {
		err := errors.New("adapter events must not include seq")
		_ = writeProtocolError(ctx, conn, "invalid_event", err.Error(), false)
		return err
	}

	eventTime := normalizedEventTime(ev.Time)
	out := protocol.Event{
		Type:      ev.Type,
		SessionID: ev.SessionID,
		Time:      eventTime.UnixMilli(),
		Payload:   clonePayload(ev.Payload),
	}
	if isEphemeralEvent(ev.Type) {
		h.broadcastEvent(ctx, out)
		return nil
	}
	if h.events == nil {
		err := errors.New("event store is not configured")
		_ = writeProtocolError(ctx, conn, "persist_failed", err.Error(), false)
		return err
	}
	firstSeq, err := h.events.Append(ctx, ev.SessionID, []store.PendingEvent{{
		Type:    ev.Type,
		Time:    eventTime,
		Payload: clonePayload(ev.Payload),
	}})
	if err != nil {
		err = fmt.Errorf("persist event: %w", err)
		_ = writeProtocolError(ctx, conn, "persist_failed", err.Error(), false)
		return err
	}
	out.Seq = &firstSeq
	h.broadcastEvent(ctx, out)
	return nil
}

func (h *webSocketHandler) broadcastEvent(ctx context.Context, ev protocol.Event) {
	h.mu.Lock()
	targets := make([]*clientConnection, 0, len(h.subscribers[ev.SessionID]))
	for client := range h.subscribers[ev.SessionID] {
		targets = append(targets, client)
	}
	h.mu.Unlock()

	for _, client := range targets {
		if err := client.sendLiveEvent(ctx, ev); err != nil {
			h.unregisterClient(client)
			if errors.Is(err, errReplayBufferOverflow) {
				_ = client.close(websocket.StatusPolicyViolation, "replay buffer overflow")
			}
		}
	}
}

func normalizedEventTime(unixMilli int64) time.Time {
	if unixMilli <= 0 {
		return time.Now().UTC()
	}
	return time.UnixMilli(unixMilli)
}

func clonePayload(payload json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), payload...)
}

func isEphemeralEvent(eventType string) bool {
	switch eventType {
	case "presence", "agent.activity", "log.tail", "resource.sample":
		return true
	default:
		return false
	}
}

type clientConnection struct {
	conn    *websocket.Conn
	writeMu sync.Mutex

	mu            sync.Mutex
	subscriptions map[string]*subscriptionState
}

type subscriptionState struct {
	lastSeq   int64
	replaying bool
	buffered  []protocol.Event
}

func newClientConnection(conn *websocket.Conn, subscriptions []protocol.Subscription, replaying bool) *clientConnection {
	peer := &clientConnection{
		conn:          conn,
		subscriptions: make(map[string]*subscriptionState, len(subscriptions)),
	}
	for _, sub := range subscriptions {
		peer.subscriptions[sub.SessionID] = &subscriptionState{
			lastSeq:   sub.LastSeq,
			replaying: replaying,
		}
	}
	return peer
}

func (c *clientConnection) writeFrame(ctx context.Context, frame protocol.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeProtocolFrame(ctx, c.conn, frame)
}

func (c *clientConnection) writeReplayEvent(ctx context.Context, ev protocol.Event) error {
	if err := c.writeFrame(ctx, &ev); err != nil {
		return err
	}
	c.markSent(ev)
	return nil
}

func (c *clientConnection) sendLiveEvent(ctx context.Context, ev protocol.Event) error {
	c.mu.Lock()
	state := c.subscriptions[ev.SessionID]
	if state == nil {
		c.mu.Unlock()
		return nil
	}
	if state.replaying {
		if ev.Seq == nil {
			c.mu.Unlock()
			return nil
		}
		if len(state.buffered) >= maxReplayBufferedEvents {
			c.mu.Unlock()
			return errReplayBufferOverflow
		}
		state.buffered = append(state.buffered, cloneProtocolEvent(ev))
		c.mu.Unlock()
		return nil
	}
	if ev.Seq != nil && *ev.Seq <= state.lastSeq {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := c.writeFrame(ctx, &ev); err != nil {
		return err
	}
	c.markSent(ev)
	return nil
}

func (c *clientConnection) finishReplay(ctx context.Context, sessionID string) error {
	for {
		c.mu.Lock()
		state := c.subscriptions[sessionID]
		if state == nil {
			c.mu.Unlock()
			return nil
		}
		buffered := append([]protocol.Event(nil), state.buffered...)
		state.buffered = nil
		if len(buffered) == 0 {
			state.replaying = false
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()

		for _, ev := range buffered {
			if err := c.writeBufferedEvent(ctx, ev); err != nil {
				return err
			}
		}
	}
}

func (c *clientConnection) writeBufferedEvent(ctx context.Context, ev protocol.Event) error {
	c.mu.Lock()
	state := c.subscriptions[ev.SessionID]
	if state == nil || (ev.Seq != nil && *ev.Seq <= state.lastSeq) {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := c.writeFrame(ctx, &ev); err != nil {
		return err
	}
	c.markSent(ev)
	return nil
}

func (c *clientConnection) markSent(ev protocol.Event) {
	if ev.Seq == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.subscriptions[ev.SessionID]
	if state != nil && *ev.Seq > state.lastSeq {
		state.lastSeq = *ev.Seq
	}
}

func (c *clientConnection) close(code websocket.StatusCode, reason string) error {
	return c.conn.Close(code, reason)
}

func cloneProtocolEvent(ev protocol.Event) protocol.Event {
	cloned := protocol.Event{
		Type:      ev.Type,
		SessionID: ev.SessionID,
		Time:      ev.Time,
		Payload:   clonePayload(ev.Payload),
	}
	if ev.Seq != nil {
		seq := *ev.Seq
		cloned.Seq = &seq
	}
	return cloned
}

func readProtocolFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	messageType, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("expected text websocket message, got %v", messageType)
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		return nil, err
	}
	return frame, nil
}

func writeProtocolFrame(ctx context.Context, conn *websocket.Conn, frame protocol.Frame) error {
	data, err := protocol.Encode(frame)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func writeProtocolError(ctx context.Context, conn *websocket.Conn, code string, message string, fatal bool) error {
	return writeProtocolFrame(ctx, conn, &protocol.Error{
		Code:    code,
		Message: message,
		Fatal:   fatal,
	})
}

func protocolErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrInvalidHello), errors.Is(err, ErrVersionUnsupported):
		return "invalid_hello"
	case errors.Is(err, auth.ErrInvalidToken), errors.Is(err, auth.ErrUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrSessionNotFound):
		return "session_not_found"
	default:
		return "internal_error"
	}
}
