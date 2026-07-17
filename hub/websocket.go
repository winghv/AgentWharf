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
const maxPendingCommandsPerSession = 64
const pendingCommandTTL = 10 * time.Minute
const maxAcceptedCommandIDs = 4096
const maxDecisionRequestIDs = 4096
const adapterEventBatchWindow = 50 * time.Millisecond
const adapterEventBatchMaxEvents = 64

var errReplayBufferOverflow = errors.New("replay buffer overflow")

type WebSocketConfig struct {
	Handshake               *Handshake
	EventStore              store.EventStore
	HandshakeTimeout        time.Duration
	CommandActivityObserver CommandActivityObserver
	AdapterActivityObserver AdapterActivityObserver
}

type EphemeralBroadcaster interface {
	http.Handler
	EmitEphemeralEvent(context.Context, protocol.Event) error
}

type CommandActivity struct {
	SessionID  string
	CommandID  string
	Type       protocol.CommandType
	At         time.Time
	DurableSeq *int64
}

type CommandActivityObserver interface {
	ObserveCommandActivity(context.Context, CommandActivity)
}

type AdapterActivity struct {
	SessionID string
	At        time.Time
}

type AdapterActivityObserver interface {
	ObserveAdapterActivity(context.Context, AdapterActivity)
}

func NewWebSocketHandler(cfg WebSocketConfig) EphemeralBroadcaster {
	timeout := cfg.HandshakeTimeout
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	handler := &webSocketHandler{
		handshake:               cfg.Handshake,
		events:                  cfg.EventStore,
		handshakeTimeout:        timeout,
		commandActivityObserver: cfg.CommandActivityObserver,
		adapterActivityObserver: cfg.AdapterActivityObserver,
		adapterAuthority:        newAdapterDispatchAuthority(cfg.Handshake, cfg.EventStore),
		adapterAdmissionLocks:   make(map[string]chan struct{}),
		subscribers:             make(map[string]map[*clientConnection]struct{}),
		adapters:                make(map[string]*adapterConnection),
		pendingCommands:         make(map[string][]queuedCommand),
		acceptedCommands:        make(map[string]struct{}),
		decisions:               make(map[string]struct{}),
	}
	if cfg.Handshake != nil {
		cfg.Handshake.SetLiveBootstrapAuthorityResolver(handler)
	}
	return handler
}

func (h *webSocketHandler) EmitEphemeralEvent(ctx context.Context, ev protocol.Event) error {
	if ev.Type == "" || ev.SessionID == "" {
		return errors.New("event type and session_id are required")
	}
	if ev.Seq != nil {
		return errors.New("ephemeral event must not include seq")
	}
	if !isEphemeralEvent(ev.Type) {
		return fmt.Errorf("event type %q is not ephemeral", ev.Type)
	}
	eventTime := normalizedEventTime(ev.Time)
	h.broadcastEvent(ctx, protocol.Event{
		Type:      ev.Type,
		SessionID: ev.SessionID,
		Time:      eventTime.UnixMilli(),
		Payload:   clonePayload(ev.Payload),
	})
	return nil
}

type webSocketHandler struct {
	handshake               *Handshake
	events                  store.EventStore
	handshakeTimeout        time.Duration
	commandActivityObserver CommandActivityObserver
	adapterActivityObserver AdapterActivityObserver
	adapterAuthority        *adapterDispatchAuthority
	adapterAdmissionMu      sync.Mutex
	adapterAdmissionLocks   map[string]chan struct{}

	mu          sync.Mutex
	subscribers map[string]map[*clientConnection]struct{}
	adapters    map[string]*adapterConnection

	commandMu            sync.Mutex
	pendingCommands      map[string][]queuedCommand
	acceptedCommands     map[string]struct{}
	acceptedCommandOrder []string
	decisions            map[string]struct{}
	decisionOrder        []string
}

type adapterConnection struct {
	conn      *websocket.Conn
	writeGate contextWriteGate
	effectMu  sync.Mutex
	sessionID string
	provider  string
	handler   *webSocketHandler
	admission store.AdapterConnectionAdmission
	events    *adapterEventBatcher
}

type queuedCommand struct {
	command   protocol.Command
	expiresAt time.Time
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
	var adapter *adapterConnection
	accepted, historyToken, err := h.acceptPeer(ctx, conn, &adapter)
	if err != nil {
		return
	}
	peer := h.registerPeer(conn, accepted)
	if peer != nil {
		defer h.unregisterClient(peer)
	}
	if adapter != nil {
		defer h.unregisterAdapter(adapter)
		defer adapter.close()
		go h.watchAdapter(ctx, adapter)
		if err := h.observeAdapterActivity(ctx, adapter, time.Now().UTC()); err != nil {
			return
		}
		if err := h.deliverPendingCommands(ctx, adapter); err != nil {
			return
		}
	}
	if err := h.replayAccepted(ctx, peer, accepted); err != nil {
		return
	}
	h.readLoop(ctx, conn, accepted, historyToken, peer, adapter)
}

func (h *webSocketHandler) acceptPeer(ctx context.Context, conn *websocket.Conn, adapterOut **adapterConnection) (AcceptedPeer, string, error) {
	frame, err := h.readHelloFrame(ctx, conn)
	if err != nil {
		_ = writeProtocolError(context.Background(), conn, "timeout", "waiting for hello", true)
		_ = conn.Close(websocket.StatusPolicyViolation, "hello timeout")
		return AcceptedPeer{}, "", err
	}
	hello, ok := frame.(*protocol.Hello)
	if !ok {
		_ = writeProtocolError(ctx, conn, "invalid_hello", "first frame must be hello", true)
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid hello")
		return AcceptedPeer{}, "", ErrInvalidHello
	}
	if h.handshake == nil {
		err := errors.New("websocket handshake is not configured")
		_ = writeProtocolError(ctx, conn, "internal_error", err.Error(), true)
		_ = conn.Close(websocket.StatusInternalError, "handshake not configured")
		return AcceptedPeer{}, "", err
	}
	ack, accepted, err := h.handshake.HandleHello(ctx, hello)
	if err != nil {
		code := protocolErrorCode(err)
		_ = writeProtocolError(ctx, conn, code, err.Error(), true)
		_ = conn.Close(websocket.StatusPolicyViolation, code)
		return AcceptedPeer{}, "", err
	}
	if accepted.Role == protocol.RoleClient && accepted.ProtocolVersion == protocol.ProtocolVersionV2 &&
		len(accepted.currentSubscriptions()) > 0 {
		if _, ok := h.events.(store.HistoryStore); ok {
			if ack.Capabilities == nil {
				ack.Capabilities = &protocol.HelloCapabilities{}
			}
			ack.Capabilities.HistoryPage = &protocol.HistoryPageCapability{MaxLimit: protocol.HistoryPageMaxLimit}
		}
	}
	adapter, err := h.registerAdapter(ctx, conn, accepted, hello.Token)
	if err != nil {
		_ = writeProtocolError(ctx, conn, "unauthorized", err.Error(), true)
		_ = conn.Close(websocket.StatusPolicyViolation, "adapter authority lost")
		return AcceptedPeer{}, "", err
	}
	ackErr := writeProtocolFrame(ctx, conn, &ack)
	if ackErr != nil {
		if adapter != nil {
			h.rejectAdapter(adapter)
			adapter.close()
		}
		return AcceptedPeer{}, "", ackErr
	}
	if adapter != nil {
		if err := h.publishAdapter(ctx, adapter); err != nil {
			h.rejectAdapter(adapter)
			adapter.close()
			return AcceptedPeer{}, "", err
		}
	}
	*adapterOut = adapter
	historyToken := ""
	if accepted.Role == protocol.RoleClient && accepted.ProtocolVersion == protocol.ProtocolVersionV2 {
		historyToken = hello.Token
	}
	return accepted, historyToken, nil
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

func (h *webSocketHandler) readLoop(ctx context.Context, conn *websocket.Conn, accepted AcceptedPeer, historyToken string, peer *clientConnection, adapter *adapterConnection) {
	for {
		frame, err := readProtocolFrame(ctx, conn)
		if err != nil {
			return
		}
		switch typed := frame.(type) {
		case *protocol.Ping:
			if err := writePongFrame(ctx, conn, peer, adapter, typed.Nonce); err != nil {
				return
			}
			if accepted.Role == protocol.RoleAdapter {
				if err := h.observeAdapterActivity(ctx, adapter, time.Now().UTC()); err != nil {
					return
				}
			}
		case *protocol.Pong:
			continue
		case *protocol.HistoryPageRequest:
			if err := h.handleHistoryPage(ctx, conn, accepted, historyToken, peer, adapter, typed); err != nil {
				return
			}
		case *protocol.Event:
			if accepted.Role != protocol.RoleAdapter {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "client event frames are not accepted"})
				continue
			}
			if err := h.handleAdapterEvent(ctx, adapter, accepted, typed); err != nil {
				continue
			}
			if err := h.observeAdapterActivity(ctx, adapter, time.Now().UTC()); err != nil {
				return
			}
		case *protocol.Command:
			if accepted.Role != protocol.RoleClient {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "adapter command frames are not accepted"})
				continue
			}
			if err := h.handleClientCommand(ctx, conn, accepted, typed); err != nil {
				continue
			}
		case *protocol.CommandAck:
			if accepted.Role != protocol.RoleAdapter {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "client command ack frames are not accepted"})
			}
			continue
		default:
			_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: fmt.Sprintf("unsupported frame %s", typed.FrameName())})
		}
	}
}

func (h *webSocketHandler) handleHistoryPage(ctx context.Context, conn *websocket.Conn, accepted AcceptedPeer, historyToken string, peer *clientConnection, adapter *adapterConnection, request *protocol.HistoryPageRequest) error {
	history, ready := h.events.(store.HistoryStore)
	if accepted.Role != protocol.RoleClient || accepted.ProtocolVersion != protocol.ProtocolVersionV2 || !ready {
		return h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{
			Code: "history_unsupported", Message: "history pagination is unavailable",
		})
	}
	if request == nil || peer == nil || !subscribesTo(accepted.Subscribed, request.SessionID) ||
		!accepted.allows(request.SessionID, auth.SessionAdmissionHistory) ||
		!h.authorizeHistory(ctx, historyToken, accepted.Principal.Subject, request.SessionID) {
		return h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{
			Code: "history_unavailable", Message: "history is unavailable",
		})
	}
	page, err := history.History(ctx, request.SessionID, request.BeforeSeq, request.Limit)
	if err != nil || !validHistoryPage(page, request) ||
		!h.authorizeHistory(ctx, historyToken, accepted.Principal.Subject, request.SessionID) {
		return h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{
			Code: "history_unavailable", Message: "history is unavailable",
		})
	}
	events := make([]protocol.HistoryPageEvent, len(page.Events))
	for index, event := range page.Events {
		events[index] = protocol.HistoryPageEvent{
			Frame: protocol.FrameEvent, Type: event.Type, SessionID: event.SessionID, Seq: event.Seq,
			Time: event.Time.UnixMilli(), Payload: clonePayload(event.Payload),
		}
	}
	return h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.HistoryPageResponse{
		RequestID: request.RequestID, SessionID: request.SessionID, Events: events,
		LatestSeq: page.LatestSeq, NextBeforeSeq: page.NextBeforeSeq, RetentionState: page.RetentionState,
	})
}

func (h *webSocketHandler) authorizeHistory(ctx context.Context, token, subject, sessionID string) bool {
	if token == "" || subject == "" || h.handshake == nil || h.handshake.authenticator == nil {
		return false
	}
	principal, err := h.handshake.authenticator.Authenticate(ctx, token)
	return err == nil && principal.Subject == subject && hasExactHistoryAccess(principal, sessionID) &&
		h.handshake.authenticator.Authorize(ctx, principal, auth.SessionView(sessionID)) == nil
}

func validHistoryPage(page store.HistoryPage, request *protocol.HistoryPageRequest) bool {
	if request == nil || request.Limit < 1 || request.Limit > protocol.HistoryPageMaxLimit ||
		len(page.Events) > request.Limit || page.LatestSeq < 0 ||
		(page.RetentionState != store.RetentionComplete && page.RetentionState != store.RetentionGap) {
		return false
	}
	for index, event := range page.Events {
		if event.SessionID != request.SessionID || event.Seq < 1 || event.Seq > page.LatestSeq ||
			event.Type == "" || !json.Valid(event.Payload) || len(event.Payload) > protocol.MaxEventPayloadBytes ||
			!protocol.EventTypeAllowed(protocol.ProtocolVersionV2, event.Type, true) ||
			request.BeforeSeq != nil && event.Seq >= *request.BeforeSeq ||
			index > 0 && page.Events[index-1].Seq >= event.Seq {
			return false
		}
	}
	if page.NextBeforeSeq != nil {
		if *page.NextBeforeSeq < 1 || len(page.Events) != request.Limit || *page.NextBeforeSeq != page.Events[0].Seq {
			return false
		}
	}
	if page.RetentionState == store.RetentionComplete {
		eligible := page.LatestSeq
		if request.BeforeSeq != nil && *request.BeforeSeq-1 < eligible {
			eligible = *request.BeforeSeq - 1
		}
		want := eligible
		if want > int64(request.Limit) {
			want = int64(request.Limit)
		}
		if int64(len(page.Events)) != want || (eligible > int64(request.Limit)) != (page.NextBeforeSeq != nil) {
			return false
		}
		first := eligible - want + 1
		for index, event := range page.Events {
			if event.Seq != first+int64(index) {
				return false
			}
		}
	}
	return true
}

func hasExactHistoryAccess(principal auth.Principal, sessionID string) bool {
	for _, scope := range principal.Scopes {
		if scope.Kind == auth.KindSession && scope.ID == sessionID &&
			(scope.Access == auth.AccessView || scope.Access == auth.AccessControl) {
			return true
		}
	}
	return false
}

func (h *webSocketHandler) writeConnectionFrame(ctx context.Context, conn *websocket.Conn, peer *clientConnection, adapter *adapterConnection, frame protocol.Frame) error {
	if peer != nil {
		return peer.writeFrame(ctx, frame)
	}
	if adapter != nil {
		return h.writeAdapterFrame(ctx, adapter, frame)
	}
	return writeProtocolFrame(ctx, conn, frame)
}

func writePongFrame(ctx context.Context, conn *websocket.Conn, peer *clientConnection, adapter *adapterConnection, nonce string) error {
	pong := &protocol.Pong{Nonce: nonce}
	if peer != nil {
		return peer.writeFrame(ctx, pong)
	}
	if adapter != nil {
		return adapter.handler.writeAdapterFrame(ctx, adapter, pong)
	}
	return writeProtocolFrame(ctx, conn, pong)
}

func (h *webSocketHandler) replayAccepted(ctx context.Context, peer *clientConnection, accepted AcceptedPeer) error {
	if h.events == nil || accepted.Role != protocol.RoleClient || peer == nil {
		return nil
	}
	for _, sub := range accepted.currentSubscriptions() {
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
	current := accepted.currentSubscriptions()
	peer := newClientConnection(conn, accepted.ProtocolVersion, current, h.events != nil)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range current {
		if h.subscribers[sub.SessionID] == nil {
			h.subscribers[sub.SessionID] = make(map[*clientConnection]struct{})
		}
		h.subscribers[sub.SessionID][peer] = struct{}{}
	}
	return peer
}

func (h *webSocketHandler) registerAdapter(ctx context.Context, conn *websocket.Conn, accepted AcceptedPeer, token string) (*adapterConnection, error) {
	if accepted.Role != protocol.RoleAdapter {
		return nil, nil
	}
	if h.adapterAuthority == nil {
		return nil, errAdapterAuthorityLost
	}
	_, unlock := h.lockAdapterAdmission(accepted.SessionID)
	defer unlock()
	admission, err := h.adapterAuthority.admit(ctx, token, accepted.Principal, accepted.SessionID)
	if err != nil {
		return nil, err
	}
	adapter := &adapterConnection{conn: conn, writeGate: newContextWriteGate(), sessionID: accepted.SessionID, provider: accepted.Provider, handler: h, admission: admission}
	if h.events != nil {
		adapter.events = newAdapterEventBatcher(adapterEventBatcherConfig{
			Store:     fencedAdapterEventStore{handler: h, adapter: adapter},
			SessionID: accepted.SessionID,
			Window:    adapterEventBatchWindow,
			MaxEvents: adapterEventBatchMaxEvents,
			Broadcast: h.broadcastEvent,
			ReportError: func(ctx context.Context, err error) {
				_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{
					Code:    "persist_failed",
					Message: err.Error(),
				})
			},
		})
	}
	return adapter, nil
}

func (h *webSocketHandler) publishAdapter(ctx context.Context, adapter *adapterConnection) error {
	previous, unlock := h.lockAdapterAdmission(adapter.sessionID)
	if _, err := h.adapterAuthority.store.ValidateAdapterAdmission(ctx, adapter.sessionID, adapter.admission); err != nil {
		unlock()
		return errAdapterAuthorityLost
	}
	h.mu.Lock()
	h.adapters[adapter.sessionID] = adapter
	h.mu.Unlock()
	unlock()
	if previous != nil && previous.conn != nil {
		previous.conn.CloseNow()
	}
	return nil
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

func (h *webSocketHandler) unregisterAdapter(adapter *adapterConnection) {
	_, unlock := h.lockAdapterAdmission(adapter.sessionID)
	defer unlock()
	h.removeAdapter(adapter)
}

func (h *webSocketHandler) removeAdapter(adapter *adapterConnection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if current := h.adapters[adapter.sessionID]; current == adapter {
		delete(h.adapters, adapter.sessionID)
	}
}

func (h *webSocketHandler) CurrentBootstrapAuthority(ctx context.Context, grant auth.AttachGrant) (auth.BootstrapAuthority, error) {
	if h.adapterAuthority == nil {
		return auth.BootstrapAuthority{}, errAdapterAuthorityLost
	}
	_, unlock := h.lockAdapterAdmission(grant.BootstrapSessionID)
	defer unlock()
	h.mu.Lock()
	adapter := h.adapters[grant.BootstrapSessionID]
	if adapter == nil || adapter.provider != grant.Provider {
		h.mu.Unlock()
		return auth.BootstrapAuthority{}, auth.ErrUnauthorized
	}
	admission := adapter.admission
	h.mu.Unlock()
	if _, err := h.adapterAuthority.store.ValidateAdapterAdmission(ctx, grant.BootstrapSessionID, store.AdapterConnectionAdmission{
		CredentialGeneration: admission.CredentialGeneration, ConnectionEpoch: admission.ConnectionEpoch,
		AcceptedFence: admission.AcceptedFence, GrantFence: grant.GrantFence,
	}); err != nil {
		return auth.BootstrapAuthority{}, auth.ErrUnauthorized
	}
	return auth.BootstrapAuthority{SessionID: grant.BootstrapSessionID, Provider: adapter.provider,
		CredentialGeneration: admission.CredentialGeneration, ConnectionEpoch: admission.ConnectionEpoch,
		AcceptedFence: admission.AcceptedFence, Live: true}, nil
}

func (a *adapterConnection) writeFrame(ctx context.Context, frame protocol.Frame) error {
	writeCtx, cancel := context.WithTimeout(ctx, adapterAuthorityPollInterval)
	defer cancel()
	release, err := a.writeGate.lock(writeCtx)
	if err != nil {
		return err
	}
	defer release()
	return writeProtocolFrame(writeCtx, a.conn, frame)
}

func (a *adapterConnection) close() {
	if a.events != nil {
		a.events.Close()
	}
}

func (h *webSocketHandler) handleAdapterEvent(ctx context.Context, adapter *adapterConnection, accepted AcceptedPeer, ev *protocol.Event) error {
	if adapter == nil {
		return errors.New("adapter connection is required")
	}
	if ev == nil || ev.Type == "" || ev.SessionID == "" {
		err := errors.New("event type and session_id are required")
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
		return err
	}
	if !protocol.PeerEventTypeAllowed(ev.Type) {
		err := fmt.Errorf("event type %q is reserved for the trusted publisher", ev.Type)
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
		return err
	}
	if ev.SessionID != accepted.SessionID {
		err := fmt.Errorf("adapter is not authorized for session %s", ev.SessionID)
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "unauthorized", Message: err.Error()})
		return err
	}
	if ev.Seq != nil {
		err := errors.New("adapter events must not include seq")
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
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
		return h.withAdapterEffect(ctx, adapter, func() error {
			return h.adapterAuthority.withAdmission(ctx, adapter, func(effectCtx context.Context) error {
				h.broadcastEvent(effectCtx, out)
				return nil
			})
		})
	}
	if h.events == nil {
		err := errors.New("event store is not configured")
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
		return err
	}
	if adapter.events == nil {
		err := errors.New("adapter event batcher is not configured")
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
		return err
	}
	return adapter.events.Enqueue(ctx, out, store.PendingEvent{
		Type:    ev.Type,
		Time:    eventTime,
		Payload: clonePayload(ev.Payload),
	})
}

func (h *webSocketHandler) handleClientCommand(ctx context.Context, conn *websocket.Conn, accepted AcceptedPeer, cmd *protocol.Command) error {
	if err := validateClientCommand(cmd); err != nil {
		_ = writeCommandAck(ctx, conn, commandID(cmd), protocol.AckRejected, "invalid_command")
		return err
	}
	if !subscribesTo(accepted.Subscribed, cmd.SessionID) {
		err := fmt.Errorf("client is not subscribed to session %s", cmd.SessionID)
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "unauthorized")
		return err
	}
	if !accepted.allows(cmd.SessionID, commandAdmissionAction(cmd.Type)) {
		err := errors.New("session admission denies command")
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "unauthorized")
		return err
	}
	if h.handshake == nil || h.handshake.authenticator == nil {
		err := errors.New("hub authenticator is not configured")
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "internal_error")
		return err
	}
	if err := h.handshake.authenticator.Authorize(ctx, accepted.Principal, auth.SessionControl(cmd.SessionID)); err != nil {
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "unauthorized")
		return err
	}

	h.commandMu.Lock()
	locked := true
	defer func() {
		if locked {
			h.commandMu.Unlock()
		}
	}()

	if _, ok := h.acceptedCommands[cmd.CommandID]; ok {
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckDuplicate, "")
		return nil
	}

	if requestID := permissionDecisionKey(cmd); requestID != "" {
		if _, ok := h.decisions[requestID]; ok {
			_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckDuplicate, "")
			return nil
		}
	}

	if err := h.preflightCommandRouteLocked(cmd); err != nil {
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, err.Error())
		return err
	}

	var persisted *protocol.Event
	if commandNeedsPersistence(cmd.Type) {
		ev, err := h.persistCommandEvent(ctx, cmd)
		if err != nil {
			_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "persist_failed")
			return err
		}
		persisted = ev
	}

	if persisted != nil {
		h.broadcastEvent(ctx, *persisted)
	}
	if err := h.routeOrBufferCommand(ctx, cmd); err != nil {
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, err.Error())
		return err
	}
	h.markCommandAcceptedLocked(cmd.CommandID)
	if requestID := permissionDecisionKey(cmd); requestID != "" {
		h.markDecisionAcceptedLocked(requestID)
	}
	activity := commandActivity(cmd, persisted)
	h.commandMu.Unlock()
	locked = false
	h.observeCommandActivity(ctx, activity)
	if err := writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckAccepted, ""); err != nil {
		return err
	}
	return nil
}

func commandAdmissionAction(commandType protocol.CommandType) auth.SessionAdmissionAction {
	switch commandType {
	case protocol.CommandSessionSend:
		return auth.SessionAdmissionSend
	case protocol.CommandPermissionRespond:
		return auth.SessionAdmissionPermission
	case protocol.CommandSessionInterrupt, protocol.CommandSessionStop:
		return auth.SessionAdmissionRunControl
	default:
		return ""
	}
}

func (h *webSocketHandler) observeCommandActivity(ctx context.Context, activity CommandActivity) {
	if h.commandActivityObserver == nil {
		return
	}
	h.commandActivityObserver.ObserveCommandActivity(ctx, activity)
}

func (h *webSocketHandler) observeAdapterActivity(ctx context.Context, adapter *adapterConnection, at time.Time) error {
	if h.adapterActivityObserver == nil {
		return nil
	}
	return h.withAdapterEffect(ctx, adapter, func() error {
		return h.adapterAuthority.withAdmission(ctx, adapter, func(effectCtx context.Context) error {
			h.adapterActivityObserver.ObserveAdapterActivity(effectCtx, AdapterActivity{SessionID: adapter.sessionID, At: at.UTC()})
			return nil
		})
	})
}

func commandActivity(cmd *protocol.Command, persisted *protocol.Event) CommandActivity {
	at := time.Now().UTC()
	var durableSeq *int64
	if persisted != nil {
		if persisted.Time > 0 {
			at = time.UnixMilli(persisted.Time).UTC()
		}
		if persisted.Seq != nil {
			seq := *persisted.Seq
			durableSeq = &seq
		}
	}
	return CommandActivity{
		SessionID:  cmd.SessionID,
		CommandID:  cmd.CommandID,
		Type:       cmd.Type,
		At:         at,
		DurableSeq: durableSeq,
	}
}

func (h *webSocketHandler) persistCommandEvent(ctx context.Context, cmd *protocol.Command) (*protocol.Event, error) {
	if h.events == nil {
		return nil, errors.New("event store is not configured")
	}
	eventType, payload, err := commandEventPayload(cmd)
	if err != nil {
		return nil, err
	}
	eventTime := time.Now().UTC()
	firstSeq, err := h.events.Append(ctx, cmd.SessionID, []store.PendingEvent{{
		Type:    eventType,
		Time:    eventTime,
		Payload: payload,
	}})
	if err != nil {
		return nil, fmt.Errorf("persist command event: %w", err)
	}
	return &protocol.Event{
		Type:      eventType,
		SessionID: cmd.SessionID,
		Seq:       &firstSeq,
		Time:      eventTime.UnixMilli(),
		Payload:   payload,
	}, nil
}

func (h *webSocketHandler) preflightCommandRouteLocked(cmd *protocol.Command) error {
	if commandCanBuffer(cmd.Type) {
		pending := h.prunePendingCommandsLocked(cmd.SessionID, time.Now().UTC())
		if len(pending) >= maxPendingCommandsPerSession {
			return errors.New("command_buffer_full")
		}
		return nil
	}
	if !h.hasAdapter(cmd.SessionID) {
		return errors.New("adapter_offline")
	}
	return nil
}

func (h *webSocketHandler) routeOrBufferCommand(ctx context.Context, cmd *protocol.Command) error {
	if err := h.routeCommand(ctx, cmd); err == nil {
		return nil
	}
	if !commandCanBuffer(cmd.Type) {
		return errors.New("adapter_offline")
	}
	return h.bufferCommandLocked(cmd)
}

func (h *webSocketHandler) routeCommand(ctx context.Context, cmd *protocol.Command) error {
	h.mu.Lock()
	adapter := h.adapters[cmd.SessionID]
	h.mu.Unlock()
	if adapter == nil {
		return errors.New("adapter_offline")
	}
	routed := cloneCommand(cmd)
	if err := h.writeAdapterFrame(ctx, adapter, &routed); err != nil {
		h.unregisterAdapter(adapter)
		return fmt.Errorf("adapter_offline: %w", err)
	}
	return nil
}

func (h *webSocketHandler) bufferCommandLocked(cmd *protocol.Command) error {
	now := time.Now().UTC()
	filtered := h.prunePendingCommandsLocked(cmd.SessionID, now)
	if len(filtered) >= maxPendingCommandsPerSession {
		h.pendingCommands[cmd.SessionID] = filtered
		return errors.New("command_buffer_full")
	}
	filtered = append(filtered, queuedCommand{
		command:   cloneCommand(cmd),
		expiresAt: now.Add(pendingCommandTTL),
	})
	h.pendingCommands[cmd.SessionID] = filtered
	return nil
}

func (h *webSocketHandler) prunePendingCommandsLocked(sessionID string, now time.Time) []queuedCommand {
	pending := h.pendingCommands[sessionID]
	filtered := pending[:0]
	for _, queued := range pending {
		if now.Before(queued.expiresAt) {
			filtered = append(filtered, queued)
		}
	}
	if len(filtered) == 0 {
		delete(h.pendingCommands, sessionID)
		return nil
	}
	h.pendingCommands[sessionID] = filtered
	return filtered
}

func (h *webSocketHandler) hasAdapter(sessionID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.adapters[sessionID] != nil
}

func (h *webSocketHandler) deliverPendingCommands(ctx context.Context, adapter *adapterConnection) error {
	h.commandMu.Lock()
	defer h.commandMu.Unlock()

	now := time.Now().UTC()
	pending := h.pendingCommands[adapter.sessionID]
	remaining := pending[:0]
	for i, queued := range pending {
		if !now.Before(queued.expiresAt) {
			continue
		}
		routed := cloneCommand(&queued.command)
		if err := h.writeAdapterFrame(ctx, adapter, &routed); err != nil {
			remaining = append(remaining, queued)
			remaining = append(remaining, pending[i+1:]...)
			h.pendingCommands[adapter.sessionID] = remaining
			h.unregisterAdapter(adapter)
			return fmt.Errorf("deliver pending command: %w", err)
		}
	}
	if len(remaining) == 0 {
		delete(h.pendingCommands, adapter.sessionID)
		return nil
	}
	h.pendingCommands[adapter.sessionID] = remaining
	return nil
}

func (h *webSocketHandler) markCommandAcceptedLocked(commandID string) {
	if _, ok := h.acceptedCommands[commandID]; ok {
		return
	}
	h.acceptedCommands[commandID] = struct{}{}
	h.acceptedCommandOrder = append(h.acceptedCommandOrder, commandID)
	for len(h.acceptedCommandOrder) > maxAcceptedCommandIDs {
		oldest := h.acceptedCommandOrder[0]
		h.acceptedCommandOrder = h.acceptedCommandOrder[1:]
		delete(h.acceptedCommands, oldest)
	}
}

func (h *webSocketHandler) markDecisionAcceptedLocked(requestID string) {
	if _, ok := h.decisions[requestID]; ok {
		return
	}
	h.decisions[requestID] = struct{}{}
	h.decisionOrder = append(h.decisionOrder, requestID)
	for len(h.decisionOrder) > maxDecisionRequestIDs {
		oldest := h.decisionOrder[0]
		h.decisionOrder = h.decisionOrder[1:]
		delete(h.decisions, oldest)
	}
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

func validateClientCommand(cmd *protocol.Command) error {
	if cmd == nil || cmd.CommandID == "" || cmd.Type == "" || cmd.SessionID == "" {
		return errors.New("command cmd_id, type, and session_id are required")
	}
	switch cmd.Type {
	case protocol.CommandSessionSend, protocol.CommandPermissionRespond, protocol.CommandSessionInterrupt, protocol.CommandSessionStop:
		return nil
	default:
		return fmt.Errorf("unsupported command type %q", cmd.Type)
	}
}

func commandID(cmd *protocol.Command) string {
	if cmd == nil {
		return ""
	}
	return cmd.CommandID
}

func subscribesTo(subscriptions []protocol.Subscription, sessionID string) bool {
	for _, sub := range subscriptions {
		if sub.SessionID == sessionID {
			return true
		}
	}
	return false
}

func commandNeedsPersistence(commandType protocol.CommandType) bool {
	return commandType == protocol.CommandSessionSend || commandType == protocol.CommandPermissionRespond
}

func commandCanBuffer(commandType protocol.CommandType) bool {
	return commandType == protocol.CommandSessionSend || commandType == protocol.CommandPermissionRespond
}

func commandEventPayload(cmd *protocol.Command) (string, json.RawMessage, error) {
	switch cmd.Type {
	case protocol.CommandSessionSend:
		payload, err := userMessagePayload(cmd)
		return "session.message", payload, err
	case protocol.CommandPermissionRespond:
		if !json.Valid(cmd.Payload) {
			return "", nil, errors.New("permission response payload is invalid JSON")
		}
		return "permission.decision", clonePayload(cmd.Payload), nil
	default:
		return "", nil, fmt.Errorf("command %q has no durable event", cmd.Type)
	}
}

func userMessagePayload(cmd *protocol.Command) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if len(cmd.Payload) == 0 {
		fields = make(map[string]json.RawMessage)
	} else if err := json.Unmarshal(cmd.Payload, &fields); err != nil {
		return nil, fmt.Errorf("decode session.send payload: %w", err)
	}
	if _, ok := fields["message_id"]; !ok {
		encoded, err := json.Marshal(cmd.CommandID)
		if err != nil {
			return nil, fmt.Errorf("marshal message id: %w", err)
		}
		fields["message_id"] = encoded
	}
	role, err := json.Marshal("user")
	if err != nil {
		return nil, fmt.Errorf("marshal user role: %w", err)
	}
	fields["role"] = role
	payload, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("marshal session.message payload: %w", err)
	}
	return payload, nil
}

func permissionDecisionKey(cmd *protocol.Command) string {
	if cmd == nil || cmd.Type != protocol.CommandPermissionRespond {
		return ""
	}
	var payload struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil || payload.RequestID == "" {
		return ""
	}
	return cmd.SessionID + ":" + payload.RequestID
}

func cloneCommand(cmd *protocol.Command) protocol.Command {
	if cmd == nil {
		return protocol.Command{}
	}
	return protocol.Command{
		CommandID: cmd.CommandID,
		Type:      cmd.Type,
		SessionID: cmd.SessionID,
		Payload:   clonePayload(cmd.Payload),
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
	case "presence", "agent.activity", "log.tail", "resource.sample", "session.idle_warning":
		return true
	default:
		return false
	}
}

type clientConnection struct {
	conn            *websocket.Conn
	protocolVersion int
	writeGate       contextWriteGate

	mu            sync.Mutex
	subscriptions map[string]*subscriptionState
}

type subscriptionState struct {
	lastSeq   int64
	replaying bool
	buffered  []protocol.Event
}

func newClientConnection(conn *websocket.Conn, protocolVersion int, subscriptions []protocol.Subscription, replaying bool) *clientConnection {
	peer := &clientConnection{
		conn:            conn,
		protocolVersion: protocolVersion,
		writeGate:       newContextWriteGate(),
		subscriptions:   make(map[string]*subscriptionState, len(subscriptions)),
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
	release, err := c.writeGate.lock(ctx)
	if err != nil {
		return err
	}
	defer release()
	return writeProtocolFrame(ctx, c.conn, frame)
}

type contextWriteGate chan struct{}

func newContextWriteGate() contextWriteGate {
	gate := make(contextWriteGate, 1)
	gate <- struct{}{}
	return gate
}

func (g contextWriteGate) lock(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-g:
		return func() { g <- struct{}{} }, nil
	}
}

func (c *clientConnection) writeReplayEvent(ctx context.Context, ev protocol.Event) error {
	if !protocol.EventTypeAllowed(c.protocolVersion, ev.Type, true) {
		c.markSent(ev)
		return nil
	}
	if err := c.writeFrame(ctx, &ev); err != nil {
		return err
	}
	c.markSent(ev)
	return nil
}

func (c *clientConnection) sendLiveEvent(ctx context.Context, ev protocol.Event) error {
	if !protocol.EventTypeAllowed(c.protocolVersion, ev.Type, ev.Seq != nil) {
		c.markSent(ev)
		return nil
	}
	c.mu.Lock()
	state := c.subscriptions[ev.SessionID]
	if state == nil {
		c.mu.Unlock()
		return nil
	}
	if state.replaying {
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

func writeCommandAck(ctx context.Context, conn *websocket.Conn, commandID string, status protocol.AckStatus, reason string) error {
	return writeProtocolFrame(ctx, conn, &protocol.CommandAck{
		CommandID: commandID,
		Status:    status,
		Reason:    reason,
	})
}

func protocolErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrInvalidHello), errors.Is(err, ErrVersionUnsupported):
		return "invalid_hello"
	case errors.Is(err, auth.ErrInvalidToken), errors.Is(err, auth.ErrUnauthorized):
		return "unauthorized"
	default:
		return "internal_error"
	}
}
