package hub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
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
const credentialRotationTTL = 15 * time.Minute

var errReplayBufferOverflow = errors.New("replay buffer overflow")
var (
	errRotationActivated       = errors.New("adapter credential rotation activated")
	errRotationFailClosed      = errors.New("adapter credential rotation fail closed")
	errSettingsTerminalReceipt = errors.New("settings terminal receipt delivery failed")
)

type WebSocketConfig struct {
	Handshake                         *Handshake
	EventStore                        store.EventStore
	ActivitySummaryStore              store.AttentionSummaryPageStore
	ActivitySink                      ActivitySink
	HandshakeTimeout                  time.Duration
	CommandActivityObserver           CommandActivityObserver
	AdapterActivityObserver           AdapterActivityObserver
	SessionCredentialIssuer           auth.SessionCredentialIssuer
	SessionCredentialLifecycle        auth.SessionCredentialLifecycle
	SessionCredentialEvidenceResolver auth.SessionCredentialEvidenceResolver
	EphemeralEventVariants            map[string]map[int]string
	// Deprecated: credential delivery is always the Hub-owned pending target
	// socket. Retained only to avoid a source-incompatible config removal.
	WarmAttachCredentialHandoff WarmAttachCredentialHandoff
}

type EphemeralBroadcaster interface {
	http.Handler
	EmitEphemeralEvent(context.Context, protocol.Event) error
	RunActivityDispatcher(context.Context) error
	RequestActivityRefresh(context.Context) error
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
	evidenceResolver := cfg.SessionCredentialEvidenceResolver
	if evidenceResolver == nil {
		evidenceResolver, _ = cfg.SessionCredentialIssuer.(auth.SessionCredentialEvidenceResolver)
	}
	if evidenceResolver == nil && cfg.Handshake != nil {
		evidenceResolver, _ = cfg.Handshake.authenticator.(auth.SessionCredentialEvidenceResolver)
	}
	handler := &webSocketHandler{
		handshake:                         cfg.Handshake,
		events:                            cfg.EventStore,
		handshakeTimeout:                  timeout,
		commandActivityObserver:           cfg.CommandActivityObserver,
		adapterActivityObserver:           cfg.AdapterActivityObserver,
		sessionCredentialIssuer:           cfg.SessionCredentialIssuer,
		sessionCredentialLifecycle:        cfg.SessionCredentialLifecycle,
		sessionCredentialEvidenceResolver: evidenceResolver,
		warmAttachCredentialHandoff:       cfg.WarmAttachCredentialHandoff,
		adapterAuthority:                  newAdapterDispatchAuthority(cfg.Handshake, cfg.EventStore),
		ephemeralEventVariants:            copyEphemeralEventVariants(cfg.EphemeralEventVariants),
		adapterAdmissionLocks:             make(map[string]chan struct{}),
		subscribers:                       make(map[string]map[*clientConnection]struct{}),
		adapters:                          make(map[string]*adapterConnection),
		attentionSubscribers:              make(map[string]map[*clientConnection]struct{}),
		attentionSubscriptions:            make(map[*clientConnection]attentionSubscription),
		pendingCommands:                   make(map[string][]queuedCommand),
		acceptedCommands:                  make(map[string]struct{}),
		decisions:                         make(map[string]struct{}),
		settingsClients:                   make(map[string]*clientConnection),
		settingsCapabilities:              make(map[string]store.SettingsCapability),
		fileReferenceCapabilities:         make(map[string]fileReferenceCapability),
		runControlClients:                 make(map[string]*clientConnection),
		pendingTargetJoins:                make(map[string]*pendingTargetJoin),
		pendingTargetJoinByAttach:         make(map[string]*pendingTargetJoin),
	}
	if cfg.ActivitySink != nil {
		pages := cfg.ActivitySummaryStore
		if pages == nil {
			pages, _ = cfg.EventStore.(store.AttentionSummaryPageStore)
		}
		if pages != nil {
			handler.activityDispatcher = NewActivityDispatcher(pages, attentionActivitySink{sink: cfg.ActivitySink, handler: handler}, ActivityDispatcherConfig{})
		} else {
			handler.activityDispatcherErr = errors.New("activity sink requires an attention summary page store")
		}
	}
	handler.publisherEphemeralTypes = publisherEphemeralTypes(handler.ephemeralEventVariants)
	// Pending target joins are Hub-owned. The historical configuration field is
	// retained for source compatibility but cannot replace this socket boundary.
	handler.warmAttachCredentialHandoff = handler
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
	if !isEphemeralEvent(ev.Type) && !h.isPublisherEphemeralSource(ev.Type) {
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
	handshake                         *Handshake
	events                            store.EventStore
	handshakeTimeout                  time.Duration
	commandActivityObserver           CommandActivityObserver
	adapterActivityObserver           AdapterActivityObserver
	adapterAuthority                  *adapterDispatchAuthority
	ephemeralEventVariants            map[string]map[int]string
	publisherEphemeralTypes           map[string]struct{}
	adapterAdmissionMu                sync.Mutex
	adapterAdmissionLocks             map[string]chan struct{}
	publication                       sessionPublicationGates
	sessionCredentialIssuer           auth.SessionCredentialIssuer
	sessionCredentialLifecycle        auth.SessionCredentialLifecycle
	sessionCredentialEvidenceResolver auth.SessionCredentialEvidenceResolver
	warmAttachCredentialHandoff       WarmAttachCredentialHandoff
	activityDispatcher                *ActivityDispatcher
	activityDispatcherErr             error

	mu                      sync.Mutex
	subscribers             map[string]map[*clientConnection]struct{}
	adapters                map[string]*adapterConnection
	attentionSubscribers    map[string]map[*clientConnection]struct{}
	attentionSubscriptions  map[*clientConnection]attentionSubscription
	nextAttentionGeneration uint64

	commandMu                 sync.Mutex
	pendingCommands           map[string][]queuedCommand
	acceptedCommands          map[string]struct{}
	acceptedCommandOrder      []string
	decisions                 map[string]struct{}
	decisionOrder             []string
	settingsClients           map[string]*clientConnection
	settingsCapabilities      map[string]store.SettingsCapability
	fileReferenceCapabilities map[string]fileReferenceCapability
	runControlClients         map[string]*clientConnection

	pendingTargetJoinMu       sync.Mutex
	pendingTargetJoins        map[string]*pendingTargetJoin
	pendingTargetJoinByAttach map[string]*pendingTargetJoin
	pendingTargetJoinTimer    func(time.Duration, func()) *time.Timer
}

type attentionSubscription struct {
	requestID   string
	sessionIDs  []string
	grant       auth.AttentionGrant
	principal   auth.Principal
	expiryTimer *time.Timer
	sent        map[string]protocol.AttentionSummary
	generation  uint64
}

// attentionActivitySink reuses the single, bounded Store activity scan for
// ledger-only summary changes. The configured sink remains the delivery owner;
// a failed callback is retried by the dispatcher before attention fanout runs.
type attentionActivitySink struct {
	sink    ActivitySink
	handler *webSocketHandler
}

func (s attentionActivitySink) PublishActivitySummary(ctx context.Context, summary ActivitySummary) error {
	if err := s.sink.PublishActivitySummary(ctx, summary); err != nil {
		return err
	}
	if s.handler != nil {
		s.handler.broadcastAttentionUpdate(ctx, summary.SessionID)
	}
	return nil
}

type fileReferenceCapability struct {
	payload  protocol.FileReferenceCapabilityPayload
	writer   store.FileReferenceWriter
	eventSeq int64
}

func (h *webSocketHandler) RunActivityDispatcher(ctx context.Context) error {
	if h.activityDispatcherErr != nil {
		return h.activityDispatcherErr
	}
	if h.activityDispatcher == nil {
		return nil
	}
	return h.activityDispatcher.Run(ctx)
}

func (h *webSocketHandler) RequestActivityRefresh(ctx context.Context) error {
	if h.activityDispatcherErr != nil {
		return h.activityDispatcherErr
	}
	if h.activityDispatcher == nil {
		return errors.New("activity dispatcher is unavailable")
	}
	return h.activityDispatcher.RequestActivityRefresh(ctx)
}

// adapterCredentialActivationRollback is an internal recovery boundary. A
// lifecycle failure after the durable CAS must restore the exact prior active
// tuple before the Hub reports rotation failure.
type sessionCredentialActivationPreflight interface {
	ValidateSessionCredentialActivation(context.Context, auth.PreparedSessionCredential) error
}

type adapterConnection struct {
	conn                 *managedConn
	writeGate            contextWriteGate
	effectMu             sync.Mutex
	sessionID            string
	provider             string
	protocolVersion      int
	handler              *webSocketHandler
	admission            store.AdapterConnectionAdmission
	credentialEvidence   auth.SessionCredentialEvidence
	settingsWriter       store.SettingsWriter
	events               *adapterEventBatcher
	providerStartAttempt int
}

type queuedCommand struct {
	command   protocol.Command
	expiresAt time.Time
}

func (h *webSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := acceptManagedConn(w, r)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	first, err := h.readHelloFrame(ctx, conn)
	if err != nil {
		_ = writeProtocolError(context.Background(), conn, "timeout", "waiting for hello", true)
		return
	}
	if join, ok := first.(*protocol.TargetJoin); ok {
		h.servePendingTargetJoin(ctx, conn, join)
		return
	}
	var adapter *adapterConnection
	accepted, historyToken, ack, err := h.acceptPeer(ctx, conn, first, &adapter)
	if err != nil {
		return
	}
	peer := h.registerPeer(conn, accepted)
	if peer != nil {
		defer h.unregisterClient(peer)
	}
	if ack != nil {
		if err := h.writeConnectionFrame(ctx, conn, peer, nil, ack); err != nil {
			return
		}
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

func (h *webSocketHandler) acceptPeer(ctx context.Context, conn *managedConn, frame protocol.Frame, adapterOut **adapterConnection) (AcceptedPeer, string, *protocol.HelloAck, error) {
	hello, ok := frame.(*protocol.Hello)
	if !ok {
		_ = writeProtocolError(ctx, conn, "invalid_hello", "first frame must be hello", true)
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid hello")
		return AcceptedPeer{}, "", nil, ErrInvalidHello
	}
	if h.handshake == nil {
		err := errors.New("websocket handshake is not configured")
		_ = writeProtocolError(ctx, conn, "internal_error", err.Error(), true)
		_ = conn.Close(websocket.StatusInternalError, "handshake not configured")
		return AcceptedPeer{}, "", nil, err
	}
	ack, accepted, err := h.handshake.HandleHello(ctx, hello)
	if err != nil {
		code := protocolErrorCode(err)
		_ = writeProtocolError(ctx, conn, code, err.Error(), true)
		_ = conn.Close(websocket.StatusPolicyViolation, code)
		return AcceptedPeer{}, "", nil, err
	}
	if accepted.Role == protocol.RoleClient && accepted.ProtocolVersion == protocol.ProtocolVersionV2 &&
		len(accepted.currentSubscriptions()) > 0 {
		if _, ok := h.events.(store.FileReferenceCommandStore); ok {
			if ack.Capabilities == nil {
				ack.Capabilities = &protocol.HelloCapabilities{}
			}
			ack.Capabilities.FileReferences = &protocol.FileReferenceHelloCapability{SchemaVersion: 1, MaxReferences: 8, MaxMetadataBytes: 8192}
		}
		if _, ok := h.events.(store.SettingsCommandStore); ok {
			if ack.Capabilities == nil {
				ack.Capabilities = &protocol.HelloCapabilities{}
			}
			if _, ok := h.events.(store.RunControlStore); ok {
				if ack.Capabilities == nil {
					ack.Capabilities = &protocol.HelloCapabilities{}
				}
				ack.Capabilities.RunControl = &protocol.RunControlCapability{SchemaVersion: 1, MaxPending: 1, CompletionTimeoutSeconds: 30}
			}
			ack.Capabilities.Settings = &protocol.SettingsCapability{SchemaVersion: protocol.SettingsCapabilitySchemaVersion, MaxPendingChanges: 1, ProviderResponseTimeoutSeconds: 30}
		}
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
		return AcceptedPeer{}, "", nil, err
	}
	if adapter != nil {
		if err := h.publishAdapterHello(ctx, adapter, &ack); err != nil {
			h.rejectAdapter(adapter)
			adapter.close()
			return AcceptedPeer{}, "", nil, err
		}
	}
	*adapterOut = adapter
	historyToken := ""
	if accepted.Role == protocol.RoleClient && accepted.ProtocolVersion == protocol.ProtocolVersionV2 {
		historyToken = hello.Token
	}
	if accepted.Role == protocol.RoleClient {
		return accepted, historyToken, &ack, nil
	}
	return accepted, historyToken, nil, nil
}

func (h *webSocketHandler) readHelloFrame(ctx context.Context, conn *managedConn) (protocol.Frame, error) {
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

func (h *webSocketHandler) readLoop(ctx context.Context, conn *managedConn, accepted AcceptedPeer, historyToken string, peer *clientConnection, adapter *adapterConnection) {
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
			if accepted.AttentionOnly {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "attention connections cannot page history"})
				continue
			}
			if err := h.handleHistoryPage(ctx, conn, accepted, historyToken, peer, adapter, typed); err != nil {
				return
			}
		case *protocol.AttentionSubscribe:
			if !accepted.AttentionOnly || peer == nil || accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "attention_unsupported", Message: "attention subscription is unavailable"})
				continue
			}
			if err := h.handleAttentionSubscribe(ctx, peer, accepted, typed); err != nil {
				_ = peer.close(websocket.StatusPolicyViolation, "attention authorization lost")
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
			if err := h.handleClientCommand(ctx, conn, peer, accepted, typed); err != nil {
				continue
			}
		case *protocol.CommandAck:
			if accepted.Role != protocol.RoleAdapter {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "client command ack frames are not accepted"})
				continue
			}
			if err := h.handleAdapterCommandAck(ctx, adapter, typed); err != nil {
				continue
			}
		case *protocol.ProviderStart:
			if accepted.Role != protocol.RoleAdapter || accepted.ProtocolVersion != protocol.ProtocolVersionV2 || adapter == nil {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "provider start is v2 Adapter-only"})
				continue
			}
			if err := h.handleProviderStart(ctx, adapter, typed); err != nil {
				return
			}
		case *protocol.CredentialRotationRequest:
			if accepted.Role != protocol.RoleAdapter || accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "credential rotation is v2 Adapter-only"})
				continue
			}
			if err := h.handleCredentialRotationRequest(ctx, adapter, typed); err != nil {
				continue
			}
		case *protocol.CredentialRotationPossession:
			if accepted.Role != protocol.RoleAdapter || accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
				_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: "credential rotation is v2 Adapter-only"})
				continue
			}
			if err := h.handleCredentialRotationPossession(ctx, adapter, typed); err != nil {
				if errors.Is(err, errRotationActivated) || errors.Is(err, errRotationFailClosed) {
					return
				}
				continue
			}
		default:
			_ = h.writeConnectionFrame(ctx, conn, peer, adapter, &protocol.Error{Code: "unsupported_frame", Message: fmt.Sprintf("unsupported frame %s", typed.FrameName())})
		}
	}
}

func (h *webSocketHandler) handleCredentialRotationRequest(ctx context.Context, adapter *adapterConnection, request *protocol.CredentialRotationRequest) error {
	if adapter == nil || request == nil || request.RotationID == "" || h.adapterAuthority == nil || h.sessionCredentialLifecycle == nil {
		return errors.New("invalid credential rotation request")
	}
	err := h.withAdapterEffect(ctx, adapter, func() error {
		connection, err := h.adapterAuthority.store.AdapterConnection(ctx, adapter.sessionID)
		if err != nil || connection.ActiveCredentialGeneration != adapter.admission.CredentialGeneration || connection.ConnectionEpoch != adapter.admission.ConnectionEpoch {
			return errAdapterAuthorityLost
		}
		if connection.PendingCredentialGeneration != nil || connection.PendingCredentialExpiresAt != nil || connection.RotationID != nil {
			if connection.PendingCredentialGeneration == nil || connection.PendingCredentialExpiresAt == nil || connection.RotationID == nil {
				return errors.New("credential rotation is already pending")
			}
			if connection.PendingCredentialExpiresAt.After(time.Now()) {
				if *connection.RotationID != request.RotationID {
					return errors.New("credential rotation is already pending")
				}
				prepared, err := h.prepareRotationCredential(ctx, adapter, request.RotationID, *connection.PendingCredentialGeneration, *connection.PendingCredentialExpiresAt)
				if err != nil {
					return err
				}
				return h.deliverRotationCredential(ctx, adapter, prepared)
			}
			if *connection.RotationID == request.RotationID {
				return errors.New("expired credential rotation requires a fresh rotation ID")
			}
		}
		if adapter.credentialEvidence.RotationID == request.RotationID && adapter.credentialEvidence.Generation == connection.ActiveCredentialGeneration && connection.PriorRecoveryGeneration != nil {
			return adapter.writeFrame(ctx, &protocol.CredentialRotationActivation{
				RotationID: request.RotationID, Generation: connection.ActiveCredentialGeneration,
				ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence,
			})
		}
		expiresAt := time.Now().UTC().Add(credentialRotationTTL).Truncate(time.Millisecond)
		prepared, err := h.prepareRotationCredential(ctx, adapter, request.RotationID, connection.CredentialGenerationHighWatermark+1, expiresAt)
		if err != nil {
			return err
		}
		pending, err := h.adapterAuthority.store.PrepareAdapterCredentialRotation(ctx, adapter.sessionID, store.AdapterCredentialRotation{
			ExpectedActiveCredentialGeneration: connection.ActiveCredentialGeneration, ExpectedEpoch: connection.ConnectionEpoch,
			PendingGeneration: prepared.Generation, ExpiresAt: prepared.ExpiresAt, RotationID: request.RotationID,
		})
		if err != nil || pending.PendingCredentialGeneration == nil || *pending.PendingCredentialGeneration != prepared.Generation || pending.RotationID == nil || *pending.RotationID != request.RotationID {
			h.discardRotationCredential(ctx, prepared)
			return errors.New("prepare credential rotation")
		}
		return h.deliverRotationCredential(ctx, adapter, prepared)
	})
	if err != nil {
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "credential_rotation_rejected", Message: "credential rotation rejected"})
	}
	return err
}

func (h *webSocketHandler) handleCredentialRotationPossession(ctx context.Context, adapter *adapterConnection, possession *protocol.CredentialRotationPossession) error {
	if adapter == nil || possession == nil || possession.SessionID != adapter.sessionID || possession.AcceptedEpoch != adapter.admission.ConnectionEpoch || h.adapterAuthority == nil {
		err := errors.New("invalid credential rotation possession")
		if adapter != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "credential_rotation_rejected", Message: "credential rotation rejected"})
		}
		return err
	}
	err := h.withAdapterEffect(ctx, adapter, func() error {
		connection, err := h.adapterAuthority.store.AdapterConnection(ctx, adapter.sessionID)
		if err != nil || connection.ActiveCredentialGeneration != adapter.admission.CredentialGeneration || connection.ConnectionEpoch != adapter.admission.ConnectionEpoch ||
			connection.PendingCredentialGeneration == nil || connection.PendingCredentialExpiresAt == nil || connection.RotationID == nil ||
			*connection.PendingCredentialGeneration != possession.Generation || *connection.RotationID != possession.RotationID {
			return errAdapterAuthorityLost
		}
		prepared, err := h.prepareRotationCredential(ctx, adapter, possession.RotationID, possession.Generation, *connection.PendingCredentialExpiresAt)
		if err != nil {
			return err
		}
		preflight, ok := h.sessionCredentialLifecycle.(sessionCredentialActivationPreflight)
		if !ok || preflight.ValidateSessionCredentialActivation(context.WithoutCancel(ctx), prepared) != nil {
			h.discardRotationCredential(ctx, prepared)
			return errors.New("validate rotated credential activation")
		}
		activated, err := h.adapterAuthority.store.ActivateAdapterCredential(ctx, adapter.sessionID, store.AdapterCredentialActivation{
			ExpectedActiveCredentialGeneration: connection.ActiveCredentialGeneration, ExpectedEpoch: connection.ConnectionEpoch,
			PendingGeneration: possession.Generation, RotationID: possession.RotationID,
		})
		if err != nil || activated.ActiveCredentialGeneration != possession.Generation || activated.ConnectionEpoch <= connection.ConnectionEpoch || activated.AcceptedFence <= connection.AcceptedFence {
			return errors.New("activate credential rotation")
		}
		if h.sessionCredentialLifecycle.ActivateSessionCredential(context.WithoutCancel(ctx), prepared) != nil {
			return errRotationFailClosed
		}
		if err := adapter.writeFrame(ctx, &protocol.CredentialRotationActivation{RotationID: possession.RotationID, Generation: activated.ActiveCredentialGeneration, ConnectionEpoch: activated.ConnectionEpoch, AcceptedFence: activated.AcceptedFence}); err != nil {
			return err
		}
		return errRotationActivated
	})
	if err != nil && !errors.Is(err, errRotationActivated) {
		_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "credential_rotation_rejected", Message: "credential rotation rejected"})
	}
	return err
}

func (h *webSocketHandler) prepareRotationCredential(ctx context.Context, adapter *adapterConnection, rotationID string, generation int64, expiresAt time.Time) (auth.PreparedSessionCredential, error) {
	if h.sessionCredentialIssuer == nil || adapter == nil || generation < 1 || !expiresAt.After(time.Now()) || rotationID == "" {
		return auth.PreparedSessionCredential{}, auth.ErrUnauthorized
	}
	lineage, ok := rotationLineage(adapter.credentialEvidence.Lineage)
	if !ok {
		return auth.PreparedSessionCredential{}, auth.ErrUnauthorized
	}
	prepared, err := h.sessionCredentialIssuer.PrepareSessionCredential(ctx, auth.SessionCredentialRequest{
		SessionID: adapter.sessionID, Lineage: lineage, Generation: generation, RotationID: rotationID, RevocationID: rotationID, ExpiresAt: expiresAt,
	})
	if err != nil || prepared.Bearer == "" || prepared.SessionID != adapter.sessionID || prepared.Lineage != lineage || prepared.Generation != generation ||
		prepared.RotationID != rotationID || prepared.RevocationID != rotationID || !prepared.ExpiresAt.Equal(expiresAt) || prepared.Scope != auth.SessionAdapter(adapter.sessionID) {
		return auth.PreparedSessionCredential{}, auth.ErrUnauthorized
	}
	return prepared, nil
}

func rotationLineage(lineage auth.SessionCredentialLineage) (auth.SessionCredentialLineage, bool) {
	switch lineage.Kind {
	case auth.SessionCredentialBootstrapInitial:
		return auth.SessionCredentialLineage{Kind: auth.SessionCredentialBootstrapInitial}, true
	case auth.SessionCredentialTargetAttach, auth.SessionCredentialTargetRotation:
		if lineage.AttachID != "" {
			return auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetRotation, AttachID: lineage.AttachID}, true
		}
	}
	return auth.SessionCredentialLineage{}, false
}

func (h *webSocketHandler) deliverRotationCredential(ctx context.Context, adapter *adapterConnection, prepared auth.PreparedSessionCredential) error {
	return h.adapterAuthority.withAdmission(ctx, adapter, func(effectCtx context.Context) error {
		return adapter.writeFrame(effectCtx, &protocol.CredentialRotationCredential{SessionID: prepared.SessionID, RotationID: prepared.RotationID, Generation: prepared.Generation, Credential: prepared.Bearer, ExpiresAt: prepared.ExpiresAt.UnixMilli()})
	})
}

func (h *webSocketHandler) discardRotationCredential(ctx context.Context, prepared auth.PreparedSessionCredential) {
	if h.sessionCredentialLifecycle != nil {
		h.sessionCredentialLifecycle.DiscardSessionCredential(ctx, prepared)
	}
}

func (h *webSocketHandler) handleHistoryPage(ctx context.Context, conn *managedConn, accepted AcceptedPeer, historyToken string, peer *clientConnection, adapter *adapterConnection, request *protocol.HistoryPageRequest) error {
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
	if err != nil || !h.validHistoryPage(page, request) ||
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

func (h *webSocketHandler) validHistoryPage(page store.HistoryPage, request *protocol.HistoryPageRequest) bool {
	if request == nil || request.Limit < 1 || request.Limit > protocol.HistoryPageMaxLimit ||
		len(page.Events) > request.Limit || page.LatestSeq < 0 ||
		(page.RetentionState != store.RetentionComplete && page.RetentionState != store.RetentionGap) {
		return false
	}
	for index, event := range page.Events {
		if event.SessionID != request.SessionID || event.Seq < 1 || event.Seq > page.LatestSeq ||
			event.Type == "" || !json.Valid(event.Payload) || len(event.Payload) > protocol.MaxEventPayloadBytes ||
			!protocol.EventTypeAllowed(protocol.ProtocolVersionV2, event.Type, true) || h.isPublisherEphemeralType(event.Type) ||
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

func (h *webSocketHandler) writeConnectionFrame(ctx context.Context, conn *managedConn, peer *clientConnection, adapter *adapterConnection, frame protocol.Frame) error {
	if peer != nil {
		return peer.writeFrame(ctx, frame)
	}
	if adapter != nil {
		return h.writeAdapterFrame(ctx, adapter, frame)
	}
	return writeProtocolFrame(ctx, conn, frame)
}

func writePongFrame(ctx context.Context, conn *managedConn, peer *clientConnection, adapter *adapterConnection, nonce string) error {
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
	if accepted.Role != protocol.RoleClient || peer == nil {
		return nil
	}
	for _, sub := range accepted.currentSubscriptions() {
		if h.events != nil {
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
		}
		if err := peer.finishReplay(ctx, sub.SessionID); err != nil {
			return err
		}
	}
	return nil
}

func (h *webSocketHandler) registerPeer(conn *managedConn, accepted AcceptedPeer) *clientConnection {
	if accepted.Role != protocol.RoleClient {
		return nil
	}
	current := accepted.currentSubscriptions()
	// Subscribe before hello.ack so no live event is lost between handshake and
	// replay. Keep every subscription replaying until the ack is on the wire:
	// sendLiveEvent then buffers concurrent fanout instead of overtaking it.
	peer := newClientConnection(conn, accepted.ProtocolVersion, current, true, h.publisherEphemeralTypes)
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

func (h *webSocketHandler) registerAdapter(ctx context.Context, conn *managedConn, accepted AcceptedPeer, token string) (*adapterConnection, error) {
	if accepted.Role != protocol.RoleAdapter {
		return nil, nil
	}
	if h.adapterAuthority == nil {
		return nil, errAdapterAuthorityLost
	}
	_, unlock := h.lockAdapterAdmission(accepted.SessionID)
	defer unlock()
	generation, credentialExpiresAt, allowInitialize, err := h.adapterAuthority.authenticate(ctx, token, accepted.Principal, accepted.SessionID)
	if err != nil {
		return nil, err
	}
	evidence, err := h.resolveAdapterCredentialEvidence(ctx, token, accepted.SessionID, generation, credentialExpiresAt)
	if err != nil {
		return nil, errAdapterAuthorityLost
	}
	admitted, err := h.adapterAuthority.admit(ctx, accepted.SessionID, generation, credentialExpiresAt, allowInitialize)
	if err != nil {
		return nil, err
	}
	adapter := &adapterConnection{conn: conn, writeGate: newContextWriteGate(), sessionID: accepted.SessionID, provider: accepted.Provider, protocolVersion: accepted.ProtocolVersion, handler: h, admission: admitted.admission, credentialEvidence: evidence, settingsWriter: admitted.writer}
	if h.events != nil {
		fencedStore := fencedAdapterEventStore{handler: h, adapter: adapter}
		adapter.events = newAdapterEventBatcher(adapterEventBatcherConfig{
			Store:     fencedStore,
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
			Publish: func(ctx context.Context, batch []pendingAdapterEvent) error {
				return fencedStore.publish(ctx, batch, h.broadcastEvent)
			},
		})
	}
	return adapter, nil
}

func (h *webSocketHandler) resolveAdapterCredentialEvidence(ctx context.Context, bearer, sessionID string, generation int64, credentialExpiresAt time.Time) (auth.SessionCredentialEvidence, error) {
	if h.sessionCredentialEvidenceResolver != nil {
		evidence, err := h.sessionCredentialEvidenceResolver.SessionCredentialEvidence(ctx, bearer)
		if err == nil {
			if evidence.SessionID != sessionID || evidence.Generation != generation || !evidence.ExpiresAt.Equal(credentialExpiresAt) ||
				!validAdapterCredentialLineage(evidence.Lineage) {
				return auth.SessionCredentialEvidence{}, auth.ErrUnauthorized
			}
			return evidence, nil
		}
	}
	if generation != 1 {
		return auth.SessionCredentialEvidence{}, auth.ErrUnauthorized
	}
	return auth.SessionCredentialEvidence{SessionID: sessionID, Lineage: auth.SessionCredentialLineage{Kind: auth.SessionCredentialBootstrapInitial}, Generation: 1}, nil
}

func validAdapterCredentialLineage(lineage auth.SessionCredentialLineage) bool {
	switch lineage.Kind {
	case auth.SessionCredentialBootstrapInitial:
		return lineage.AttachID == "" && lineage.JTI == ""
	case auth.SessionCredentialTargetAttach:
		return lineage.AttachID != "" && lineage.JTI != ""
	case auth.SessionCredentialTargetRotation:
		return lineage.AttachID != "" && lineage.JTI == ""
	default:
		return false
	}
}

func (h *webSocketHandler) publishAdapter(ctx context.Context, adapter *adapterConnection) error {
	return h.publishAdapterHello(ctx, adapter, nil)
}

// publishAdapterHello holds the per-Session admission boundary across the last
// Store proof, v2 receipt write, and live-socket publication. A replacement
// cannot interleave a valid receipt for an already fenced connection.
func (h *webSocketHandler) publishAdapterHello(ctx context.Context, adapter *adapterConnection, ack *protocol.HelloAck) error {
	if adapter == nil || h.adapterAuthority == nil {
		return errAdapterAuthorityLost
	}
	previous, unlock := h.lockAdapterAdmission(adapter.sessionID)
	defer unlock()
	if err := withAdapterAuthorityBudget(ctx, func(authorityCtx context.Context) error {
		if _, err := h.adapterAuthority.store.ValidateAdapterAdmission(authorityCtx, adapter.sessionID, adapter.admission); err != nil {
			return errAdapterAuthorityLost
		}
		if ack != nil {
			if adapter.protocolVersion == protocol.ProtocolVersionV2 {
				receipt, err := h.issueConnectionAuthorityReceipt(authorityCtx, adapter)
				if err != nil {
					return errAdapterAuthorityLost
				}
				ack.ConnectionAuthority = receipt
			}
			if err := writeProtocolFrame(authorityCtx, adapter.conn, ack); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	h.mu.Lock()
	h.adapters[adapter.sessionID] = adapter
	h.mu.Unlock()
	if previous != nil && previous.conn != nil {
		previous.conn.CloseNow()
	}
	return nil
}

func withAdapterAuthorityBudget(ctx context.Context, effect func(context.Context) error) error {
	authorityCtx, cancel := context.WithTimeout(ctx, adapterAuthorityPollInterval)
	defer cancel()
	return effect(authorityCtx)
}

func (h *webSocketHandler) issueConnectionAuthorityReceipt(ctx context.Context, adapter *adapterConnection) (*protocol.ConnectionAuthorityReceipt, error) {
	issuer, ok := h.adapterAuthority.store.(store.AdapterConnectionAuthorityReceiptStore)
	if !ok {
		return nil, errAdapterAuthorityLost
	}
	proof, err := issuer.IssueAdapterConnectionAuthorityReceipt(ctx, adapter.sessionID, adapter.admission, adapter.settingsWriter)
	if err != nil || proof.SessionID != adapter.sessionID || proof.ConnectionEpoch != adapter.admission.ConnectionEpoch ||
		proof.CredentialGeneration != adapter.admission.CredentialGeneration || proof.AcceptedFence != adapter.admission.AcceptedFence ||
		proof.WriterLeaseID != adapter.settingsWriter.LeaseID || !proof.ExpiresAt.After(time.Now()) {
		return nil, errAdapterAuthorityLost
	}
	return &protocol.ConnectionAuthorityReceipt{
		SessionID: proof.SessionID, ConnectionEpoch: proof.ConnectionEpoch, CredentialGeneration: proof.CredentialGeneration,
		AcceptedFence: proof.AcceptedFence, WriterLeaseID: proof.WriterLeaseID, ExpiresAt: proof.ExpiresAt.UnixMilli(),
	}, nil
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
	h.removeAttentionSubscriptionLocked(peer)
}

func (h *webSocketHandler) handleAttentionSubscribe(ctx context.Context, peer *clientConnection, accepted AcceptedPeer, request *protocol.AttentionSubscribe) error {
	if request == nil || peer == nil || !accepted.AttentionOnly || h.handshake == nil {
		return auth.ErrUnauthorized
	}
	grant, err := h.handshake.AuthorizeAttention(ctx, accepted.Principal)
	if err != nil {
		return err
	}
	pageStore, ok := h.events.(store.AttentionSummaryStore)
	if !ok {
		return errors.New("attention summary store is not configured")
	}
	summaries, err := pageStore.AttentionSnapshot(ctx, grant.SessionIDs)
	if err != nil {
		return err
	}
	// Recheck after the Store read so revocation/membership replacement cannot
	// race a snapshot into the websocket write path.
	fresh, err := h.handshake.AuthorizeAttention(ctx, accepted.Principal)
	if err != nil || !sameAttentionGrant(grant, fresh) {
		return auth.ErrUnauthorized
	}
	frame := attentionSummaryFrame(request.RequestID, "snapshot", summaries, fresh.SessionIDs)
	peer.attentionMu.Lock()
	defer peer.attentionMu.Unlock()
	h.mu.Lock()
	h.removeAttentionSubscriptionLocked(peer)
	h.nextAttentionGeneration++
	subscription := attentionSubscription{requestID: request.RequestID, sessionIDs: append([]string(nil), fresh.SessionIDs...), grant: copyAttentionGrant(fresh), principal: accepted.Principal, sent: attentionSummaryIndex(frame.Summaries), generation: h.nextAttentionGeneration}
	h.attentionSubscriptions[peer] = subscription
	for _, sessionID := range subscription.sessionIDs {
		if h.attentionSubscribers[sessionID] == nil {
			h.attentionSubscribers[sessionID] = make(map[*clientConnection]struct{})
		}
		h.attentionSubscribers[sessionID][peer] = struct{}{}
	}
	h.mu.Unlock()
	delay := time.Until(fresh.ExpiresAt)
	if delay <= 0 {
		h.unregisterClient(peer)
		return auth.ErrUnauthorized
	}
	generation := subscription.generation
	timer := time.AfterFunc(delay, func() {
		h.expireAttentionSubscription(peer, generation)
	})
	h.mu.Lock()
	current, found := h.attentionSubscriptions[peer]
	if found && current.requestID == request.RequestID && sameAttentionGrant(current.grant, fresh) {
		current.expiryTimer = timer
		h.attentionSubscriptions[peer] = current
		h.mu.Unlock()
	} else {
		h.mu.Unlock()
		timer.Stop()
		return auth.ErrUnauthorized
	}
	if latest, err := h.handshake.AuthorizeAttention(ctx, accepted.Principal); err != nil || !sameAttentionGrant(latest, fresh) {
		h.unregisterClient(peer)
		return auth.ErrUnauthorized
	}
	if err := peer.writeFrame(ctx, frame); err != nil {
		h.unregisterClient(peer)
		return err
	}
	return nil
}

func (h *webSocketHandler) expireAttentionSubscription(peer *clientConnection, generation uint64) {
	peer.attentionMu.Lock()
	defer peer.attentionMu.Unlock()
	h.mu.Lock()
	current, found := h.attentionSubscriptions[peer]
	if found && current.generation == generation {
		h.removeAttentionSubscriptionLocked(peer)
	}
	h.mu.Unlock()
	if found && current.generation == generation {
		_ = peer.close(websocket.StatusPolicyViolation, "attention authorization expired")
	}
}

func (h *webSocketHandler) removeAttentionSubscriptionLocked(peer *clientConnection) {
	subscription, found := h.attentionSubscriptions[peer]
	if !found {
		return
	}
	if subscription.expiryTimer != nil {
		subscription.expiryTimer.Stop()
	}
	for _, sessionID := range subscription.sessionIDs {
		delete(h.attentionSubscribers[sessionID], peer)
		if len(h.attentionSubscribers[sessionID]) == 0 {
			delete(h.attentionSubscribers, sessionID)
		}
	}
	delete(h.attentionSubscriptions, peer)
}

func (h *webSocketHandler) removeAttentionMembershipLocked(peer *clientConnection, sessionID string) {
	subscription, found := h.attentionSubscriptions[peer]
	if !found {
		return
	}
	for index, member := range subscription.sessionIDs {
		if member != sessionID {
			continue
		}
		subscription.sessionIDs = append(subscription.sessionIDs[:index], subscription.sessionIDs[index+1:]...)
		delete(h.attentionSubscribers[sessionID], peer)
		if len(h.attentionSubscribers[sessionID]) == 0 {
			delete(h.attentionSubscribers, sessionID)
		}
		h.attentionSubscriptions[peer] = subscription
		return
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
	if h.isPublisherEphemeralType(ev.Type) {
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
	durable := !isEphemeralEvent(ev.Type)
	if accepted.ProtocolVersion == protocol.ProtocolVersionV2 {
		if durable && (len(ev.ProposalID) == 0 || len(ev.ProposalID) > 255) {
			err := errors.New("v2 durable adapter events require a bounded proposal_id")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if durable && ev.Time <= 0 {
			err := errors.New("v2 durable adapter events require a positive time")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if !durable && ev.ProposalID != "" {
			err := errors.New("ephemeral adapter events must not include proposal_id")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
	} else if ev.ProposalID != "" {
		err := errors.New("v1 adapter events must not include proposal_id")
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
	if !durable {
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
	if ev.Type == "session.file_references.capabilities" {
		if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
			err := errors.New("file-reference capabilities are v2 Adapter-only")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if err := h.commitFileReferenceCapabilityProposal(ctx, adapter, out, ev.ProposalID); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
			return err
		}
		return nil
	}
	if ev.Type == "session.file_references.outcome" {
		if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
			err := errors.New("file-reference outcomes are v2 Adapter-only")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if err := h.finalizeFileReferenceOutcome(ctx, adapter, out, ev.ProposalID); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
			return err
		}
		return nil
	}
	if ev.Type == "session.settings.capabilities" {
		if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
			err := errors.New("settings capabilities are v2 Adapter-only")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if _, err := protocol.DecodeSettingsCapabilityPayload(ev.Payload); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: "invalid settings capability"})
			return err
		}
		if err := h.commitSettingsCapabilityProposal(ctx, adapter, out, ev.ProposalID); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
			return err
		}
		return nil
	}
	if ev.Type == "session.settings.effective" {
		if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
			err := errors.New("settings effective results are v2 Adapter-only")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if err := h.finalizeSettingsEffective(ctx, adapter, out, ev.ProposalID); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
			return err
		}
		return nil
	}
	if ev.Type == "session.run.capabilities" {
		if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
			err := errors.New("run-control capabilities are v2 Adapter-only")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if _, err := protocol.DecodeRunControlCapabilityPayload(ev.Payload); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: "invalid run-control capability"})
			return err
		}
		if err := h.commitRunControlCapabilityProposal(ctx, adapter, out, ev.ProposalID); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
			return err
		}
		return nil
	}
	if ev.Type == "session.run.outcome" {
		if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
			err := errors.New("run-control outcomes are v2 Adapter-only")
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "invalid_event", Message: err.Error()})
			return err
		}
		if err := h.finalizeRunControlOutcome(ctx, adapter, out, ev.ProposalID); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
			return err
		}
		return nil
	}
	if accepted.ProtocolVersion == protocol.ProtocolVersionV2 {
		if err := h.commitAdapterProposal(ctx, adapter, out, ev.ProposalID, nil); err != nil {
			_ = h.writeAdapterFrame(ctx, adapter, &protocol.Error{Code: "persist_failed", Message: err.Error()})
			return err
		}
		return nil
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

func (h *webSocketHandler) commitAdapterProposal(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string, afterCommit func(context.Context, int64) ([]protocol.Event, error)) error {
	proposals, ok := h.events.(store.ProposedEventStore)
	if !ok {
		return errors.New("proposed event store is not configured")
	}

	receiptWriteFailed := false
	if err := h.withSessionPublication(ctx, event.SessionID, func() error {
		adapter.effectMu.Lock()
		defer adapter.effectMu.Unlock()
		if err := h.validateAdapter(ctx, adapter); err != nil {
			return err
		}

		commitCtx, cancelCommit := context.WithTimeout(ctx, adapterAuthorityPollInterval)
		authority := store.CommandAuthority{
			ConnectionEpoch: adapter.admission.ConnectionEpoch, CredentialGeneration: adapter.admission.CredentialGeneration,
		}
		request := store.ProposedEventRequest{ProposalID: proposalID, Event: store.PendingEvent{
			Type: event.Type, Time: time.UnixMilli(event.Time), Payload: clonePayload(event.Payload),
		}}
		receipt, err := proposals.CommitProposedEvent(commitCtx, event.SessionID, authority, request)
		cancelCommit()
		if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
			recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), adapterAuthorityPollInterval)
			receipt, err = proposals.CommitProposedEvent(recoveryCtx, event.SessionID, authority, request)
			cancelRecovery()
		}
		if err != nil {
			return fmt.Errorf("commit adapter proposal: %w", err)
		}
		if receipt.SessionID != event.SessionID || receipt.ProposalID != proposalID || receipt.Seq < 1 || receipt.Status != store.ProposedEventAccepted {
			return errors.New("proposed event store returned an invalid receipt")
		}
		var afterEvents []protocol.Event
		if afterCommit != nil {
			afterEvents, err = afterCommit(ctx, receipt.Seq)
			if err != nil {
				return err
			}
		}

		seq := receipt.Seq
		event.Seq = &seq
		receiptCtx, cancelReceipt := context.WithTimeout(context.Background(), adapterAuthorityPollInterval)
		if err := adapter.writeFrame(receiptCtx, &protocol.EventReceipt{
			ProposalID: proposalID, Seq: receipt.Seq, Status: protocol.EventReceiptAccepted,
		}); err != nil {
			receiptWriteFailed = true
		}
		cancelReceipt()
		broadcastCtx, cancelBroadcast := context.WithTimeout(context.Background(), adapterAuthorityPollInterval)
		h.broadcastEvent(broadcastCtx, event)
		for _, afterEvent := range afterEvents {
			h.broadcastEvent(broadcastCtx, afterEvent)
		}
		cancelBroadcast()
		return nil
	}); err != nil {
		return err
	}
	if receiptWriteFailed {
		h.rejectAdapter(adapter)
	}
	return nil
}

func (h *webSocketHandler) commitSettingsCapabilityProposal(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string) error {
	ledger, ok := h.events.(store.SettingsCommandStore)
	if !ok || adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		return errors.New("settings command store is not configured")
	}
	capability, err := protocol.DecodeSettingsCapabilityPayload(event.Payload)
	if err != nil {
		return err
	}
	if err := h.commitAdapterProposal(ctx, adapter, event, proposalID, func(commitCtx context.Context, seq int64) ([]protocol.Event, error) {
		h.commandMu.Lock()
		cached, alreadyPublished := h.settingsCapabilities[event.SessionID]
		h.commandMu.Unlock()
		if alreadyPublished && cached.EventSeq == seq && cached.Fingerprint == capability.Fingerprint &&
			cached.EffectiveModelID == capability.EffectiveModelID && sameSettingsOptionalID(cached.EffectiveReasoningEffortID, capability.EffectiveReasoningEffortID) && cached.EffectivePermissionModeID == capability.EffectivePermissionModeID &&
			cached.Writer == adapter.settingsWriter {
			return nil, nil
		}
		published, err := ledger.PublishSettingsCapability(commitCtx, event.SessionID, store.SettingsCapabilityUpdate{
			EventSeq:                   seq,
			Fingerprint:                capability.Fingerprint,
			EffectiveModelID:           capability.EffectiveModelID,
			EffectiveReasoningEffortID: capability.EffectiveReasoningEffortID,
			EffectivePermissionModeID:  capability.EffectivePermissionModeID,
			Writer:                     adapter.settingsWriter,
		})
		if err != nil {
			return nil, fmt.Errorf("publish settings capability: %w", err)
		}
		h.commandMu.Lock()
		h.settingsCapabilities[event.SessionID] = published
		h.commandMu.Unlock()
		pending, err := ledger.PendingSettingsCommands(commitCtx, event.SessionID)
		if err != nil {
			return nil, fmt.Errorf("list pending settings commands: %w", err)
		}
		recovered := make([]protocol.Event, 0, len(pending))
		for _, command := range pending {
			if command.Writer == adapter.settingsWriter {
				continue
			}
			if command.Status == store.SettingsCommandDeliveryPending {
				if command.DeliveryDeadline.After(time.Now()) {
					continue
				}
				rejected, err := ledger.RecoverSettingsCommand(commitCtx, event.SessionID, command.CommandID, command.Writer)
				if err != nil {
					return nil, fmt.Errorf("recover expired settings delivery: %w", err)
				}
				if rejected.Status != store.SettingsCommandRejected || rejected.TerminalEventSeq == nil {
					return nil, errors.New("settings delivery recovery returned an invalid command")
				}
				reason := "adapter_delivery_failed"
				payload, err := store.SettingsTerminalEventPayload(rejected, rejected.ReservedCapability, rejected.Status, &reason)
				if err != nil {
					return nil, err
				}
				terminalSeq := *rejected.TerminalEventSeq
				recovered = append(recovered, protocol.Event{Type: "session.settings.effective", SessionID: event.SessionID, Seq: &terminalSeq, Time: time.Now().UTC().UnixMilli(), Payload: payload})
				continue
			}
			if command.Status != store.SettingsCommandPending && command.Status != store.SettingsCommandRecoveryPending {
				continue
			}
			if command.Status == store.SettingsCommandPending {
				command, err = ledger.RecoverSettingsCommand(commitCtx, event.SessionID, command.CommandID, command.Writer)
				if err != nil {
					return nil, fmt.Errorf("recover settings command: %w", err)
				}
				if command.Status != store.SettingsCommandRecoveryPending {
					return nil, errors.New("settings recovery returned an invalid command")
				}
			}
			reason := "recovery_unconfirmed"
			finalized, err := ledger.FinalizeSettingsCommand(commitCtx, event.SessionID, command.CommandID, store.SettingsCommandFinalize{
				ReservationVersion:  command.ReservationVersion,
				ExpectedStatus:      store.SettingsCommandRecoveryPending,
				Outcome:             store.SettingsCommandOutcomeUnknown,
				ReasonCode:          &reason,
				EffectiveCapability: published,
			})
			if err != nil || finalized.TerminalEventSeq == nil {
				if err == nil {
					err = errors.New("settings recovery returned no terminal event")
				}
				return nil, err
			}
			payload, err := store.SettingsTerminalEventPayload(finalized, published, finalized.Status, &reason)
			if err != nil {
				return nil, err
			}
			terminalSeq := *finalized.TerminalEventSeq
			recovered = append(recovered, protocol.Event{Type: "session.settings.effective", SessionID: event.SessionID, Seq: &terminalSeq, Time: time.Now().UTC().UnixMilli(), Payload: payload})
		}
		return recovered, nil
	}); err != nil {
		return err
	}
	return nil
}

func (h *webSocketHandler) commitFileReferenceCapabilityProposal(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string) error {
	ledger, ok := h.events.(store.FileReferenceCommandStore)
	if !ok || adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		return errors.New("file-reference command store is not configured")
	}
	capability, err := protocol.DecodeFileReferenceCapabilityPayload(event.Payload)
	if err != nil {
		return err
	}
	return h.commitAdapterProposal(ctx, adapter, event, proposalID, func(commitCtx context.Context, seq int64) ([]protocol.Event, error) {
		published, err := ledger.PublishFileReferenceCapability(commitCtx, event.SessionID, store.FileReferenceCapabilityUpdate{
			EventSeq: seq, Fingerprint: capability.Fingerprint, Writer: adapter.settingsWriter,
		})
		if err != nil {
			return nil, fmt.Errorf("publish file-reference capability: %w", err)
		}
		if published.EventSeq != seq || published.Fingerprint != capability.Fingerprint || published.Writer != adapter.settingsWriter {
			return nil, errors.New("file-reference capability publication is invalid")
		}
		h.commandMu.Lock()
		h.fileReferenceCapabilities[event.SessionID] = fileReferenceCapability{payload: capability, writer: published.Writer, eventSeq: published.EventSeq}
		h.commandMu.Unlock()
		return nil, nil
	})
}

func (h *webSocketHandler) finalizeFileReferenceOutcome(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string) error {
	ledger, ok := h.events.(store.FileReferenceCommandStore)
	if !ok || adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		return errors.New("file-reference command store is not configured")
	}
	proposal, err := protocol.DecodeFileReferenceOutcomePayload(event.Payload)
	if err != nil {
		return err
	}
	return h.withSessionPublication(ctx, event.SessionID, func() error {
		return h.withAdapterEffect(ctx, adapter, func() error {
			command, err := ledger.FileReferenceCommand(ctx, event.SessionID, proposal.CommandID)
			if err != nil || command.MessageID != proposal.MessageID || command.Writer == nil || *command.Writer != adapter.settingsWriter || command.Status != store.FileReferencePending {
				if err == nil {
					err = errors.New("file-reference outcome is fenced")
				}
				return err
			}
			writer := adapter.settingsWriter
			finalized, err := ledger.FinalizeFileReferenceCommand(ctx, event.SessionID, proposal.CommandID, store.FileReferenceCommandFinalize{
				ReservationVersion: command.ReservationVersion, Writer: &writer, Outcome: store.FileReferenceCommandStatus(proposal.Outcome), ReasonCode: proposal.Reason, ReferenceIndex: proposal.ReferenceIndex,
			})
			if err != nil || finalized.TerminalEventSeq == nil {
				if err == nil {
					err = errors.New("file-reference outcome finalization is invalid")
				}
				return err
			}
			stored, err := h.eventAt(ctx, event.SessionID, *finalized.TerminalEventSeq)
			if err != nil || stored.Type != "session.file_references.outcome" {
				if err == nil {
					err = errors.New("file-reference outcome event is invalid")
				}
				return err
			}
			persisted, err := protocol.DecodeFileReferenceOutcomePayload(stored.Payload)
			if err != nil || !sameFileReferenceOutcome(persisted, proposal) {
				if err == nil {
					err = errors.New("file-reference Store outcome does not match proposal")
				}
				return err
			}
			if err := adapter.writeFrame(ctx, &protocol.EventReceipt{ProposalID: proposalID, Seq: *finalized.TerminalEventSeq, Status: protocol.EventReceiptAccepted}); err != nil {
				return err
			}
			h.broadcastEvent(ctx, stored)
			return nil
		})
	})
}

func sameFileReferenceOutcome(left, right protocol.FileReferenceOutcomePayload) bool {
	if left.MessageID != right.MessageID || left.CommandID != right.CommandID || left.Outcome != right.Outcome || (left.ReferenceIndex == nil) != (right.ReferenceIndex == nil) || (left.Reason == nil) != (right.Reason == nil) {
		return false
	}
	return (left.ReferenceIndex == nil || *left.ReferenceIndex == *right.ReferenceIndex) && (left.Reason == nil || *left.Reason == *right.Reason)
}

func (h *webSocketHandler) commitRunControlCapabilityProposal(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string) error {
	ledger, ok := h.events.(store.RunControlStore)
	if !ok || adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		return errors.New("run-control store is not configured")
	}
	capability, err := protocol.DecodeRunControlCapabilityPayload(event.Payload)
	if err != nil {
		return err
	}
	return h.commitAdapterProposal(ctx, adapter, event, proposalID, func(commitCtx context.Context, seq int64) ([]protocol.Event, error) {
		if _, err := ledger.PublishRunControlCapability(commitCtx, event.SessionID, store.RunControlCapabilityUpdate{
			EventSeq: seq, InterruptSupported: capability.InterruptSupported, StopSupported: capability.StopSupported, Writer: adapter.settingsWriter,
		}); err != nil {
			return nil, fmt.Errorf("publish run-control capability: %w", err)
		}
		pending, err := ledger.PendingRunControls(commitCtx, event.SessionID)
		if err != nil {
			return nil, fmt.Errorf("list pending run controls: %w", err)
		}
		recovered := make([]protocol.Event, 0, len(pending))
		for _, reservation := range pending {
			if reservation.Writer == adapter.settingsWriter {
				continue
			}
			finalized, err := ledger.RecoverRunControl(commitCtx, event.SessionID, reservation.CommandID, "recovery_unconfirmed")
			if err != nil {
				return nil, fmt.Errorf("recover run control: %w", err)
			}
			events, err := h.runControlTerminalEvents(commitCtx, event.SessionID, finalized)
			if err != nil {
				return nil, err
			}
			recovered = append(recovered, events...)
		}
		return recovered, nil
	})
}

func isRunControlCommand(commandType protocol.CommandType) bool {
	return commandType == protocol.CommandSessionInterrupt || commandType == protocol.CommandSessionStop
}

func runControlOperation(commandType protocol.CommandType) store.RunControlOperation {
	if commandType == protocol.CommandSessionStop {
		return store.RunControlStop
	}
	return store.RunControlInterrupt
}

func (h *webSocketHandler) handleRunControl(ctx context.Context, conn *managedConn, peer *clientConnection, accepted AcceptedPeer, cmd *protocol.Command) error {
	if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "run_control_unavailable")
		return errors.New("run controls are v2 Client-only")
	}
	ledger, ok := h.events.(store.RunControlStore)
	if !ok {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "run_control_unavailable")
		return errors.New("run-control store is not configured")
	}
	adapter := h.settingsCurrentAdapter(cmd.SessionID)
	if adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "run_control_unavailable")
		return errors.New("run-control adapter is unavailable")
	}
	state, stateSeq, err := h.currentRunControlState(ctx, cmd.SessionID)
	if err != nil {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "run_control_invalid_state")
		return err
	}
	reserve, err := ledger.RunControlReserve(ctx, cmd.SessionID, store.RunControlRequest{
		CommandID: cmd.CommandID, Operation: runControlOperation(cmd.Type), PreControlState: state, PreControlStateSeq: stateSeq, Writer: adapter.settingsWriter,
	})
	if err != nil {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, runControlReserveFailureReason(ctx, ledger, cmd.SessionID, cmd.Type, err))
		return err
	}
	if reserve.Duplicate {
		return writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckDuplicate, "")
	}

	key := settingsCommandKey(cmd.SessionID, cmd.CommandID)
	h.commandMu.Lock()
	h.runControlClients[key] = peer
	h.commandMu.Unlock()
	routed := cloneCommand(cmd)
	if err := h.writeDurableAdapterFrame(ctx, adapter, &routed); err != nil {
		h.removeRunControlClient(key)
		h.unregisterAdapter(adapter)
		_, _ = ledger.RecoverRunControl(ctx, cmd.SessionID, cmd.CommandID, "adapter_disconnected")
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "adapter_delivery_failed")
		return fmt.Errorf("deliver run-control reservation: %w", err)
	}
	return nil
}

func runControlReserveFailureReason(ctx context.Context, ledger store.RunControlStore, sessionID string, commandType protocol.CommandType, err error) string {
	if err == nil {
		return "internal_error"
	}
	switch {
	case strings.Contains(err.Error(), "ID is reused"):
		return "cmd_id_reused"
	case strings.Contains(err.Error(), "unsupported"):
		if commandType == protocol.CommandSessionStop {
			return "stop_unsupported"
		}
		return "interrupt_unsupported"
	case strings.Contains(err.Error(), "stale"), strings.Contains(err.Error(), "writer is fenced"), strings.Contains(err.Error(), "capability"):
		return "run_control_unavailable"
	case strings.Contains(err.Error(), "state"):
		return "run_control_invalid_state"
	}
	if pending, pendingErr := ledger.PendingRunControls(ctx, sessionID); pendingErr == nil && len(pending) > 0 {
		return "run_control_pending"
	}
	return "internal_error"
}

func (h *webSocketHandler) currentRunControlState(ctx context.Context, sessionID string) (string, int64, error) {
	if h.events == nil {
		return "", 0, errors.New("event store is not configured")
	}
	var state string
	var stateSeq int64
	err := h.events.Replay(ctx, sessionID, 0, func(event store.Event) error {
		if event.Type != "session.state" {
			return nil
		}
		var payload struct {
			State string `json:"state"`
		}
		if json.Unmarshal(event.Payload, &payload) != nil || payload.State == "" {
			return errors.New("invalid durable session state")
		}
		state, stateSeq = payload.State, event.Seq
		return nil
	})
	if err != nil || stateSeq < 1 {
		if err == nil {
			err = errors.New("durable session state is unavailable")
		}
		return "", 0, err
	}
	return state, stateSeq, nil
}

func (h *webSocketHandler) finalizeRunControlOutcome(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string) error {
	ledger, ok := h.events.(store.RunControlStore)
	if !ok || adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		return errors.New("run-control store is not configured")
	}
	proposal, err := protocol.DecodeRunControlOutcomePayload(event.Payload)
	if err != nil {
		return err
	}
	return h.withSessionPublication(ctx, event.SessionID, func() error {
		return h.withAdapterEffect(ctx, adapter, func() error {
			reservation, err := ledger.RunControl(ctx, event.SessionID, proposal.CommandID)
			if err != nil || reservation.Outcome != store.RunControlPending || reservation.Writer != adapter.settingsWriter || string(reservation.Operation) != proposal.Operation {
				if err == nil {
					err = errors.New("run-control outcome is fenced")
				}
				return err
			}
			writer := adapter.settingsWriter
			finalized, err := ledger.RunControlFinalize(ctx, event.SessionID, proposal.CommandID, store.RunControlFinalize{
				ReservationVersion: reservation.ReservationVersion, Writer: &writer, Outcome: store.RunControlOutcome(proposal.Outcome), ReasonCode: proposal.ReasonCode,
			})
			if err != nil {
				return err
			}
			events, err := h.runControlTerminalEvents(ctx, event.SessionID, finalized)
			if err != nil {
				return err
			}
			if len(events) == 0 || events[len(events)-1].Type != "session.run.outcome" {
				return errors.New("run-control Store outcome does not match proposal")
			}
			storedOutcome, err := protocol.DecodeRunControlOutcomePayload(events[len(events)-1].Payload)
			if err != nil || !sameRunControlOutcome(storedOutcome, proposal) {
				return errors.New("run-control Store outcome does not match proposal")
			}
			if err := adapter.writeFrame(ctx, &protocol.EventReceipt{ProposalID: proposalID, Seq: *finalized.TerminalEventSeq, Status: protocol.EventReceiptAccepted}); err != nil {
				return err
			}
			for _, terminal := range events {
				h.broadcastEvent(ctx, terminal)
			}
			return nil
		})
	})
}

func sameRunControlOutcome(left, right protocol.RunControlOutcomePayload) bool {
	if left.CommandID != right.CommandID || left.Operation != right.Operation || left.Outcome != right.Outcome {
		return false
	}
	if (left.CompletionState == nil) != (right.CompletionState == nil) || (left.ReasonCode == nil) != (right.ReasonCode == nil) {
		return false
	}
	return (left.CompletionState == nil || *left.CompletionState == *right.CompletionState) &&
		(left.ReasonCode == nil || *left.ReasonCode == *right.ReasonCode)
}

func (h *webSocketHandler) runControlTerminalEvents(ctx context.Context, sessionID string, reservation store.RunControlReservation) ([]protocol.Event, error) {
	if reservation.TerminalEventSeq == nil || reservation.Outcome == store.RunControlPending {
		return nil, errors.New("run-control terminal event is unavailable")
	}
	outcome, err := h.eventAt(ctx, sessionID, *reservation.TerminalEventSeq)
	if err != nil || outcome.Type != "session.run.outcome" {
		if err == nil {
			err = errors.New("run-control terminal event is invalid")
		}
		return nil, err
	}
	if _, err := protocol.DecodeRunControlOutcomePayload(outcome.Payload); err != nil {
		return nil, err
	}
	events := []protocol.Event{outcome}
	if reservation.Outcome == store.RunControlCompleted {
		state, err := h.eventAt(ctx, sessionID, *reservation.TerminalEventSeq-1)
		if err != nil || state.Type != "session.state" {
			if err == nil {
				err = errors.New("run-control completion state is unavailable")
			}
			return nil, err
		}
		events = []protocol.Event{state, outcome}
	}
	return events, nil
}

var errRunControlEventFound = errors.New("run-control event found")

func (h *webSocketHandler) eventAt(ctx context.Context, sessionID string, seq int64) (protocol.Event, error) {
	if h.events == nil || seq < 1 {
		return protocol.Event{}, errors.New("durable event is unavailable")
	}
	var found *store.Event
	err := h.events.Replay(ctx, sessionID, seq-1, func(event store.Event) error {
		if event.Seq == seq {
			copy := event
			found = &copy
			return errRunControlEventFound
		}
		return nil
	})
	if err != nil && !errors.Is(err, errRunControlEventFound) {
		return protocol.Event{}, err
	}
	if found == nil {
		return protocol.Event{}, errors.New("durable event is missing")
	}
	foundSeq := found.Seq
	return protocol.Event{Type: found.Type, SessionID: found.SessionID, Seq: &foundSeq, Time: found.Time.UnixMilli(), Payload: clonePayload(found.Payload)}, nil
}

func (h *webSocketHandler) handleSettingsChange(ctx context.Context, conn *managedConn, peer *clientConnection, accepted AcceptedPeer, cmd *protocol.Command) error {
	if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "settings_unsupported")
		return errors.New("settings changes are v2 Client-only")
	}
	ledger, ok := h.events.(store.SettingsCommandStore)
	if !ok {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "internal_error")
		return errors.New("settings command store is not configured")
	}
	change, err := protocol.DecodeSettingsChangePayload(cmd.Payload)
	if err != nil {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "invalid_command")
		return err
	}
	adapter := h.settingsCurrentAdapter(cmd.SessionID)
	if adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "adapter_offline")
		return errors.New("settings adapter is offline")
	}

	reserve, err := ledger.SettingsCommandReserve(ctx, cmd.SessionID, store.SettingsCommandRequest{
		CommandID:                  cmd.CommandID,
		RequestFingerprint:         change.CapabilityFingerprint,
		RequestedModelID:           change.RequestedModelID,
		RequestedReasoningEffortID: change.RequestedReasoningEffortID,
		RequestedPermissionModeID:  change.RequestedPermissionModeID,
		Writer:                     adapter.settingsWriter,
	})
	if err != nil {
		reason := settingsReserveFailureReason(ctx, ledger, cmd.SessionID, err)
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, reason)
		return err
	}
	if reserve.Duplicate {
		return writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckDuplicate, "")
	}

	key := settingsCommandKey(cmd.SessionID, cmd.CommandID)
	h.commandMu.Lock()
	h.settingsClients[key] = peer
	h.commandMu.Unlock()
	routed := cloneCommand(cmd)
	if err := h.writeDurableAdapterFrame(ctx, adapter, &routed); err != nil {
		h.removeSettingsClient(key)
		h.unregisterAdapter(adapter)
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "adapter_offline")
		return fmt.Errorf("deliver settings reservation: %w", err)
	}
	return nil
}

func (h *webSocketHandler) handleProviderStart(ctx context.Context, adapter *adapterConnection, request *protocol.ProviderStart) error {
	if adapter == nil || request == nil || h.events == nil {
		return errAdapterAuthorityLost
	}
	legacy := request.Attempt == 0
	attempt := request.Attempt
	if legacy {
		attempt = 1
	}
	ack := func(status protocol.ProviderStartStatus, handle string) *protocol.ProviderStartAck {
		if legacy {
			return &protocol.ProviderStartAck{Status: status}
		}
		return &protocol.ProviderStartAck{Attempt: attempt, Status: status, RecoveryHandle: handle}
	}
	if attempt != adapter.providerStartAttempt+1 {
		return adapter.writeFrame(ctx, ack(protocol.ProviderStartRejected, ""))
	}
	admissions, ok := h.events.(store.ProviderStartAdmissionStore)
	if !ok {
		return adapter.writeFrame(ctx, ack(protocol.ProviderStartRejected, ""))
	}
	err := withAdapterAuthorityBudget(ctx, func(admissionCtx context.Context) error {
		kind := store.ProviderStartAttached
		if adapter.credentialEvidence.Lineage.Kind == auth.SessionCredentialBootstrapInitial {
			kind = store.ProviderStartStandalone
		}
		_, err := admissions.WithProviderStartAdmission(admissionCtx, store.ProviderStartAdmission{
			SessionID: adapter.sessionID, Admission: adapter.admission, Writer: adapter.settingsWriter,
			Kind: kind, ReAdmission: attempt > 1,
		}, func(startCtx context.Context) error {
			prepare := &protocol.ProviderStartPrepare{Attempt: attempt}
			if legacy {
				prepare.Attempt = 0
			}
			if err := adapter.writeFrame(startCtx, prepare); err != nil {
				return fmt.Errorf("send provider start prepare: %w", err)
			}
			frame, err := readProtocolFrame(startCtx, adapter.conn)
			if err != nil {
				return fmt.Errorf("read provider start proof: %w", err)
			}
			started, ok := frame.(*protocol.ProviderStartStarted)
			if !ok || started.Attempt != request.Attempt {
				return fmt.Errorf("provider start proof = %T", frame)
			}
			return nil
		})
		return err
	})
	if err != nil {
		return adapter.writeFrame(ctx, ack(protocol.ProviderStartRejected, ""))
	}
	handle, err := newRecoveryStartHandle()
	if err != nil {
		return adapter.writeFrame(ctx, ack(protocol.ProviderStartRejected, ""))
	}
	adapter.providerStartAttempt = attempt
	return adapter.writeFrame(ctx, ack(protocol.ProviderStartAdmitted, handle))
}

func newRecoveryStartHandle() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (h *webSocketHandler) handleAdapterCommandAck(ctx context.Context, adapter *adapterConnection, ack *protocol.CommandAck) error {
	if adapter == nil || ack == nil || ack.CommandID == "" {
		return errors.New("invalid adapter command acknowledgement")
	}
	key := settingsCommandKey(adapter.sessionID, ack.CommandID)
	if client := h.runControlClient(key); client != nil {
		return h.handleRunControlCommandAck(ctx, adapter, ack, key, client)
	}
	client := h.settingsClient(key)
	if client == nil {
		return errors.New("adapter acknowledgement has no settings reservation")
	}
	if ack.Status != protocol.AckAccepted || ack.Reason != "" || adapter.protocolVersion != protocol.ProtocolVersionV2 || h.settingsCurrentAdapter(adapter.sessionID) != adapter {
		h.removeSettingsClient(key)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "adapter_delivery_failed")
		return errors.New("settings delivery acknowledgement is rejected")
	}
	ledger, ok := h.events.(store.SettingsCommandStore)
	if !ok {
		h.removeSettingsClient(key)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "internal_error")
		return errors.New("settings command store is not configured")
	}
	command, err := ledger.SettingsCommand(ctx, adapter.sessionID, ack.CommandID)
	if err != nil || command.Writer != adapter.settingsWriter || command.Status != store.SettingsCommandDeliveryPending {
		h.removeSettingsClient(key)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "adapter_delivery_failed")
		if err == nil {
			err = errors.New("settings delivery acknowledgement is fenced")
		}
		return err
	}
	command, err = ledger.AcknowledgeSettingsCommandDelivery(ctx, adapter.sessionID, ack.CommandID, command.ReservationVersion, adapter.settingsWriter)
	if err != nil || command.Status != store.SettingsCommandPending || command.Writer != adapter.settingsWriter {
		h.removeSettingsClient(key)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "adapter_delivery_failed")
		if err == nil {
			err = errors.New("settings delivery acknowledgement returned an invalid command")
		}
		return err
	}
	if err := h.writeDurableAdapterFrame(ctx, adapter, &protocol.SettingsDeliveryExecute{
		SessionID: adapter.sessionID, CommandID: ack.CommandID, ReservationVersion: command.ReservationVersion, OperationTimeoutMS: 30000,
	}); err != nil {
		h.removeSettingsClient(key)
		h.unregisterAdapter(adapter)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "adapter_delivery_failed")
		return fmt.Errorf("deliver settings execute: %w", err)
	}
	h.removeSettingsClient(key)
	return writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckAccepted, "")
}

func (h *webSocketHandler) handleRunControlCommandAck(ctx context.Context, adapter *adapterConnection, ack *protocol.CommandAck, key string, client *clientConnection) error {
	if ack.Status != protocol.AckAccepted || ack.Reason != "" || adapter.protocolVersion != protocol.ProtocolVersionV2 || h.settingsCurrentAdapter(adapter.sessionID) != adapter {
		h.removeRunControlClient(key)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "adapter_delivery_failed")
		return errors.New("run-control delivery acknowledgement is rejected")
	}
	ledger, ok := h.events.(store.RunControlStore)
	if !ok {
		h.removeRunControlClient(key)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "internal_error")
		return errors.New("run-control store is not configured")
	}
	reservation, err := ledger.RunControl(ctx, adapter.sessionID, ack.CommandID)
	if err != nil || reservation.Outcome != store.RunControlPending || reservation.Writer != adapter.settingsWriter {
		h.removeRunControlClient(key)
		_ = writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckRejected, "adapter_delivery_failed")
		if err == nil {
			err = errors.New("run-control delivery acknowledgement is fenced")
		}
		return err
	}
	h.removeRunControlClient(key)
	return writeClientCommandAck(ctx, client, nil, ack.CommandID, protocol.AckAccepted, "")
}

func (h *webSocketHandler) finalizeSettingsEffective(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string) error {
	err := h.withSessionPublication(ctx, event.SessionID, func() error {
		return h.finalizeSettingsEffectiveLocked(ctx, adapter, event, proposalID)
	})
	if errors.Is(err, errSettingsTerminalReceipt) {
		h.rejectAdapter(adapter)
	}
	return err
}

// finalizeSettingsEffectiveLocked keeps the Store terminal CAS and its live
// fanout in the session publication order used by every other durable event.
func (h *webSocketHandler) finalizeSettingsEffectiveLocked(ctx context.Context, adapter *adapterConnection, event protocol.Event, proposalID string) error {
	ledger, ok := h.events.(store.SettingsCommandStore)
	if !ok || adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 {
		return errors.New("settings command store is not configured")
	}
	effective, err := protocol.DecodeSettingsEffectivePayload(event.Payload)
	if err != nil {
		return err
	}
	command, err := ledger.SettingsCommand(ctx, event.SessionID, effective.CommandID)
	if err != nil || command.RequestFingerprint != effective.RequestFingerprint || command.Writer != adapter.settingsWriter || command.Status != store.SettingsCommandPending {
		if err == nil {
			err = errors.New("settings effective result is fenced")
		}
		return err
	}
	h.commandMu.Lock()
	capability, found := h.settingsCapabilities[event.SessionID]
	h.commandMu.Unlock()
	if !found || capability.Fingerprint != effective.EffectiveFingerprint || capability.EffectiveModelID != effective.EffectiveModelID ||
		!sameSettingsOptionalID(capability.EffectiveReasoningEffortID, effective.EffectiveReasoningEffortID) || capability.EffectivePermissionModeID != effective.EffectivePermissionModeID || capability.Writer != adapter.settingsWriter {
		return errors.New("settings effective capability is not current")
	}
	outcome, reason := settingsFinalizationOutcome(command, capability, effective)
	writer := adapter.settingsWriter
	finalized, err := ledger.FinalizeSettingsCommand(ctx, event.SessionID, effective.CommandID, store.SettingsCommandFinalize{
		ReservationVersion:  command.ReservationVersion,
		ExpectedStatus:      store.SettingsCommandPending,
		Writer:              &writer,
		Outcome:             outcome,
		ReasonCode:          reason,
		EffectiveCapability: capability,
	})
	if err != nil || finalized.TerminalEventSeq == nil {
		if err == nil {
			err = errors.New("settings finalization returned no terminal event")
		}
		return err
	}
	payload, err := store.SettingsTerminalEventPayload(finalized, capability, finalized.Status, reason)
	if err != nil {
		return err
	}
	seq := *finalized.TerminalEventSeq
	receiptErr := adapter.writeFrame(ctx, &protocol.EventReceipt{ProposalID: proposalID, Seq: seq, Status: protocol.EventReceiptAccepted})
	h.broadcastEvent(ctx, protocol.Event{Type: "session.settings.effective", SessionID: event.SessionID, Seq: &seq, Time: time.Now().UTC().UnixMilli(), Payload: payload})
	if receiptErr != nil {
		return fmt.Errorf("%w: %w", errSettingsTerminalReceipt, receiptErr)
	}
	return nil
}

func settingsFinalizationOutcome(command store.SettingsCommand, capability store.SettingsCapability, effective protocol.SettingsEffectivePayload) (store.SettingsCommandStatus, *string) {
	outcome := store.SettingsCommandStatus(effective.Outcome)
	if outcome == store.SettingsCommandApplied {
		modelMatches := command.RequestedModelID == nil && capability.EffectiveModelID == command.ReservedCapability.EffectiveModelID ||
			command.RequestedModelID != nil && capability.EffectiveModelID == *command.RequestedModelID
		reasoningMatches := command.RequestedReasoningEffortID == nil && sameSettingsOptionalID(capability.EffectiveReasoningEffortID, command.ReservedCapability.EffectiveReasoningEffortID) ||
			command.RequestedReasoningEffortID != nil && capability.EffectiveReasoningEffortID != nil && *capability.EffectiveReasoningEffortID == *command.RequestedReasoningEffortID
		permissionMatches := command.RequestedPermissionModeID == nil && capability.EffectivePermissionModeID == command.ReservedCapability.EffectivePermissionModeID ||
			command.RequestedPermissionModeID != nil && capability.EffectivePermissionModeID == *command.RequestedPermissionModeID
		if modelMatches && reasoningMatches && permissionMatches {
			return outcome, effective.ReasonCode
		}
	} else if outcome != store.SettingsCommandOutcomeUnknown && outcome != store.SettingsCommandMismatched &&
		capability.EffectiveModelID == command.ReservedCapability.EffectiveModelID &&
		sameSettingsOptionalID(capability.EffectiveReasoningEffortID, command.ReservedCapability.EffectiveReasoningEffortID) &&
		capability.EffectivePermissionModeID == command.ReservedCapability.EffectivePermissionModeID {
		return outcome, effective.ReasonCode
	} else if outcome == store.SettingsCommandOutcomeUnknown || outcome == store.SettingsCommandMismatched {
		return outcome, effective.ReasonCode
	}
	reason := "provider_mismatched_effective"
	return store.SettingsCommandMismatched, &reason
}

func settingsReserveFailureReason(ctx context.Context, ledger store.SettingsCommandStore, sessionID string, err error) string {
	if err == nil {
		return "internal_error"
	}
	switch {
	case strings.Contains(err.Error(), "ID is reused"):
		return "cmd_id_reused"
	case strings.Contains(err.Error(), "capability is stale"), strings.Contains(err.Error(), "settings writer"):
		return "stale_capability"
	}
	if pending, pendingErr := ledger.PendingSettingsCommands(ctx, sessionID); pendingErr == nil && len(pending) > 0 {
		return "settings_change_pending"
	}
	return "internal_error"
}

func settingsCommandKey(sessionID, commandID string) string { return sessionID + "\x00" + commandID }

func sameSettingsOptionalID(left, right *string) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func (h *webSocketHandler) settingsCurrentAdapter(sessionID string) *adapterConnection {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.adapters[sessionID]
}

func (h *webSocketHandler) settingsClient(key string) *clientConnection {
	h.commandMu.Lock()
	defer h.commandMu.Unlock()
	return h.settingsClients[key]
}

func (h *webSocketHandler) removeSettingsClient(key string) {
	h.commandMu.Lock()
	delete(h.settingsClients, key)
	h.commandMu.Unlock()
}

func (h *webSocketHandler) runControlClient(key string) *clientConnection {
	h.commandMu.Lock()
	defer h.commandMu.Unlock()
	return h.runControlClients[key]
}

func (h *webSocketHandler) removeRunControlClient(key string) {
	h.commandMu.Lock()
	delete(h.runControlClients, key)
	h.commandMu.Unlock()
}

func (h *webSocketHandler) handleClientCommand(ctx context.Context, conn *managedConn, peer *clientConnection, accepted AcceptedPeer, cmd *protocol.Command) error {
	if err := validateClientCommand(cmd); err != nil {
		_ = writeCommandAck(ctx, conn, commandID(cmd), protocol.AckRejected, "invalid_command")
		return err
	}
	if cmd.Type == protocol.CommandSessionAttach {
		return h.handleWarmAttach(ctx, conn, accepted, cmd)
	}
	if !subscribesTo(accepted.Subscribed, cmd.SessionID) {
		err := fmt.Errorf("client is not subscribed to session %s", cmd.SessionID)
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "unauthorized")
		return err
	}
	if (cmd.Type == protocol.CommandSettingsChange || isRunControlCommand(cmd.Type)) && !hasLiteralSessionControl(accepted.Principal, cmd.SessionID) {
		err := errors.New("settings and run-control commands require literal session control scope")
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
	if cmd.Type == protocol.CommandSettingsChange {
		return h.handleSettingsChange(ctx, conn, peer, accepted, cmd)
	}
	if isRunControlCommand(cmd.Type) && accepted.ProtocolVersion == protocol.ProtocolVersionV2 {
		return h.handleRunControl(ctx, conn, peer, accepted, cmd)
	}
	if cmd.Type == protocol.CommandSessionSend {
		fileReferences, err := protocol.DecodeFileReferenceSendPayload(cmd.Payload)
		if err != nil {
			_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "invalid_command")
			return err
		}
		if fileReferences.HasReferences {
			return h.handleFileReferenceSend(ctx, conn, peer, accepted, cmd, fileReferences)
		}
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
		err := h.withSessionPublication(ctx, cmd.SessionID, func() error {
			ev, err := h.persistCommandEvent(ctx, cmd)
			if err != nil {
				return err
			}
			persisted = ev
			h.broadcastEvent(ctx, *persisted)
			return nil
		})
		if err != nil {
			_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "persist_failed")
			return err
		}
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

func (h *webSocketHandler) handleFileReferenceSend(ctx context.Context, conn *managedConn, peer *clientConnection, accepted AcceptedPeer, cmd *protocol.Command, request protocol.FileReferenceSendPayload) error {
	if accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
		err := errors.New("file references are v2 Client-only")
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "file_references_unsupported")
		return err
	}
	ledger, ok := h.events.(store.FileReferenceCommandStore)
	if !ok {
		err := errors.New("file-reference command store is not configured")
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "file_references_unsupported")
		return err
	}
	h.commandMu.Lock()
	if _, duplicate := h.acceptedCommands[cmd.CommandID]; duplicate {
		h.commandMu.Unlock()
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckDuplicate, "")
		return nil
	}
	capability, found := h.fileReferenceCapabilities[cmd.SessionID]
	h.commandMu.Unlock()
	h.mu.Lock()
	adapter := h.adapters[cmd.SessionID]
	h.mu.Unlock()
	if !found || capability.payload.Fingerprint != request.CapabilityFingerprint || adapter == nil || adapter.protocolVersion != protocol.ProtocolVersionV2 || capability.writer != adapter.settingsWriter || !fileReferenceRequestAllowed(request, capability.payload) {
		err := errors.New("file-reference capability is unavailable")
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "file_references_unsupported")
		return err
	}
	durablePayload, err := userMessagePayload(cmd)
	if err != nil {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "invalid_command")
		return err
	}
	forwardPayload, err := fileReferenceForwardPayload(cmd)
	if err != nil {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "invalid_command")
		return err
	}
	var persisted *protocol.Event
	var reservation store.FileReferenceCommand
	err = h.withSessionPublication(ctx, cmd.SessionID, func() error {
		reserved, err := ledger.CommitFileReferenceCommand(ctx, cmd.SessionID, store.PendingEvent{Type: "session.message", Time: time.Now().UTC(), Payload: durablePayload}, store.FileReferenceCommandRequest{
			CommandID: cmd.CommandID, MessageID: cmd.CommandID, CapabilityFingerprint: request.CapabilityFingerprint, RequestFingerprint: request.RequestFingerprint, ReferenceCount: request.ReferenceCount,
		})
		if err != nil {
			return err
		}
		if reserved.Duplicate {
			reservation = reserved.Command
			return nil
		}
		writer := adapter.settingsWriter
		reservation, err = ledger.AcknowledgeFileReferenceDelivery(ctx, cmd.SessionID, cmd.CommandID, reserved.Command.ReservationVersion, writer)
		if err != nil || reservation.Status != store.FileReferencePending || reservation.Writer == nil || *reservation.Writer != writer {
			if err == nil {
				err = errors.New("file-reference delivery acknowledgement is invalid")
			}
			return err
		}
		seq, err := h.events.LatestSeq(ctx, cmd.SessionID)
		if err != nil || seq < 1 {
			if err == nil {
				err = errors.New("file-reference message sequence is unavailable")
			}
			return err
		}
		persisted = &protocol.Event{Type: "session.message", SessionID: cmd.SessionID, Seq: &seq, Time: time.Now().UTC().UnixMilli(), Payload: durablePayload}
		h.broadcastEvent(ctx, *persisted)
		return nil
	})
	if err != nil {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckRejected, "persist_failed")
		return err
	}
	if reservation.Status != store.FileReferencePending {
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckDuplicate, "")
		return nil
	}
	routed := protocol.Command{CommandID: cmd.CommandID, Type: cmd.Type, SessionID: cmd.SessionID, Payload: forwardPayload}
	if err := h.writeDurableAdapterFrame(ctx, adapter, &routed); err != nil {
		_ = h.withSessionPublication(ctx, cmd.SessionID, func() error {
			finalized, recoverErr := ledger.RecoverFileReferenceCommand(ctx, cmd.SessionID, cmd.CommandID, "delivery_unconfirmed")
			if recoverErr != nil || finalized.TerminalEventSeq == nil {
				if recoverErr == nil {
					recoverErr = errors.New("file-reference delivery recovery returned no terminal event")
				}
				return recoverErr
			}
			terminal, recoverErr := h.eventAt(ctx, cmd.SessionID, *finalized.TerminalEventSeq)
			if recoverErr != nil || terminal.Type != "session.file_references.outcome" {
				if recoverErr == nil {
					recoverErr = errors.New("file-reference delivery recovery returned an invalid event")
				}
				return recoverErr
			}
			h.broadcastEvent(ctx, terminal)
			return nil
		})
		_ = writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckAccepted, "")
		return nil
	}
	h.commandMu.Lock()
	h.markCommandAcceptedLocked(cmd.CommandID)
	h.commandMu.Unlock()
	h.observeCommandActivity(ctx, commandActivity(cmd, persisted))
	return writeClientCommandAck(ctx, peer, conn, cmd.CommandID, protocol.AckAccepted, "")
}

func fileReferenceRequestAllowed(request protocol.FileReferenceSendPayload, capability protocol.FileReferenceCapabilityPayload) bool {
	if request.ReferenceCount < 1 || request.ReferenceCount > capability.MaxReferences || len(request.References) != request.ReferenceCount {
		return false
	}
	var total int64
	for _, reference := range request.References {
		total += reference.Bytes
		if total > capability.MaxTotalBytes {
			return false
		}
		if reference.Disposition == "file" {
			if capability.File.Mode != "allowed" || capability.File.MaxBytes == nil || reference.Bytes > *capability.File.MaxBytes {
				return false
			}
			continue
		}
		if capability.Image.Mode != "allowed" || capability.Image.MaxBytes == nil || reference.Bytes > *capability.Image.MaxBytes || reference.MediaType == nil {
			return false
		}
		allowed := false
		for _, mediaType := range capability.Image.MediaTypes {
			if *reference.MediaType == mediaType {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	return true
}

func fileReferenceForwardPayload(cmd *protocol.Command) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(cmd.Payload, &fields); err != nil {
		return nil, err
	}
	if _, found := fields["message_id"]; found {
		return nil, errors.New("client supplied file-reference message ID")
	}
	messageID, err := json.Marshal(cmd.CommandID)
	if err != nil {
		return nil, err
	}
	fields["message_id"] = messageID
	return json.Marshal(fields)
}

func (h *webSocketHandler) handleWarmAttach(ctx context.Context, conn *managedConn, accepted AcceptedPeer, cmd *protocol.Command) error {
	rawGrant, err := protocol.DecodeAttachGrantPayload(cmd.Payload)
	if err != nil {
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "invalid_command")
		return err
	}
	if h.handshake == nil {
		err := errors.New("hub handshake is not configured")
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "internal_error")
		return err
	}
	authorization, err := h.handshake.AuthorizeAttach(ctx, accepted, rawGrant)
	if err != nil || authorization.Grant.TargetSessionID != cmd.SessionID || auth.ValidateAttachCommitMaterial(authorization.Grant) != nil {
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "unauthorized")
		return auth.ErrUnauthorized
	}
	_, unlockTarget := h.lockAdapterAdmission(authorization.Grant.TargetSessionID)
	defer unlockTarget()
	warmStore, ok := h.events.(warmAttachCredentialStore)
	if !ok {
		err := errors.New("warm attach store is not configured")
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "internal_error")
		return err
	}
	activation, prepared, err := h.prepareWarmAttachCredential(ctx, authorization)
	if err != nil {
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "attach_rejected")
		return err
	}
	if err := h.beginPendingTargetJoin(ctx, authorization, activation); err != nil {
		h.discardWarmAttachCredential(ctx, prepared)
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "attach_rejected")
		return err
	}
	commit, err := warmStore.CommitWarmAttach(ctx, store.WarmAttachRequest{
		Attempt: store.AttachAttemptRequest{
			Identity: store.AttachAttemptIdentity{
				JTIHash: authorization.Grant.Commit.JTIHash, AttachID: authorization.Grant.AttachID,
				BootstrapSessionID: authorization.Grant.BootstrapSessionID, TargetSessionID: authorization.Grant.TargetSessionID,
				Provider: authorization.Grant.Provider,
			},
			Fingerprint: store.AttachAttemptFingerprint{
				Domain: authorization.Grant.Commit.Fingerprint.Domain, Version: authorization.Grant.Commit.Fingerprint.Version,
				Digest: authorization.Grant.Commit.Fingerprint.Digest, KeyVersion: authorization.Grant.Commit.Fingerprint.KeyVersion,
			},
			ExpiresAt: authorization.Grant.ExpiresAt, Outcome: store.AttachAttemptAccepted,
			IssuedCredentialGeneration: &activation.Generation,
		},
		Attachment: store.AttachmentCreate{
			Identity: store.AttachmentIdentity{
				AttachID: authorization.Grant.AttachID, BootstrapSessionID: authorization.Grant.BootstrapSessionID,
				TargetSessionID:            authorization.Grant.TargetSessionID,
				TargetCredentialLineageRef: authorization.Grant.Commit.TargetCredentialLineageRef,
			},
			ExpiresAt: activation.ExpiresAt,
		},
		TargetActivation: activation,
		BootstrapAdmission: store.AdapterConnectionAdmission{
			CredentialGeneration: authorization.Bootstrap.CredentialGeneration,
			ConnectionEpoch:      authorization.Bootstrap.ConnectionEpoch, AcceptedFence: authorization.Bootstrap.AcceptedFence,
			GrantFence: authorization.Grant.GrantFence,
		},
		FirstDelivery: store.WarmAttachFirstDelivery{
			CommandID: cmd.CommandID, ReferenceID: authorization.Grant.Commit.FirstDeliveryReferenceID,
			ReferenceDigest: authorization.Grant.Commit.FirstDeliveryReferenceDigest, ExpiresAt: activation.ExpiresAt,
		},
	})
	if err != nil {
		h.cancelPendingTargetJoin(authorization.Grant.AttachID)
		h.discardWarmAttachCredential(ctx, prepared)
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "attach_rejected")
		return fmt.Errorf("commit warm attach: %w", err)
	}
	postCommitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), warmAttachCredentialHandoffTimeout)
	defer cancel()
	if commit.Duplicate && commit.Attachment.DeliveryState == store.AttachmentDeliveryOutcomeUnknown {
		h.cancelPendingTargetJoin(authorization.Grant.AttachID)
		h.discardWarmAttachCredential(postCommitCtx, prepared)
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "attach_rejected")
		return errors.New("warm attach credential handoff outcome is unknown")
	}
	if err := h.activateCommittedWarmAttachCredential(postCommitCtx, prepared); err != nil {
		h.cancelPendingTargetJoin(authorization.Grant.AttachID)
		h.discardWarmAttachCredential(postCommitCtx, prepared)
		_ = h.failClosedWarmAttachCredentialHandoff(commit.Attachment, warmStore)
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "attach_rejected")
		return err
	}
	if err := h.handoffCommittedWarmAttachCredential(postCommitCtx, warmStore, authorization, store.AdapterConnectionAdmission{
		CredentialGeneration: authorization.Bootstrap.CredentialGeneration, ConnectionEpoch: authorization.Bootstrap.ConnectionEpoch,
		AcceptedFence: authorization.Bootstrap.AcceptedFence, GrantFence: authorization.Grant.GrantFence,
	}, commit, prepared); err != nil {
		h.cancelPendingTargetJoin(authorization.Grant.AttachID)
		h.discardWarmAttachCredential(postCommitCtx, prepared)
		_ = writeCommandAck(ctx, conn, cmd.CommandID, protocol.AckRejected, "attach_rejected")
		return err
	}
	status := protocol.AckAccepted
	if commit.Duplicate {
		status = protocol.AckDuplicate
	}
	return writeCommandAck(ctx, conn, cmd.CommandID, status, "")
}

func commandAdmissionAction(commandType protocol.CommandType) auth.SessionAdmissionAction {
	switch commandType {
	case protocol.CommandSessionSend:
		return auth.SessionAdmissionSend
	case protocol.CommandPermissionRespond:
		return auth.SessionAdmissionPermission
	case protocol.CommandSessionInterrupt, protocol.CommandSessionStop:
		return auth.SessionAdmissionRunControl
	case protocol.CommandSettingsChange:
		return auth.SessionAdmissionSettings
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
	if ledger, ok := h.events.(store.CommandLedgerStore); ok {
		authority := store.CommandAuthority{
			ConnectionEpoch:      adapter.admission.ConnectionEpoch,
			CredentialGeneration: adapter.admission.CredentialGeneration,
		}
		pending, err := ledger.ListPendingCommands(ctx, adapter.sessionID, authority)
		if err != nil {
			return fmt.Errorf("list durable pending commands: %w", err)
		}
		for _, command := range pending {
			claim, err := ledger.ClaimPendingCommand(ctx, adapter.sessionID, authority, command.CommandID)
			if err != nil {
				return fmt.Errorf("claim durable pending command %s: %w", command.CommandID, err)
			}
			if claim.Command.Status != store.PendingCommandReceived {
				continue
			}
			if !claim.Claimed {
				if _, err := ledger.ResolvePendingCommandUnknown(ctx, adapter.sessionID, command.CommandID); err != nil {
					return fmt.Errorf("resolve previously received durable command %s: %w", command.CommandID, err)
				}
				continue
			}
			routed, err := h.commandFromPendingEvent(ctx, claim.Command)
			if err != nil {
				if _, resolveErr := ledger.ResolvePendingCommandUnknown(ctx, adapter.sessionID, command.CommandID); resolveErr != nil {
					return fmt.Errorf("resolve malformed durable command %s: %w", command.CommandID, resolveErr)
				}
				continue
			}
			if err := h.writeDurableAdapterFrame(ctx, adapter, &routed); err != nil {
				_, _ = ledger.ResolvePendingCommandUnknown(ctx, adapter.sessionID, command.CommandID)
				h.unregisterAdapter(adapter)
				return fmt.Errorf("deliver durable pending command %s: %w", command.CommandID, err)
			}
			if _, err := ledger.ResolvePendingCommand(ctx, adapter.sessionID, authority, command.CommandID, store.PendingCommandCompleted); err != nil {
				_, _ = ledger.ResolvePendingCommandUnknown(ctx, adapter.sessionID, command.CommandID)
				return fmt.Errorf("resolve delivered durable command %s: %w", command.CommandID, err)
			}
		}
	}

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

func (h *webSocketHandler) writeDurableAdapterFrame(ctx context.Context, adapter *adapterConnection, frame protocol.Frame) error {
	if h.adapterAuthority == nil {
		return errAdapterAuthorityLost
	}
	adapter.effectMu.Lock()
	defer adapter.effectMu.Unlock()
	return h.adapterAuthority.withAdmission(ctx, adapter, func(effectCtx context.Context) error {
		return adapter.writeFrame(effectCtx, frame)
	})
}

var errPendingEventFound = errors.New("pending command event found")

func (h *webSocketHandler) commandFromPendingEvent(ctx context.Context, pending store.PendingCommand) (protocol.Command, error) {
	if h.events == nil {
		return protocol.Command{}, errors.New("event store is not configured")
	}
	var event *store.Event
	err := h.events.Replay(ctx, pending.SessionID, pending.EventSeq-1, func(candidate store.Event) error {
		if candidate.Seq == pending.EventSeq {
			copy := candidate
			copy.Payload = clonePayload(candidate.Payload)
			event = &copy
			return errPendingEventFound
		}
		return nil
	})
	if err != nil && !errors.Is(err, errPendingEventFound) {
		return protocol.Command{}, fmt.Errorf("replay durable command event: %w", err)
	}
	if event == nil {
		return protocol.Command{}, errors.New("durable command event is missing")
	}
	var commandType protocol.CommandType
	switch event.Type {
	case "session.message":
		commandType = protocol.CommandSessionSend
	default:
		return protocol.Command{}, fmt.Errorf("durable command event type %q is unsupported", event.Type)
	}
	return protocol.Command{CommandID: pending.CommandID, Type: commandType, SessionID: pending.SessionID, Payload: event.Payload}, nil
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
		out := ev
		if ev.Seq == nil {
			out.Type = h.selectEphemeralVariant(client.protocolVersion, ev.Type)
		}
		writeCtx, cancel := context.WithTimeout(ctx, adapterAuthorityPollInterval)
		err := client.sendLiveEvent(writeCtx, out)
		cancel()
		if err != nil {
			h.unregisterClient(client)
			if errors.Is(err, errReplayBufferOverflow) {
				_ = client.close(websocket.StatusPolicyViolation, "replay buffer overflow")
			}
		}
	}
	h.broadcastAttentionUpdate(ctx, ev.SessionID)
}

func (h *webSocketHandler) broadcastAttentionUpdate(ctx context.Context, sessionID string) {
	pageStore, ok := h.events.(store.AttentionSummaryStore)
	if !ok || h.handshake == nil {
		return
	}
	h.mu.Lock()
	targets := make([]*clientConnection, 0, len(h.attentionSubscribers[sessionID]))
	for peer := range h.attentionSubscribers[sessionID] {
		targets = append(targets, peer)
	}
	h.mu.Unlock()
	for _, peer := range targets {
		h.broadcastAttentionUpdateToPeer(ctx, pageStore, peer, sessionID)
	}
}

func (h *webSocketHandler) broadcastAttentionUpdateToPeer(ctx context.Context, pageStore store.AttentionSummaryStore, peer *clientConnection, sessionID string) {
	peer.attentionMu.Lock()
	defer peer.attentionMu.Unlock()
	h.mu.Lock()
	subscription, found := h.attentionSubscriptions[peer]
	h.mu.Unlock()
	if !found {
		return
	}

	principal := attentionPrincipalFor(peer, h)
	if principal.Subject == "" {
		h.unregisterClient(peer)
		_ = peer.close(websocket.StatusPolicyViolation, "attention authorization lost")
		return
	}
	grant, err := h.handshake.AuthorizeAttention(ctx, principal)
	if err != nil || !sameAttentionGrant(subscription.grant, grant) {
		h.unregisterClient(peer)
		_ = peer.close(websocket.StatusPolicyViolation, "attention authorization lost")
		return
	}
	summaries, err := pageStore.AttentionSnapshot(ctx, []string{sessionID})
	if err != nil {
		return
	}
	fresh, err := h.handshake.AuthorizeAttention(ctx, principal)
	if err != nil || !sameAttentionGrant(fresh, grant) {
		h.unregisterClient(peer)
		_ = peer.close(websocket.StatusPolicyViolation, "attention authorization lost")
		return
	}
	frame := attentionSummaryFrame(subscription.requestID, "update", summaries, subscription.sessionIDs)
	if attentionFrameUnchanged(frame, subscription.sent) {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, adapterAuthorityPollInterval)
	err = peer.writeFrame(writeCtx, frame)
	cancel()
	if err != nil {
		h.unregisterClient(peer)
		return
	}
	h.mu.Lock()
	current, found := h.attentionSubscriptions[peer]
	if found && current.requestID == subscription.requestID && sameSessionIDs(current.sessionIDs, subscription.sessionIDs) && sameAttentionGrant(current.grant, subscription.grant) {
		if current.sent == nil {
			current.sent = make(map[string]protocol.AttentionSummary)
		}
		for sessionID, summary := range attentionSummaryIndex(frame.Summaries) {
			current.sent[sessionID] = summary
		}
		h.attentionSubscriptions[peer] = current
	}
	h.mu.Unlock()
	for _, summary := range summaries {
		if summary.SessionID == sessionID && summary.TerminalOutcome != nil {
			h.mu.Lock()
			h.removeAttentionMembershipLocked(peer, sessionID)
			h.mu.Unlock()
		}
	}
}

func attentionPrincipalFor(peer *clientConnection, h *webSocketHandler) auth.Principal {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.attentionSubscriptions[peer].principal
}

func sameSessionIDs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameAttentionGrant(left, right auth.AttentionGrant) bool {
	return left.Subject == right.Subject && left.MaxSessions == right.MaxSessions && left.ExpiresAt.Equal(right.ExpiresAt) && sameSessionIDs(left.SessionIDs, right.SessionIDs)
}

func copyAttentionGrant(grant auth.AttentionGrant) auth.AttentionGrant {
	grant.SessionIDs = append([]string(nil), grant.SessionIDs...)
	return grant
}

func attentionSummaryFrame(requestID, kind string, summaries []store.SessionAttentionSummary, members []string) *protocol.AttentionSummaryFrame {
	memberSet := make(map[string]struct{}, len(members))
	for _, sessionID := range members {
		memberSet[sessionID] = struct{}{}
	}
	out := &protocol.AttentionSummaryFrame{RequestID: requestID, Kind: kind, SubscriptionState: "complete", Summaries: make([]protocol.AttentionSummary, 0, len(summaries))}
	for _, summary := range summaries {
		if _, authorized := memberSet[summary.SessionID]; !authorized {
			continue
		}
		converted := protocol.AttentionSummary{SessionID: summary.SessionID, LatestSeq: summary.LatestSeq, State: summary.State, TerminalOutcome: summary.TerminalOutcome, LatestChangeSeq: summary.LatestChangeSeq, SummaryVersion: summary.SummaryVersion, SummaryState: summary.StateOfProjection}
		if converted.State == "" {
			converted.State = "unknown"
		}
		if converted.SummaryState != "complete" {
			converted.SummaryState = "incomplete"
			out.SubscriptionState = "incomplete"
		}
		if summary.Permission != nil {
			converted.Permission = &protocol.AttentionPermission{ID: summary.Permission.ID, Status: summary.Permission.Status}
		}
		if summary.Blocker != nil {
			blocker := &protocol.AttentionBlocker{Kind: summary.Blocker.Kind, Reason: summary.Blocker.Reason, Operation: summary.Blocker.Operation}
			if summary.Blocker.ExpiresAt != nil {
				value := summary.Blocker.ExpiresAt.UTC().UnixMilli()
				blocker.ExpiresAt = &value
			}
			if summary.Blocker.BlockingSessionID != nil {
				if _, allowed := memberSet[*summary.Blocker.BlockingSessionID]; allowed {
					value := *summary.Blocker.BlockingSessionID
					blocker.BlockingSessionID = &value
				}
			}
			converted.Blocker = blocker
		}
		out.Summaries = append(out.Summaries, converted)
	}
	present := make(map[string]struct{}, len(out.Summaries))
	for _, summary := range out.Summaries {
		present[summary.SessionID] = struct{}{}
	}
	if kind == "snapshot" {
		for _, sessionID := range members {
			if _, found := present[sessionID]; !found {
				out.SubscriptionState = "incomplete"
				out.Summaries = append(out.Summaries, protocol.AttentionSummary{SessionID: sessionID, LatestSeq: 0, State: "unknown", SummaryVersion: 0, SummaryState: "incomplete"})
			}
		}
	}
	sort.Slice(out.Summaries, func(left, right int) bool { return out.Summaries[left].SessionID < out.Summaries[right].SessionID })
	return out
}

func attentionSummaryIndex(summaries []protocol.AttentionSummary) map[string]protocol.AttentionSummary {
	index := make(map[string]protocol.AttentionSummary, len(summaries))
	for _, summary := range summaries {
		index[summary.SessionID] = summary
	}
	return index
}

func attentionFrameUnchanged(frame *protocol.AttentionSummaryFrame, prior map[string]protocol.AttentionSummary) bool {
	if len(frame.Summaries) == 0 {
		return true
	}
	for _, summary := range frame.Summaries {
		if previous, found := prior[summary.SessionID]; !found || !reflect.DeepEqual(previous, summary) {
			return false
		}
	}
	return true
}

func (h *webSocketHandler) withSessionPublication(ctx context.Context, sessionID string, publish func() error) error {
	release, err := h.publication.acquire(ctx, sessionID)
	if err != nil {
		return err
	}
	defer release()
	return publish()
}

func validateClientCommand(cmd *protocol.Command) error {
	if cmd == nil || cmd.CommandID == "" || cmd.Type == "" || cmd.SessionID == "" {
		return errors.New("command cmd_id, type, and session_id are required")
	}
	switch cmd.Type {
	case protocol.CommandSessionSend, protocol.CommandPermissionRespond, protocol.CommandSessionInterrupt, protocol.CommandSessionStop,
		protocol.CommandSessionAttach:
		return nil
	case protocol.CommandSettingsChange:
		_, err := protocol.DecodeSettingsChangePayload(cmd.Payload)
		return err
	default:
		return fmt.Errorf("unsupported command type %q", cmd.Type)
	}
}

func hasLiteralSessionControl(principal auth.Principal, sessionID string) bool {
	for _, scope := range principal.Scopes {
		if scope.Kind == auth.KindSession && scope.ID == sessionID && scope.Access == auth.AccessControl {
			return true
		}
	}
	return false
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
	case "presence", "agent.activity", "log.tail", "resource.sample":
		return true
	default:
		return false
	}
}

func copyEphemeralEventVariants(input map[string]map[int]string) map[string]map[int]string {
	variants := make(map[string]map[int]string, len(input))
	for source, byVersion := range input {
		if !validOpaqueEventType(source) || len(byVersion) == 0 {
			continue
		}
		copied := make(map[int]string, len(byVersion))
		for version, selected := range byVersion {
			if version >= protocol.ProtocolVersion && validOpaqueEventType(selected) {
				copied[version] = selected
			}
		}
		if len(copied) > 0 {
			variants[source] = copied
		}
	}
	return variants
}

func publisherEphemeralTypes(variants map[string]map[int]string) map[string]struct{} {
	types := make(map[string]struct{})
	for source, byVersion := range variants {
		types[source] = struct{}{}
		for _, selected := range byVersion {
			types[selected] = struct{}{}
		}
	}
	return types
}

func validOpaqueEventType(eventType string) bool {
	return len(eventType) > 0 && len(eventType) <= 255
}

func (h *webSocketHandler) isPublisherEphemeralType(eventType string) bool {
	_, ok := h.publisherEphemeralTypes[eventType]
	return ok
}

func (h *webSocketHandler) isPublisherEphemeralSource(eventType string) bool {
	_, ok := h.ephemeralEventVariants[eventType]
	return ok
}

func (h *webSocketHandler) selectEphemeralVariant(version int, eventType string) string {
	if selected := h.ephemeralEventVariants[eventType][version]; selected != "" {
		return selected
	}
	return eventType
}

type clientConnection struct {
	conn                    *managedConn
	protocolVersion         int
	publisherEphemeralTypes map[string]struct{}
	writeGate               contextWriteGate

	mu            sync.Mutex
	attentionMu   sync.Mutex
	subscriptions map[string]*subscriptionState
}

type subscriptionState struct {
	lastSeq            int64
	replaying          bool
	buffered           []protocol.Event
	bufferedEphemerals map[string]int
}

func newClientConnection(conn *managedConn, protocolVersion int, subscriptions []protocol.Subscription, replaying bool, publisherEphemeralTypes map[string]struct{}) *clientConnection {
	peer := &clientConnection{
		conn:                    conn,
		protocolVersion:         protocolVersion,
		writeGate:               newContextWriteGate(),
		publisherEphemeralTypes: publisherEphemeralTypes,
		subscriptions:           make(map[string]*subscriptionState, len(subscriptions)),
	}
	for _, sub := range subscriptions {
		peer.subscriptions[sub.SessionID] = &subscriptionState{
			lastSeq:            sub.LastSeq,
			replaying:          replaying,
			bufferedEphemerals: make(map[string]int),
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
	if !protocol.EventTypeAllowed(c.protocolVersion, ev.Type, true) || c.isPublisherEphemeralType(ev.Type) {
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
	if !protocol.EventTypeAllowed(c.protocolVersion, ev.Type, ev.Seq != nil) || (ev.Seq != nil && c.isPublisherEphemeralType(ev.Type)) {
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
		if ev.Seq == nil {
			if index, found := state.bufferedEphemerals[ev.Type]; found {
				state.buffered[index] = cloneProtocolEvent(ev)
				c.mu.Unlock()
				return nil
			}
		}
		if len(state.buffered) >= maxReplayBufferedEvents {
			c.mu.Unlock()
			return errReplayBufferOverflow
		}
		state.buffered = append(state.buffered, cloneProtocolEvent(ev))
		if ev.Seq == nil {
			state.bufferedEphemerals[ev.Type] = len(state.buffered) - 1
		}
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

func (c *clientConnection) isPublisherEphemeralType(eventType string) bool {
	_, ok := c.publisherEphemeralTypes[eventType]
	return ok
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
		clear(state.bufferedEphemerals)
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

func readProtocolFrame(ctx context.Context, conn *managedConn) (protocol.Frame, error) {
	messageType, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if websocket.MessageType(messageType) != websocket.MessageText {
		return nil, fmt.Errorf("expected text websocket message, got %v", messageType)
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		return nil, err
	}
	return frame, nil
}

func writeProtocolFrame(ctx context.Context, conn *managedConn, frame protocol.Frame) error {
	data, err := protocol.Encode(frame)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func writeProtocolError(ctx context.Context, conn *managedConn, code string, message string, fatal bool) error {
	return writeProtocolFrame(ctx, conn, &protocol.Error{
		Code:    code,
		Message: message,
		Fatal:   fatal,
	})
}

func writeCommandAck(ctx context.Context, conn *managedConn, commandID string, status protocol.AckStatus, reason string) error {
	return writeProtocolFrame(ctx, conn, &protocol.CommandAck{
		CommandID: commandID,
		Status:    status,
		Reason:    reason,
	})
}

func writeClientCommandAck(ctx context.Context, peer *clientConnection, conn *managedConn, commandID string, status protocol.AckStatus, reason string) error {
	ack := &protocol.CommandAck{CommandID: commandID, Status: status, Reason: reason}
	if peer != nil {
		return peer.writeFrame(ctx, ack)
	}
	return writeCommandAck(ctx, conn, commandID, status, reason)
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
