package hub

import (
	"context"
	"errors"
	"fmt"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
)

var (
	ErrInvalidHello       = errors.New("invalid hello")
	ErrVersionUnsupported = errors.New("protocol version unsupported")
	ErrSessionNotFound    = errors.New("session not found")
)

type SessionInfo struct {
	State    string
	Provider string
}

type SessionLookup interface {
	LookupSession(ctx context.Context, sessionID string) (SessionInfo, error)
}

type HandshakeConfig struct {
	Authenticator auth.Authenticator
	EventStore    interface {
		LatestSeq(ctx context.Context, sessionID string) (int64, error)
	}
	SessionLookup SessionLookup
}

type Handshake struct {
	authenticator auth.Authenticator
	events        interface {
		LatestSeq(ctx context.Context, sessionID string) (int64, error)
	}
	sessions SessionLookup
}

type AcceptedPeer struct {
	Role       protocol.Role
	Principal  auth.Principal
	SessionID  string
	Provider   string
	Resume     bool
	Subscribed []protocol.Subscription
}

func NewHandshake(cfg HandshakeConfig) *Handshake {
	events := cfg.EventStore
	if events == nil {
		events = noopEventStore{}
	}
	sessions := cfg.SessionLookup
	if sessions == nil {
		sessions = noopSessionLookup{}
	}
	return &Handshake{
		authenticator: cfg.Authenticator,
		events:        events,
		sessions:      sessions,
	}
}

func (h *Handshake) HandleHello(ctx context.Context, hello *protocol.Hello) (protocol.HelloAck, AcceptedPeer, error) {
	if hello == nil || hello.Token == "" {
		return protocol.HelloAck{}, AcceptedPeer{}, ErrInvalidHello
	}
	if hello.ProtocolVersion != protocol.ProtocolVersion {
		return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: peer=%d hub=%d", ErrVersionUnsupported, hello.ProtocolVersion, protocol.ProtocolVersion)
	}
	if h.authenticator == nil {
		return protocol.HelloAck{}, AcceptedPeer{}, errors.New("hub authenticator is nil")
	}

	principal, err := h.authenticator.Authenticate(ctx, hello.Token)
	if err != nil {
		return protocol.HelloAck{}, AcceptedPeer{}, err
	}

	switch hello.Role {
	case protocol.RoleClient:
		return h.handleClient(ctx, hello, principal)
	case protocol.RoleAdapter:
		return h.handleAdapter(ctx, hello, principal)
	default:
		return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: unknown role %q", ErrInvalidHello, hello.Role)
	}
}

func (h *Handshake) handleClient(ctx context.Context, hello *protocol.Hello, principal auth.Principal) (protocol.HelloAck, AcceptedPeer, error) {
	if len(hello.Subscriptions) == 0 {
		return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: client subscriptions are required", ErrInvalidHello)
	}

	ack := protocol.HelloAck{
		ProtocolVersion: protocol.ProtocolVersion,
		Sessions:        make([]protocol.SessionSummary, 0, len(hello.Subscriptions)),
	}
	accepted := AcceptedPeer{
		Role:       protocol.RoleClient,
		Principal:  principal,
		Subscribed: append([]protocol.Subscription(nil), hello.Subscriptions...),
	}

	for _, sub := range hello.Subscriptions {
		if sub.SessionID == "" || sub.LastSeq < 0 {
			return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: invalid subscription", ErrInvalidHello)
		}
		if err := h.authenticator.Authorize(ctx, principal, auth.SessionView(sub.SessionID)); err != nil {
			return protocol.HelloAck{}, AcceptedPeer{}, err
		}
		summary, err := h.summary(ctx, sub.SessionID, sub.LastSeq)
		if err != nil {
			return protocol.HelloAck{}, AcceptedPeer{}, err
		}
		ack.Sessions = append(ack.Sessions, summary)
	}

	return ack, accepted, nil
}

func (h *Handshake) handleAdapter(ctx context.Context, hello *protocol.Hello, principal auth.Principal) (protocol.HelloAck, AcceptedPeer, error) {
	if hello.SessionID == "" || hello.Provider == "" {
		return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: adapter session_id and provider are required", ErrInvalidHello)
	}
	if err := h.authenticator.Authorize(ctx, principal, auth.SessionAdapter(hello.SessionID)); err != nil {
		return protocol.HelloAck{}, AcceptedPeer{}, err
	}
	summary, err := h.adapterSummary(ctx, hello.SessionID)
	if err != nil {
		return protocol.HelloAck{}, AcceptedPeer{}, err
	}

	return protocol.HelloAck{
			ProtocolVersion: protocol.ProtocolVersion,
			Sessions:        []protocol.SessionSummary{summary},
		}, AcceptedPeer{
			Role:      protocol.RoleAdapter,
			Principal: principal,
			SessionID: hello.SessionID,
			Provider:  hello.Provider,
			Resume:    hello.Resume,
		}, nil
}

func (h *Handshake) adapterSummary(ctx context.Context, sessionID string) (protocol.SessionSummary, error) {
	summary, err := h.summary(ctx, sessionID, 0)
	if err != nil {
		return protocol.SessionSummary{}, err
	}
	summary.ReplayFrom = summary.LatestSeq + 1
	return summary, nil
}

func (h *Handshake) summary(ctx context.Context, sessionID string, lastSeq int64) (protocol.SessionSummary, error) {
	info, err := h.sessions.LookupSession(ctx, sessionID)
	if err != nil {
		return protocol.SessionSummary{}, err
	}
	latest, err := h.events.LatestSeq(ctx, sessionID)
	if err != nil {
		return protocol.SessionSummary{}, fmt.Errorf("latest seq for %s: %w", sessionID, err)
	}
	replayFrom := lastSeq + 1
	return protocol.SessionSummary{
		SessionID:  sessionID,
		State:      info.State,
		Provider:   info.Provider,
		LatestSeq:  latest,
		ReplayFrom: replayFrom,
	}, nil
}

type noopEventStore struct{}

func (noopEventStore) LatestSeq(context.Context, string) (int64, error) {
	return 0, nil
}

type noopSessionLookup struct{}

func (noopSessionLookup) LookupSession(context.Context, string) (SessionInfo, error) {
	return SessionInfo{}, ErrSessionNotFound
}
