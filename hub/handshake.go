package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

var (
	ErrInvalidHello       = errors.New("invalid hello")
	ErrVersionUnsupported = errors.New("protocol version unsupported")
)

const maxRawAttachGrantBytes = 64 * 1024

type SessionAdmissionAuthenticator interface {
	auth.Authenticator
	SessionAdmissionClaim(context.Context, auth.Principal, string) (auth.SessionAdmissionClaim, error)
}

type HandshakeConfig struct {
	Authenticator          auth.Authenticator
	AttachGrantVerifier    auth.AttachGrantVerifier
	AttachGrantAudience    string
	LiveBootstrapAuthority LiveBootstrapAuthorityResolver
	EventStore             interface {
		LatestSeq(ctx context.Context, sessionID string) (int64, error)
	}
}

type Handshake struct {
	authenticator          auth.Authenticator
	attachGrantVerifier    auth.AttachGrantVerifier
	attachGrantAudience    string
	liveBootstrapAuthority LiveBootstrapAuthorityResolver
	authorityMu            sync.RWMutex
	events                 interface {
		LatestSeq(ctx context.Context, sessionID string) (int64, error)
	}
}

type LiveBootstrapAuthorityResolver interface {
	CurrentBootstrapAuthority(context.Context, auth.AttachGrant) (auth.BootstrapAuthority, error)
}

type AcceptedPeer struct {
	Role            protocol.Role
	ProtocolVersion int
	Principal       auth.Principal
	SessionID       string
	Provider        string
	Resume          bool
	Subscribed      []protocol.Subscription
	Admissions      map[string]auth.SessionAdmissionDecision
}

func NewHandshake(cfg HandshakeConfig) *Handshake {
	events := cfg.EventStore
	if events == nil {
		events = noopEventStore{}
	}
	return &Handshake{
		authenticator:          cfg.Authenticator,
		attachGrantVerifier:    cfg.AttachGrantVerifier,
		attachGrantAudience:    cfg.AttachGrantAudience,
		liveBootstrapAuthority: cfg.LiveBootstrapAuthority,
		events:                 events,
	}
}

func (h *Handshake) SetLiveBootstrapAuthorityResolver(resolver LiveBootstrapAuthorityResolver) {
	h.authorityMu.Lock()
	defer h.authorityMu.Unlock()
	h.liveBootstrapAuthority = resolver
}

// AuthorizeAttach consumes the raw grant exactly at Client-to-Hub ingress.
// It returns verified bounded claims only; T18B must repeat bootstrap checks
// in its Store transaction before creating any durable attach state.
func (h *Handshake) AuthorizeAttach(ctx context.Context, peer AcceptedPeer, rawGrant string) (auth.AttachGrant, error) {
	if h.attachGrantVerifier == nil || h.attachGrantAudience == "" || len(rawGrant) == 0 || len(rawGrant) > maxRawAttachGrantBytes ||
		peer.Role != protocol.RoleClient || peer.ProtocolVersion != protocol.ProtocolVersionV2 {
		return auth.AttachGrant{}, auth.ErrUnauthorized
	}
	grant, err := h.attachGrantVerifier.VerifyAttachGrant(ctx, rawGrant, h.attachGrantAudience)
	decision, admitted := peer.Admissions[grant.TargetSessionID]
	if err != nil || !subscribesTo(peer.Subscribed, grant.TargetSessionID) || !admitted ||
		decision.Mode != auth.SessionAdmissionAttachOnly || decision.MayMutate {
		return auth.AttachGrant{}, auth.ErrUnauthorized
	}
	h.authorityMu.RLock()
	resolver := h.liveBootstrapAuthority
	h.authorityMu.RUnlock()
	if resolver == nil {
		return auth.AttachGrant{}, auth.ErrUnauthorized
	}
	bootstrap, err := resolver.CurrentBootstrapAuthority(ctx, grant)
	if err != nil {
		return auth.AttachGrant{}, auth.ErrUnauthorized
	}
	if err := auth.EvaluateAttachAuthorization(auth.AttachAuthorizationRequest{
		Principal: peer.Principal, Grant: grant, Bootstrap: bootstrap, ExpectedAudience: h.attachGrantAudience,
	}); err != nil {
		return auth.AttachGrant{}, auth.ErrUnauthorized
	}
	return grant, nil
}

func (h *Handshake) HandleHello(ctx context.Context, hello *protocol.Hello) (protocol.HelloAck, AcceptedPeer, error) {
	if hello == nil || hello.Token == "" {
		return protocol.HelloAck{}, AcceptedPeer{}, ErrInvalidHello
	}
	selectedVersion, err := negotiateHelloVersion(hello)
	if err != nil {
		return protocol.HelloAck{}, AcceptedPeer{}, err
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
		return h.handleClient(ctx, hello, principal, selectedVersion)
	case protocol.RoleAdapter:
		return h.handleAdapter(ctx, hello, principal, selectedVersion)
	default:
		return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: unknown role %q", ErrInvalidHello, hello.Role)
	}
}

func negotiateHelloVersion(hello *protocol.Hello) (int, error) {
	switch hello.Role {
	case protocol.RoleClient:
		selected, err := protocol.NegotiateHighestVersion(hello.ProtocolVersion, protocol.HubProtocolVersion)
		if err != nil {
			return 0, fmt.Errorf("%w: peer=%d hub=%d", ErrVersionUnsupported, hello.ProtocolVersion, protocol.HubProtocolVersion)
		}
		return selected, nil
	case protocol.RoleAdapter:
		if hello.ProtocolVersion != protocol.ProtocolVersion {
			return 0, fmt.Errorf("%w: peer=%d adapter=%d", ErrVersionUnsupported, hello.ProtocolVersion, protocol.ProtocolVersion)
		}
		return protocol.ProtocolVersion, nil
	default:
		return 0, fmt.Errorf("%w: unknown role %q", ErrInvalidHello, hello.Role)
	}
}

func (h *Handshake) handleClient(ctx context.Context, hello *protocol.Hello, principal auth.Principal, selectedVersion int) (protocol.HelloAck, AcceptedPeer, error) {
	if len(hello.Subscriptions) == 0 {
		return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: client subscriptions are required", ErrInvalidHello)
	}

	ack := protocol.HelloAck{
		ProtocolVersion: selectedVersion,
		Sessions:        make([]protocol.SessionSummary, 0, len(hello.Subscriptions)),
	}
	accepted := AcceptedPeer{
		Role: protocol.RoleClient, ProtocolVersion: selectedVersion, Principal: principal,
		Subscribed: append([]protocol.Subscription(nil), hello.Subscriptions...),
		Admissions: make(map[string]auth.SessionAdmissionDecision, len(hello.Subscriptions)),
	}

	for _, sub := range hello.Subscriptions {
		if sub.SessionID == "" || sub.LastSeq < 0 {
			return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: invalid subscription", ErrInvalidHello)
		}
		access := exactSessionAccess(principal, sub.SessionID)
		if access == "" || h.authenticator.Authorize(ctx, principal, auth.Scope{Kind: auth.KindSession, ID: sub.SessionID, Access: access}) != nil {
			return protocol.HelloAck{}, AcceptedPeer{}, auth.ErrUnauthorized
		}
		claim, decision, err := h.clientAdmission(ctx, principal, sub.SessionID, access)
		if err != nil {
			return protocol.HelloAck{}, AcceptedPeer{}, err
		}
		if decision.Mode == auth.SessionAdmissionAttachOnly &&
			(selectedVersion != protocol.ProtocolVersionV2 || len(hello.Subscriptions) != 1 ||
				!isExclusiveAttachOnlyPrincipal(principal, sub.SessionID)) {
			return protocol.HelloAck{}, AcceptedPeer{}, auth.ErrUnauthorized
		}
		state := "ready"
		if decision.Mode == auth.SessionAdmissionAttachOnly {
			state = string(auth.SessionAdmissionAttachOnly)
		}
		summary, err := h.summary(ctx, sub.SessionID, sub.LastSeq, state, claim.Provider)
		if err != nil {
			return protocol.HelloAck{}, AcceptedPeer{}, err
		}
		if decision.Mode == auth.SessionAdmissionAttachOnly && summary.LatestSeq != 0 {
			return protocol.HelloAck{}, AcceptedPeer{}, auth.ErrUnauthorized
		}
		ack.Sessions = append(ack.Sessions, summary)
		accepted.Admissions[sub.SessionID] = decision
	}

	return ack, accepted, nil
}

func (h *Handshake) handleAdapter(ctx context.Context, hello *protocol.Hello, principal auth.Principal, selectedVersion int) (protocol.HelloAck, AcceptedPeer, error) {
	if hello.SessionID == "" || hello.Provider == "" {
		return protocol.HelloAck{}, AcceptedPeer{}, fmt.Errorf("%w: adapter session_id and provider are required", ErrInvalidHello)
	}
	if !hasExactSessionAccess(principal, hello.SessionID, auth.AccessAdapter) ||
		h.authenticator.Authorize(ctx, principal, auth.SessionAdapter(hello.SessionID)) != nil {
		return protocol.HelloAck{}, AcceptedPeer{}, auth.ErrUnauthorized
	}
	claim, err := h.sessionAdmissionClaim(ctx, principal, hello.SessionID)
	if err != nil || claim.Provider != hello.Provider {
		return protocol.HelloAck{}, AcceptedPeer{}, auth.ErrUnauthorized
	}
	truth, err := h.sessionAdmissionTruth(ctx, hello.SessionID)
	if err != nil || !truth.Exists || !truth.Complete || truth.Terminal || truth.Conflicting {
		return protocol.HelloAck{}, AcceptedPeer{}, auth.ErrUnauthorized
	}
	summary, err := h.adapterSummary(ctx, hello.SessionID, claim.Provider)
	if err != nil {
		return protocol.HelloAck{}, AcceptedPeer{}, err
	}

	return protocol.HelloAck{
			ProtocolVersion: selectedVersion,
			Sessions:        []protocol.SessionSummary{summary},
		}, AcceptedPeer{
			Role: protocol.RoleAdapter, ProtocolVersion: selectedVersion, Principal: principal,
			SessionID: hello.SessionID, Provider: hello.Provider, Resume: hello.Resume,
		}, nil
}

func (h *Handshake) adapterSummary(ctx context.Context, sessionID, provider string) (protocol.SessionSummary, error) {
	summary, err := h.summary(ctx, sessionID, 0, "ready", provider)
	if err != nil {
		return protocol.SessionSummary{}, err
	}
	summary.ReplayFrom = summary.LatestSeq + 1
	return summary, nil
}

func (h *Handshake) summary(ctx context.Context, sessionID string, lastSeq int64, state, provider string) (protocol.SessionSummary, error) {
	latest, err := h.events.LatestSeq(ctx, sessionID)
	if err != nil {
		return protocol.SessionSummary{}, fmt.Errorf("latest seq for %s: %w", sessionID, err)
	}
	replayFrom := lastSeq + 1
	return protocol.SessionSummary{
		SessionID:  sessionID,
		State:      state,
		Provider:   provider,
		LatestSeq:  latest,
		ReplayFrom: replayFrom,
	}, nil
}

type noopEventStore struct{}

func (noopEventStore) LatestSeq(context.Context, string) (int64, error) {
	return 0, nil
}

type sessionAdmissionTruthStore interface {
	SessionAdmissionTruth(context.Context, string) (store.SessionAdmissionTruth, error)
}

func (h *Handshake) clientAdmission(ctx context.Context, principal auth.Principal, sessionID string, access auth.Access) (auth.SessionAdmissionClaim, auth.SessionAdmissionDecision, error) {
	claim, err := h.sessionAdmissionClaim(ctx, principal, sessionID)
	if err != nil {
		return auth.SessionAdmissionClaim{}, auth.SessionAdmissionDecision{}, err
	}
	truth, err := h.sessionAdmissionTruth(ctx, sessionID)
	if err != nil {
		return auth.SessionAdmissionClaim{}, auth.SessionAdmissionDecision{}, err
	}
	if access == auth.AccessControl {
		decision, err := auth.EvaluateSessionAdmission(auth.SessionAdmissionRequest{Principal: principal, Claim: claim, Truth: truth})
		return claim, decision, err
	}
	if access != auth.AccessView || !truth.Exists || !truth.Complete || truth.Terminal || truth.Conflicting || !truth.Live {
		return auth.SessionAdmissionClaim{}, auth.SessionAdmissionDecision{}, auth.ErrUnauthorized
	}
	return claim, auth.SessionAdmissionDecision{Mode: auth.SessionAdmissionCurrent}, nil
}

func (h *Handshake) sessionAdmissionClaim(ctx context.Context, principal auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	authenticator, ok := h.authenticator.(SessionAdmissionAuthenticator)
	if !ok {
		return auth.SessionAdmissionClaim{}, auth.ErrUnauthorized
	}
	claim, err := authenticator.SessionAdmissionClaim(ctx, principal, sessionID)
	now := time.Now()
	if err != nil || claim.SessionID != sessionID || claim.Provider == "" || !claim.ExpiresAt.After(now) || claim.ExpiresAt.After(now.Add(5*time.Minute)) {
		return auth.SessionAdmissionClaim{}, auth.ErrUnauthorized
	}
	return claim, nil
}

func (h *Handshake) sessionAdmissionTruth(ctx context.Context, sessionID string) (store.SessionAdmissionTruth, error) {
	truthStore, ok := h.events.(sessionAdmissionTruthStore)
	if !ok {
		return store.SessionAdmissionTruth{}, auth.ErrUnauthorized
	}
	truth, err := truthStore.SessionAdmissionTruth(ctx, sessionID)
	if err != nil || truth.SessionID != sessionID {
		return store.SessionAdmissionTruth{}, auth.ErrUnauthorized
	}
	return truth, nil
}

func exactSessionAccess(principal auth.Principal, sessionID string) auth.Access {
	if hasExactSessionAccess(principal, sessionID, auth.AccessControl) {
		return auth.AccessControl
	}
	if hasExactSessionAccess(principal, sessionID, auth.AccessView) {
		return auth.AccessView
	}
	return ""
}

func hasExactSessionAccess(principal auth.Principal, sessionID string, access auth.Access) bool {
	for _, scope := range principal.Scopes {
		if scope.Kind == auth.KindSession && scope.ID == sessionID && scope.Access == access {
			return true
		}
	}
	return false
}

func isExclusiveAttachOnlyPrincipal(principal auth.Principal, sessionID string) bool {
	return len(principal.Scopes) == 1 && hasExactSessionAccess(principal, sessionID, auth.AccessControl)
}

func (p AcceptedPeer) currentSubscriptions() []protocol.Subscription {
	current := make([]protocol.Subscription, 0, len(p.Subscribed))
	for _, sub := range p.Subscribed {
		if p.Admissions[sub.SessionID].Mode == auth.SessionAdmissionCurrent {
			current = append(current, sub)
		}
	}
	return current
}

func (p AcceptedPeer) allows(sessionID string, action auth.SessionAdmissionAction) bool {
	decision, ok := p.Admissions[sessionID]
	if !ok {
		return false
	}
	if action == auth.SessionAdmissionHistory {
		return decision.Mode == auth.SessionAdmissionCurrent
	}
	return decision.Allows(action)
}
