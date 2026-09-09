package hub

import (
	"context"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

type e2eeTestAuth struct{ principal auth.Principal }

func (a e2eeTestAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return a.principal, nil
}
func (a e2eeTestAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}
func (a e2eeTestAuth) SessionAdmissionClaim(_ context.Context, _ auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	return auth.SessionAdmissionClaim{SessionID: sessionID, Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (a e2eeTestAuth) SessionContentMode(context.Context, auth.Principal, string) (string, error) {
	return "required", nil
}

type e2eeStore struct{}

func (e2eeStore) LatestSeq(context.Context, string) (int64, error) { return 0, nil }
func (e2eeStore) SessionAdmissionTruth(context.Context, string) (store.SessionAdmissionTruth, error) {
	return store.SessionAdmissionTruth{SessionID: "session", Exists: true, Complete: true, Live: true}, nil
}

type modeAuth struct {
	e2eeTestAuth
	mode string
}

func (m modeAuth) SessionContentMode(context.Context, auth.Principal, string) (string, error) {
	return m.mode, nil
}

type plainAuth struct{ principal auth.Principal }

func (a plainAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return a.principal, nil
}
func (a plainAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}
func (a plainAuth) SessionAdmissionClaim(_ context.Context, _ auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	return auth.SessionAdmissionClaim{SessionID: sessionID, Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func TestRequiredAdapterContentModeIsPreserved(t *testing.T) {
	principal := auth.Principal{Subject: "adapter", Scopes: []auth.Scope{auth.SessionAdapter("session")}}
	h := NewHandshake(HandshakeConfig{Authenticator: e2eeTestAuth{principal: principal}, EventStore: e2eeStore{}})
	ack, peer, err := h.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2,
		Role:            protocol.RoleAdapter,
		Token:           "token",
		SessionID:       "session",
		Provider:        "claude-code",
		ContentMode:     protocol.ContentModeRequired,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.ContentMode != protocol.ContentModeRequired || peer.ContentMode != protocol.ContentModeRequired {
		t.Fatalf("adapter mode was not preserved: ack=%q peer=%q", ack.ContentMode, peer.ContentMode)
	}
}

func TestContentModeRequiresDurableAuthorizerAndExactBinding(t *testing.T) {
	principal := auth.Principal{Subject: "client", Scopes: []auth.Scope{auth.SessionControl("session")}}
	base := e2eeTestAuth{principal: principal}
	for _, tc := range []struct {
		name    string
		a       auth.Authenticator
		mode    string
		wantErr bool
	}{
		{"missing authorizer", plainAuth(base), protocol.ContentModeRequired, true},
		{"wrong mode", modeAuth{base, protocol.ContentModeLegacy}, protocol.ContentModeRequired, true},
		{"exact required", modeAuth{base, protocol.ContentModeRequired}, protocol.ContentModeRequired, false},
		{"omitted required", modeAuth{base, protocol.ContentModeRequired}, "", true},
		{"downgraded required", modeAuth{base, protocol.ContentModeRequired}, protocol.ContentModeLegacy, true},
		{"unknown requested mode", modeAuth{base, "unknown"}, "unknown", true},
		{"unknown stored mode", modeAuth{base, "unknown"}, "", true},
		{"legacy omitted", modeAuth{base, protocol.ContentModeLegacy}, "", false},
		{"legacy explicit", modeAuth{base, protocol.ContentModeLegacy}, protocol.ContentModeLegacy, false},
		{"old authenticator legacy", plainAuth(base), protocol.ContentModeLegacy, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandshake(HandshakeConfig{Authenticator: tc.a, EventStore: e2eeStore{}})
			ack, peer, err := h.HandleHello(context.Background(), &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "token", ContentMode: tc.mode, Subscriptions: []protocol.Subscription{{SessionID: "session"}}})
			if err == nil && (ack.ContentMode != tc.mode || peer.ContentMode != tc.mode) {
				t.Fatal("accepted mode was not preserved in acknowledgement and peer")
			}
			if (err != nil) != tc.wantErr {
				t.Fatal(err)
			}
		})
	}
}
