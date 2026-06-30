package hub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
)

func TestHandshakeClientHello(t *testing.T) {
	t.Parallel()

	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{
			token: "client-token",
			principal: auth.Principal{
				Subject: "client_1",
				Scopes: []auth.Scope{
					auth.SessionControl("ses_1"),
					auth.SessionView("ses_2"),
				},
			},
		},
		EventStore: fakeStore{latest: map[string]int64{
			"ses_1": 57,
			"ses_2": 3,
		}},
		SessionLookup: fakeSessions{
			"ses_1": {State: "ready", Provider: "claude-code"},
			"ses_2": {State: "busy", Provider: "claude-code"},
		},
	})

	ack, accepted, err := core.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions: []protocol.Subscription{
			{SessionID: "ses_1", LastSeq: 41},
			{SessionID: "ses_2", LastSeq: 0},
		},
	})
	if err != nil {
		t.Fatalf("HandleHello() error = %v", err)
	}
	if accepted.Role != protocol.RoleClient || accepted.Principal.Subject != "client_1" {
		t.Fatalf("accepted = %+v", accepted)
	}
	if ack.ProtocolVersion != protocol.ProtocolVersion {
		t.Fatalf("protocol version = %d, want %d", ack.ProtocolVersion, protocol.ProtocolVersion)
	}
	if len(ack.Sessions) != 2 {
		t.Fatalf("ack sessions = %d, want 2", len(ack.Sessions))
	}
	assertSummary(t, ack.Sessions[0], protocol.SessionSummary{
		SessionID:  "ses_1",
		State:      "ready",
		Provider:   "claude-code",
		LatestSeq:  57,
		ReplayFrom: 42,
	})
	assertSummary(t, ack.Sessions[1], protocol.SessionSummary{
		SessionID:  "ses_2",
		State:      "busy",
		Provider:   "claude-code",
		LatestSeq:  3,
		ReplayFrom: 1,
	})
}

func TestHandshakeAdapterHello(t *testing.T) {
	t.Parallel()

	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{
			token: "adapter-token",
			principal: auth.Principal{
				Subject: "adapter_1",
				Scopes:  []auth.Scope{auth.SessionAdapter("ses_1")},
			},
		},
		EventStore: fakeStore{latest: map[string]int64{"ses_1": 9}},
		SessionLookup: fakeSessions{
			"ses_1": {State: "recovering", Provider: "claude-code"},
		},
	})

	ack, accepted, err := core.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleAdapter,
		Token:           "adapter-token",
		SessionID:       "ses_1",
		Provider:        "claude-code",
		Resume:          true,
	})
	if err != nil {
		t.Fatalf("HandleHello() error = %v", err)
	}
	if accepted.Role != protocol.RoleAdapter || accepted.SessionID != "ses_1" || !accepted.Resume {
		t.Fatalf("accepted = %+v", accepted)
	}
	if len(ack.Sessions) != 1 {
		t.Fatalf("ack sessions = %d, want 1", len(ack.Sessions))
	}
	assertSummary(t, ack.Sessions[0], protocol.SessionSummary{
		SessionID:  "ses_1",
		State:      "recovering",
		Provider:   "claude-code",
		LatestSeq:  9,
		ReplayFrom: 10,
	})
}

func TestHandshakeRejectsInvalidHello(t *testing.T) {
	t.Parallel()

	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{
			token: "client-token",
			principal: auth.Principal{
				Subject: "client_1",
				Scopes:  []auth.Scope{auth.SessionView("ses_1")},
			},
		},
		EventStore:    fakeStore{latest: map[string]int64{"ses_1": 1}},
		SessionLookup: fakeSessions{"ses_1": {State: "ready", Provider: "claude-code"}},
	})

	tests := []struct {
		name string
		in   *protocol.Hello
		want error
	}{
		{
			name: "version incompatible",
			in: &protocol.Hello{
				ProtocolVersion: 2,
				Role:            protocol.RoleClient,
				Token:           "client-token",
				Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
			},
			want: hub.ErrVersionUnsupported,
		},
		{
			name: "bad token",
			in: &protocol.Hello{
				ProtocolVersion: protocol.ProtocolVersion,
				Role:            protocol.RoleClient,
				Token:           "bad-token",
				Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
			},
			want: auth.ErrInvalidToken,
		},
		{
			name: "missing client subscriptions",
			in: &protocol.Hello{
				ProtocolVersion: protocol.ProtocolVersion,
				Role:            protocol.RoleClient,
				Token:           "client-token",
			},
			want: hub.ErrInvalidHello,
		},
		{
			name: "missing adapter session",
			in: &protocol.Hello{
				ProtocolVersion: protocol.ProtocolVersion,
				Role:            protocol.RoleAdapter,
				Token:           "client-token",
				Provider:        "claude-code",
			},
			want: hub.ErrInvalidHello,
		},
		{
			name: "client token cannot act as adapter",
			in: &protocol.Hello{
				ProtocolVersion: protocol.ProtocolVersion,
				Role:            protocol.RoleAdapter,
				Token:           "client-token",
				SessionID:       "ses_1",
				Provider:        "claude-code",
			},
			want: auth.ErrUnauthorized,
		},
		{
			name: "unknown role",
			in: &protocol.Hello{
				ProtocolVersion: protocol.ProtocolVersion,
				Role:            protocol.Role("future"),
				Token:           "client-token",
			},
			want: hub.ErrInvalidHello,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, _, err := core.HandleHello(context.Background(), tt.in); !errors.Is(err, tt.want) {
				t.Fatalf("HandleHello() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestHandshakeRejectsUnauthorizedSubscription(t *testing.T) {
	t.Parallel()

	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{
			token: "client-token",
			principal: auth.Principal{
				Subject: "client_1",
				Scopes:  []auth.Scope{auth.SessionView("ses_1")},
			},
		},
		EventStore:    fakeStore{latest: map[string]int64{"ses_2": 1}},
		SessionLookup: fakeSessions{"ses_2": {State: "ready", Provider: "claude-code"}},
	})

	_, _, err := core.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_2"}},
	})
	if !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("HandleHello() error = %v, want ErrUnauthorized", err)
	}
}

func assertSummary(t *testing.T, got protocol.SessionSummary, want protocol.SessionSummary) {
	t.Helper()

	if got != want {
		t.Fatalf("summary = %+v, want %+v", got, want)
	}
}

type fakeAuth struct {
	token     string
	principal auth.Principal
}

func (f fakeAuth) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	if token != f.token {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return f.principal, nil
}

func (f fakeAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}

type fakeStore struct {
	latest map[string]int64
}

func (f fakeStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	return f.latest[sessionID], nil
}

type fakeSessions map[string]hub.SessionInfo

func (f fakeSessions) LookupSession(_ context.Context, sessionID string) (hub.SessionInfo, error) {
	info, ok := f[sessionID]
	if !ok {
		return hub.SessionInfo{}, hub.ErrSessionNotFound
	}
	return info, nil
}
