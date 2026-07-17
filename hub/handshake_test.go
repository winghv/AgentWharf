package hub_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
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
		State:      "ready",
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
		State:      "ready",
		Provider:   "claude-code",
		LatestSeq:  9,
		ReplayFrom: 10,
	})
}

func TestHandshakeAdapterRejectsExpiredAdmissionClaim(t *testing.T) {
	t.Parallel()
	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{
			token: "adapter-token", principal: auth.Principal{Subject: "adapter_1", Scopes: []auth.Scope{auth.SessionAdapter("ses_1")}},
			claim: &auth.SessionAdmissionClaim{SessionID: "ses_1", Provider: "claude-code", ExpiresAt: time.Now().Add(-time.Minute)},
		},
		EventStore: fakeStore{latest: map[string]int64{"ses_1": 9}, truth: map[string]store.SessionAdmissionTruth{
			"ses_1": {SessionID: "ses_1", Exists: true, Complete: true, Live: true},
		}},
	})
	_, _, err := core.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion, Role: protocol.RoleAdapter, Token: "adapter-token", SessionID: "ses_1", Provider: "claude-code",
	})
	if !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("HandleHello() error = %v, want unauthorized", err)
	}
}

func TestHandshakeNegotiatesClientV2AndRetainsVersion(t *testing.T) {
	t.Parallel()
	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{token: "client-token", principal: auth.Principal{
			Subject: "client_1", Scopes: []auth.Scope{auth.SessionView("ses_1")},
		}},
		EventStore: fakeStore{latest: map[string]int64{"ses_1": 1}},
	})
	ack, accepted, err := core.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
	})
	if err != nil {
		t.Fatalf("HandleHello() error = %v", err)
	}
	if ack.ProtocolVersion != protocol.ProtocolVersionV2 || accepted.ProtocolVersion != protocol.ProtocolVersionV2 {
		t.Fatalf("negotiated ack/peer versions = %d/%d, want 2/2", ack.ProtocolVersion, accepted.ProtocolVersion)
	}
	if ack.Capabilities != nil {
		t.Fatalf("history capability advertised before handler readiness: %+v", ack.Capabilities)
	}
}

func TestHandshakeAuthorizesVerifiedAttachGrant(t *testing.T) {
	now := time.Now()
	grant := auth.AttachGrant{
		Audience: "deploy-attach", JTI: "jti_1", AttachID: "attach_1",
		BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code",
		IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute),
		DeliveryDeadline: now.Add(61 * time.Second), GrantFence: 2,
	}
	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{token: "client-token", principal: auth.Principal{
			Subject: "client", Scopes: []auth.Scope{auth.SessionControl("ses_target")},
		}},
		EventStore:          fakeStore{latest: map[string]int64{}, connections: map[string]store.AdapterConnection{"ses_bootstrap": liveBootstrapConnection(1)}},
		AttachGrantVerifier: fakeAttachGrantVerifier{raw: "signed-grant", audience: "deploy-attach", grant: grant},
		AttachGrantAudience: "deploy-attach",
	})
	_, peer, err := core.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_target"}},
	})
	if err != nil {
		t.Fatalf("HandleHello() error = %v", err)
	}
	if peer.Admissions["ses_target"].Mode != auth.SessionAdmissionAttachOnly {
		t.Fatalf("admission = %+v, want attach_only", peer.Admissions["ses_target"])
	}
	got, err := core.AuthorizeAttach(context.Background(), peer, "signed-grant")
	if err != nil || got != grant {
		t.Fatalf("AuthorizeAttach() = %+v, %v; want %+v, nil", got, err, grant)
	}
}

func TestHandshakeAttachAuthorizationFailsClosed(t *testing.T) {
	now := time.Now()
	grant := auth.AttachGrant{
		Audience: "deploy-attach", JTI: "jti_1", AttachID: "attach_1",
		BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code",
		IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute),
		DeliveryDeadline: now.Add(61 * time.Second), GrantFence: 2,
	}
	newPeer := func(t *testing.T, verifier auth.AttachGrantVerifier, connection store.AdapterConnection) (*hub.Handshake, hub.AcceptedPeer) {
		t.Helper()
		core := hub.NewHandshake(hub.HandshakeConfig{
			Authenticator: fakeAuth{token: "client-token", principal: auth.Principal{
				Subject: "client", Scopes: []auth.Scope{auth.SessionControl("ses_target")},
			}},
			EventStore: fakeStore{latest: map[string]int64{}, connections: map[string]store.AdapterConnection{"ses_bootstrap": connection}}, AttachGrantVerifier: verifier, AttachGrantAudience: "deploy-attach",
		})
		_, peer, err := core.HandleHello(context.Background(), &protocol.Hello{
			ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
			Subscriptions: []protocol.Subscription{{SessionID: "ses_target"}},
		})
		if err != nil {
			t.Fatalf("HandleHello() error = %v", err)
		}
		return core, peer
	}
	for _, test := range []struct {
		name       string
		verifier   auth.AttachGrantVerifier
		raw        string
		connection store.AdapterConnection
	}{
		{name: "missing verifier", connection: liveBootstrapConnection(1)},
		{name: "invalid signature", verifier: fakeAttachGrantVerifier{raw: "signed-grant", err: errors.New("invalid signature")}, raw: "signed-grant", connection: liveBootstrapConnection(1)},
		{name: "wrong raw grant", verifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, raw: "forged", connection: liveBootstrapConnection(1)},
		{name: "stale reconnect fence", verifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, raw: "signed-grant", connection: liveBootstrapConnection(2)},
		{name: "expired bootstrap", verifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, raw: "signed-grant", connection: expiredBootstrapConnection()},
		{name: "revoked bootstrap", verifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, raw: "signed-grant", connection: revokedBootstrapConnection()},
		{name: "terminal bootstrap", verifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, raw: "signed-grant", connection: terminalBootstrapConnection()},
		{name: "oversized raw grant", verifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, raw: strings.Repeat("x", 64*1024+1), connection: liveBootstrapConnection(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, peer := newPeer(t, test.verifier, test.connection)
			if _, err := core.AuthorizeAttach(context.Background(), peer, test.raw); !errors.Is(err, auth.ErrUnauthorized) {
				t.Fatalf("AuthorizeAttach() error = %v, want unauthorized", err)
			}
		})
	}
}

func TestHandshakeAttachRejectsCurrentTarget(t *testing.T) {
	now := time.Now()
	grant := auth.AttachGrant{Audience: "deploy-attach", JTI: "jti_1", AttachID: "attach_1", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code", IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute), DeliveryDeadline: now.Add(61 * time.Second), GrantFence: 2}
	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator:       fakeAuth{token: "client-token", principal: auth.Principal{Subject: "client", Scopes: []auth.Scope{auth.SessionControl("ses_target")}}},
		EventStore:          fakeStore{latest: map[string]int64{"ses_target": 1}, truth: map[string]store.SessionAdmissionTruth{"ses_target": {SessionID: "ses_target", Exists: true, Complete: true, Live: true}}, connections: map[string]store.AdapterConnection{"ses_bootstrap": liveBootstrapConnection(1)}},
		AttachGrantVerifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, AttachGrantAudience: "deploy-attach",
	})
	_, peer, err := core.HandleHello(context.Background(), &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_target"}}})
	if err != nil {
		t.Fatalf("HandleHello() error = %v", err)
	}
	if _, err := core.AuthorizeAttach(context.Background(), peer, "signed-grant"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("AuthorizeAttach() error = %v, want unauthorized", err)
	}
}

func TestHandshakeAttachRejectsGrantForUnsubscribedTarget(t *testing.T) {
	now := time.Now()
	grant := auth.AttachGrant{Audience: "deploy-attach", JTI: "jti_1", AttachID: "attach_1", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_other", Provider: "claude-code", IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute), DeliveryDeadline: now.Add(61 * time.Second), GrantFence: 2}
	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator:       fakeAuth{token: "client-token", principal: auth.Principal{Subject: "client", Scopes: []auth.Scope{auth.SessionControl("ses_target")}}},
		EventStore:          fakeStore{latest: map[string]int64{}, connections: map[string]store.AdapterConnection{"ses_bootstrap": liveBootstrapConnection(1)}},
		AttachGrantVerifier: fakeAttachGrantVerifier{raw: "signed-grant", grant: grant}, AttachGrantAudience: "deploy-attach",
	})
	_, peer, err := core.HandleHello(context.Background(), &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_target"}}})
	if err != nil {
		t.Fatalf("HandleHello() error = %v", err)
	}
	if _, err := core.AuthorizeAttach(context.Background(), peer, "signed-grant"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("AuthorizeAttach() error = %v, want unauthorized", err)
	}
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
		EventStore: fakeStore{latest: map[string]int64{"ses_1": 1}},
	})

	tests := []struct {
		name string
		in   *protocol.Hello
		want error
	}{
		{
			name: "adapter v2 disabled",
			in: &protocol.Hello{
				ProtocolVersion: 2,
				Role:            protocol.RoleAdapter,
				Token:           "client-token",
				SessionID:       "ses_1",
				Provider:        "claude-code",
			},
			want: hub.ErrVersionUnsupported,
		},
		{
			name: "version below one",
			in: &protocol.Hello{ProtocolVersion: 0, Role: protocol.RoleClient, Token: "client-token",
				Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}},
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
		EventStore: fakeStore{latest: map[string]int64{"ses_2": 1}},
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

func TestHandshakeFreshTargetIsAttachOnly(t *testing.T) {
	t.Parallel()
	core := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: fakeAuth{token: "fresh-token", principal: auth.Principal{Subject: "fresh", Scopes: []auth.Scope{auth.SessionControl("ses_fresh")}}},
		EventStore: fakeStore{latest: map[string]int64{}, truth: map[string]store.SessionAdmissionTruth{
			"ses_fresh": {SessionID: "ses_fresh"},
		}},
	})
	ack, accepted, err := core.HandleHello(context.Background(), &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "fresh-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}},
	})
	if err != nil {
		t.Fatalf("HandleHello() error = %v", err)
	}
	decision := accepted.Admissions["ses_fresh"]
	if decision.Mode != auth.SessionAdmissionAttachOnly || decision.MayMutate || ack.Sessions[0].State != "attach_only" || ack.Sessions[0].Provider != "claude-code" {
		t.Fatalf("attach-only ack/decision = %+v / %+v", ack.Sessions[0], decision)
	}
}

func TestHandshakeFreshTargetRejectsMixedAuthority(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		version       int
		scopes        []auth.Scope
		subscriptions []protocol.Subscription
		truth         map[string]store.SessionAdmissionTruth
	}{
		{
			name:          "v1 protocol",
			version:       protocol.ProtocolVersion,
			scopes:        []auth.Scope{auth.SessionControl("ses_fresh")},
			subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}},
			truth:         map[string]store.SessionAdmissionTruth{"ses_fresh": {SessionID: "ses_fresh"}},
		},
		{
			name:          "api wildcard",
			scopes:        []auth.Scope{auth.SessionControl("ses_fresh"), auth.API()},
			subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}},
			truth:         map[string]store.SessionAdmissionTruth{"ses_fresh": {SessionID: "ses_fresh"}},
		},
		{
			name:          "group scope",
			scopes:        []auth.Scope{auth.SessionControl("ses_fresh"), auth.GroupControl("grp_1")},
			subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}},
			truth:         map[string]store.SessionAdmissionTruth{"ses_fresh": {SessionID: "ses_fresh"}},
		},
		{
			name:          "adapter scope",
			scopes:        []auth.Scope{auth.SessionControl("ses_fresh"), auth.SessionAdapter("ses_fresh")},
			subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}},
			truth:         map[string]store.SessionAdmissionTruth{"ses_fresh": {SessionID: "ses_fresh"}},
		},
		{
			name:          "view scope",
			scopes:        []auth.Scope{auth.SessionControl("ses_fresh"), auth.SessionView("ses_fresh")},
			subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}},
			truth:         map[string]store.SessionAdmissionTruth{"ses_fresh": {SessionID: "ses_fresh"}},
		},
		{
			name:          "other session scope",
			scopes:        []auth.Scope{auth.SessionControl("ses_fresh"), auth.SessionControl("ses_other")},
			subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}},
			truth:         map[string]store.SessionAdmissionTruth{"ses_fresh": {SessionID: "ses_fresh"}},
		},
		{
			name:          "mixed fresh and current subscriptions",
			scopes:        []auth.Scope{auth.SessionControl("ses_fresh"), auth.SessionView("ses_current")},
			subscriptions: []protocol.Subscription{{SessionID: "ses_fresh"}, {SessionID: "ses_current"}},
			truth: map[string]store.SessionAdmissionTruth{
				"ses_fresh":   {SessionID: "ses_fresh"},
				"ses_current": {SessionID: "ses_current", Exists: true, Complete: true, Live: true},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			version := test.version
			if version == 0 {
				version = protocol.ProtocolVersionV2
			}
			core := hub.NewHandshake(hub.HandshakeConfig{
				Authenticator: fakeAuth{token: "fresh-token", principal: auth.Principal{Subject: "fresh", Scopes: test.scopes}},
				EventStore:    fakeStore{latest: map[string]int64{}, truth: test.truth},
			})
			_, _, err := core.HandleHello(context.Background(), &protocol.Hello{
				ProtocolVersion: version,
				Role:            protocol.RoleClient,
				Token:           "fresh-token",
				Subscriptions:   test.subscriptions,
			})
			if !errors.Is(err, auth.ErrUnauthorized) {
				t.Fatalf("HandleHello() error = %v, want ErrUnauthorized", err)
			}
		})
	}
}

func TestHandshakeFailsClosedOnAdmissionTruth(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		principal auth.Principal
		truth     store.SessionAdmissionTruth
		claim     *auth.SessionAdmissionClaim
	}{
		{name: "fresh view", principal: auth.Principal{Subject: "view", Scopes: []auth.Scope{auth.SessionView("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1"}},
		{name: "fresh with history", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1"}},
		{name: "incomplete", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1", Exists: true, Live: true}},
		{name: "terminal", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1", Exists: true, Complete: true, Live: true, Terminal: true}},
		{name: "conflicting", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1", Exists: true, Complete: true, Live: true, Conflicting: true}},
		{name: "offline", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1", Exists: true, Complete: true}},
		{name: "providerless claim", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1", Exists: true, Complete: true, Live: true}, claim: &auth.SessionAdmissionClaim{SessionID: "ses_1", ExpiresAt: time.Now().Add(time.Minute)}},
		{name: "expired claim", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1", Exists: true, Complete: true, Live: true}, claim: &auth.SessionAdmissionClaim{SessionID: "ses_1", Provider: "claude-code", ExpiresAt: time.Now().Add(-time.Minute)}},
		{name: "wrong session claim", principal: auth.Principal{Subject: "control", Scopes: []auth.Scope{auth.SessionControl("ses_1")}}, truth: store.SessionAdmissionTruth{SessionID: "ses_1", Exists: true, Complete: true, Live: true}, claim: &auth.SessionAdmissionClaim{SessionID: "ses_other", Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			core := hub.NewHandshake(hub.HandshakeConfig{
				Authenticator: fakeAuth{token: "token", principal: test.principal, claim: test.claim},
				EventStore:    fakeStore{latest: map[string]int64{"ses_1": 1}, truth: map[string]store.SessionAdmissionTruth{"ses_1": test.truth}},
			})
			_, _, err := core.HandleHello(context.Background(), &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
			if !errors.Is(err, auth.ErrUnauthorized) {
				t.Fatalf("HandleHello() error = %v, want unauthorized", err)
			}
		})
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
	claim     *auth.SessionAdmissionClaim
}

type fakeAttachGrantVerifier struct {
	raw      string
	audience string
	grant    auth.AttachGrant
	err      error
}

func (f fakeAttachGrantVerifier) VerifyAttachGrant(_ context.Context, rawGrant, expectedAudience string) (auth.AttachGrant, error) {
	if f.err != nil || rawGrant != f.raw || (f.audience != "" && expectedAudience != f.audience) {
		return auth.AttachGrant{}, f.err
	}
	return f.grant, nil
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

func (f fakeAuth) SessionAdmissionClaim(_ context.Context, _ auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	if f.claim != nil {
		return *f.claim, nil
	}
	return auth.SessionAdmissionClaim{SessionID: sessionID, Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

type fakeStore struct {
	latest      map[string]int64
	truth       map[string]store.SessionAdmissionTruth
	connections map[string]store.AdapterConnection
}

func (f fakeStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	return f.latest[sessionID], nil
}

func (f fakeStore) SessionAdmissionTruth(_ context.Context, sessionID string) (store.SessionAdmissionTruth, error) {
	if truth, ok := f.truth[sessionID]; ok {
		return truth, nil
	}
	_, exists := f.latest[sessionID]
	return store.SessionAdmissionTruth{SessionID: sessionID, Exists: exists, Complete: exists, Live: exists}, nil
}

func (f fakeStore) AdapterConnection(_ context.Context, sessionID string) (store.AdapterConnection, error) {
	connection, ok := f.connections[sessionID]
	if !ok {
		return store.AdapterConnection{}, errors.New("adapter connection not found")
	}
	return connection, nil
}

func liveBootstrapConnection(acceptedFence int64) store.AdapterConnection {
	return store.AdapterConnection{SessionID: "ses_bootstrap", ActiveCredentialGeneration: 1, ConnectionEpoch: 1, AcceptedFence: acceptedFence, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
}

func expiredBootstrapConnection() store.AdapterConnection {
	connection := liveBootstrapConnection(1)
	connection.ActiveCredentialExpiresAt = time.Now().Add(-time.Second)
	return connection
}

func revokedBootstrapConnection() store.AdapterConnection {
	connection := liveBootstrapConnection(1)
	at := time.Now()
	connection.RevokedAt = &at
	return connection
}

func terminalBootstrapConnection() store.AdapterConnection {
	connection := liveBootstrapConnection(1)
	at := time.Now()
	connection.TerminalAt = &at
	return connection
}
