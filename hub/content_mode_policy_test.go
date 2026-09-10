package hub

import (
	"context"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
)

type sessionModeAuth struct {
	plainAuth
	modes   map[string]string
	fail    bool
	queried []string
}

func (a *sessionModeAuth) SessionContentMode(_ context.Context, _ auth.Principal, sessionID string) (string, error) {
	a.queried = append(a.queried, sessionID)
	if a.fail {
		return "", errors.New("store unavailable")
	}
	return a.modes[sessionID], nil
}

func TestContentModePolicyChecksEverySessionAndAdapter(t *testing.T) {
	for _, tc := range []struct {
		name          string
		role          protocol.Role
		mode          string
		version       int
		subscriptions []protocol.Subscription
		modes         map[string]string
		fail          bool
		wantErr       bool
		wantQueries   int
	}{
		{name: "mixed sessions cannot downgrade", role: protocol.RoleClient, version: 2, subscriptions: []protocol.Subscription{{SessionID: "legacy"}, {SessionID: "encrypted"}}, modes: map[string]string{"legacy": "legacy", "encrypted": "required"}, wantErr: true, wantQueries: 2},
		{name: "all required sessions", role: protocol.RoleClient, mode: "required", version: 2, subscriptions: []protocol.Subscription{{SessionID: "one"}, {SessionID: "two"}}, modes: map[string]string{"one": "required", "two": "required"}, wantQueries: 2},
		{name: "adapter omission", role: protocol.RoleAdapter, version: 2, modes: map[string]string{"machine": "required"}, wantErr: true, wantQueries: 1},
		{name: "adapter required", role: protocol.RoleAdapter, mode: "required", version: 2, modes: map[string]string{"machine": "required"}, wantQueries: 1},
		{name: "required rejects v1", role: protocol.RoleAdapter, mode: "required", version: 1, modes: map[string]string{"machine": "required"}, wantErr: true, wantQueries: 1},
		{name: "lookup failure", role: protocol.RoleAdapter, mode: "required", version: 2, fail: true, wantErr: true, wantQueries: 1},
		{name: "missing session", role: protocol.RoleAdapter, mode: "required", version: 2, wantErr: true, wantQueries: 1},
		{name: "required without subscriptions", role: protocol.RoleClient, mode: "required", version: 2, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &sessionModeAuth{modes: tc.modes, fail: tc.fail}
			h := NewHandshake(HandshakeConfig{Authenticator: a})
			err := h.authorizeContentMode(context.Background(), &protocol.Hello{Role: tc.role, SessionID: "machine", ContentMode: tc.mode, Subscriptions: tc.subscriptions}, auth.Principal{}, tc.version)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, want rejection %v", err, tc.wantErr)
			}
			if len(a.queried) != tc.wantQueries {
				t.Fatalf("queried %v, want %d lookups", a.queried, tc.wantQueries)
			}
			if tc.role == protocol.RoleAdapter && len(a.queried) > 0 && a.queried[0] != "machine" {
				t.Fatalf("adapter lookup = %v", a.queried)
			}
		})
	}
}
