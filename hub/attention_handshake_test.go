package hub_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
)

func TestAttentionHelloIsV2OnlyAndCannotCarryReplaySubscriptions(t *testing.T) {
	now := time.Now()
	principal := auth.Principal{Subject: "user_1", Scopes: []auth.Scope{auth.Attention("grant_1")}}
	core := hub.NewHandshake(hub.HandshakeConfig{Authenticator: attentionHandshakeAuth{principal: principal, grant: auth.AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_1"}, MaxSessions: 1, ExpiresAt: now.Add(time.Minute)}}})
	ack, accepted, err := core.HandleHello(context.Background(), &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token"})
	if err != nil || !accepted.AttentionOnly || len(ack.Sessions) != 0 {
		t.Fatalf("attention hello = %+v %+v %v", ack, accepted, err)
	}
	if _, _, err := core.HandleHello(context.Background(), &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "attention-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}}); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("attention hello with replay subscription error = %v, want unauthorized", err)
	}
	if _, _, err := core.HandleHello(context.Background(), &protocol.Hello{ProtocolVersion: protocol.ProtocolVersion, Role: protocol.RoleClient, Token: "attention-token"}); !errors.Is(err, hub.ErrVersionUnsupported) {
		t.Fatalf("v1 attention hello error = %v, want unsupported", err)
	}
}

type attentionHandshakeAuth struct {
	principal auth.Principal
	grant     auth.AttentionGrant
}

func (a attentionHandshakeAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return a.principal, nil
}
func (a attentionHandshakeAuth) Authorize(ctx context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}
func (a attentionHandshakeAuth) AuthorizeAttention(context.Context, auth.Principal) (auth.AttentionGrant, error) {
	return a.grant, nil
}
