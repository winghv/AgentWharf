package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEvaluateAttentionAuthorizationFailsClosed(t *testing.T) {
	now := time.Now()
	valid := AttentionGrant{
		Subject: "user_1", SessionIDs: []string{"ses_2", "ses_1"}, MaxSessions: 2,
		ExpiresAt: now.Add(time.Minute),
	}
	principal := Principal{Subject: "user_1", Scopes: []Scope{Attention("grant_1")}}
	if grant, err := EvaluateAttentionAuthorization(principal, valid, now); err != nil || len(grant.SessionIDs) != 2 {
		t.Fatalf("EvaluateAttentionAuthorization() = %+v, %v", grant, err)
	}
	for _, test := range []struct {
		name      string
		principal Principal
		grant     AttentionGrant
	}{
		{name: "control cannot upgrade", principal: Principal{Subject: "user_1", Scopes: []Scope{SessionControl("ses_1")}}, grant: valid},
		{name: "view cannot upgrade", principal: Principal{Subject: "user_1", Scopes: []Scope{SessionView("ses_1")}}, grant: valid},
		{name: "api cannot upgrade", principal: Principal{Subject: "user_1", Scopes: []Scope{API()}}, grant: valid},
		{name: "duplicate membership", principal: principal, grant: AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_1", "ses_1"}, MaxSessions: 2, ExpiresAt: now.Add(time.Minute)}},
		{name: "expired", principal: principal, grant: AttentionGrant{Subject: "user_1", SessionIDs: []string{"ses_1"}, MaxSessions: 1, ExpiresAt: now.Add(-time.Second)}},
		{name: "cross subject", principal: principal, grant: AttentionGrant{Subject: "user_2", SessionIDs: []string{"ses_1"}, MaxSessions: 1, ExpiresAt: now.Add(time.Minute)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EvaluateAttentionAuthorization(test.principal, test.grant, now); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("EvaluateAttentionAuthorization() error = %v, want ErrUnauthorized", err)
			}
		})
	}
}

func TestAttentionAuthorizerUsesOnlyOpaqueGrant(t *testing.T) {
	var _ AttentionAuthorizer = attentionAuthorizerFunc(func(context.Context, Principal) (AttentionGrant, error) {
		return AttentionGrant{}, nil
	})
}

func TestAPIScopeDoesNotAuthorizeAttention(t *testing.T) {
	if err := Authorize(Principal{Subject: "api", Scopes: []Scope{API()}}, Attention("grant_1")); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(api, attention) error = %v, want ErrUnauthorized", err)
	}
}

type attentionAuthorizerFunc func(context.Context, Principal) (AttentionGrant, error)

func (fn attentionAuthorizerFunc) AuthorizeAttention(ctx context.Context, principal Principal) (AttentionGrant, error) {
	return fn(ctx, principal)
}
