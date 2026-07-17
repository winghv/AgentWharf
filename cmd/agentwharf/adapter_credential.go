package main

import (
	"context"

	"github.com/winghv/agentwharf/auth"
)

func (a localSessionAuthenticator) AdapterCredential(ctx context.Context, token string, principal auth.Principal, sessionID string) (int64, int64, bool, error) {
	authenticator, ok := a.Authenticator.(interface {
		AdapterCredential(context.Context, string, auth.Principal, string) (int64, int64, bool, error)
	})
	if !ok {
		return 0, 0, false, auth.ErrUnauthorized
	}
	return authenticator.AdapterCredential(ctx, token, principal, sessionID)
}
