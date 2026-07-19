package main

import (
	"context"

	"github.com/winghv/agentwharf/auth"
)

func (a localSessionAuthenticator) AdapterCredential(ctx context.Context, token string, principal auth.Principal, sessionID string) (int64, int64, bool, error) {
	if a.sessionCredentialIssuer != nil {
		prepared, err := a.sessionCredentialIssuer.ActiveSessionCredential(ctx, token)
		if err == nil && prepared.SessionID == sessionID && len(principal.Scopes) == 1 && principal.Scopes[0] == auth.SessionAdapter(sessionID) {
			return prepared.Generation, prepared.ExpiresAt.UnixNano(), true, nil
		}
	}
	if a.staticAdapterCredential == nil {
		return 0, 0, false, auth.ErrUnauthorized
	}
	return a.staticAdapterCredential(ctx, token, principal, sessionID)
}
