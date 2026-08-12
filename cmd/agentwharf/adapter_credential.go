package main

import (
	"context"
	"errors"
	"time"

	"github.com/winghv/agentwharf/auth"
)

func (a localSessionAuthenticator) AdapterCredential(ctx context.Context, token string, principal auth.Principal, sessionID string) (int64, int64, bool, error) {
	if a.sessionCredentialIssuer != nil {
		prepared, err := a.sessionCredentialIssuer.ActiveSessionCredential(ctx, token)
		if err == nil && prepared.SessionID == sessionID && len(principal.Scopes) == 1 && principal.Scopes[0] == auth.SessionAdapter(sessionID) {
			return prepared.Generation, prepared.ExpiresAt.UnixNano(), true, nil
		}
	}
	if sessionID != a.sessionID || a.Authenticator == nil || a.staticAdapterExpiresAt <= time.Now().UnixNano() ||
		a.Authorize(ctx, principal, auth.SessionAdapter(sessionID)) != nil {
		return 0, 0, false, auth.ErrUnauthorized
	}
	return 1, a.staticAdapterExpiresAt, true, nil
}

func (a localSessionAuthenticator) SessionCredentialEvidence(ctx context.Context, token string) (auth.SessionCredentialEvidence, error) {
	if a.sessionCredentialIssuer != nil {
		evidence, err := a.sessionCredentialIssuer.SessionCredentialEvidence(ctx, token)
		if err == nil && evidence.SessionID == a.sessionID {
			return evidence, nil
		}
		if err == nil || !errors.Is(err, auth.ErrUnauthorized) {
			return auth.SessionCredentialEvidence{}, auth.ErrUnauthorized
		}
	}
	principal, err := a.Authenticator.Authenticate(ctx, token)
	if err != nil || len(principal.Scopes) != 1 || principal.Scopes[0] != auth.SessionAdapter(a.sessionID) ||
		a.Authorize(ctx, principal, auth.SessionAdapter(a.sessionID)) != nil || a.staticAdapterExpiresAt <= time.Now().UnixNano() {
		return auth.SessionCredentialEvidence{}, auth.ErrUnauthorized
	}
	return auth.SessionCredentialEvidence{
		SessionID:  a.sessionID,
		Lineage:    auth.SessionCredentialLineage{Kind: auth.SessionCredentialBootstrapInitial},
		Generation: 1,
		ExpiresAt:  time.Unix(0, a.staticAdapterExpiresAt),
	}, nil
}
