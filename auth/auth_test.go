package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/auth"
)

func TestParseScopeRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []string{
		"session:ses_123:control",
		"session:ses_123:view",
		"session:ses_123:adapter",
		"group:grp_123:control",
		"api:*",
	}

	for _, raw := range tests {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			scope, err := auth.ParseScope(raw)
			if err != nil {
				t.Fatalf("ParseScope() error = %v", err)
			}
			if got := scope.String(); got != raw {
				t.Fatalf("scope.String() = %q, want %q", got, raw)
			}
		})
	}
}

func TestParseScopeRejectsInvalidScopes(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"session",
		"session:ses_123",
		"session:ses_123:admin",
		"session::control",
		"group:grp_123:view",
		"group::control",
		"api:read",
		"future:thing:control",
	}

	for _, raw := range tests {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			if _, err := auth.ParseScope(raw); !errors.Is(err, auth.ErrInvalidScope) {
				t.Fatalf("ParseScope() error = %v, want ErrInvalidScope", err)
			}
		})
	}
}

func TestAuthorizeSessionScopeRules(t *testing.T) {
	t.Parallel()

	principal := auth.Principal{
		Subject: "client_1",
		Scopes:  []auth.Scope{auth.SessionControl("ses_1")},
	}

	assertAuthorized(t, principal, auth.SessionControl("ses_1"))
	assertAuthorized(t, principal, auth.SessionView("ses_1"))
	assertUnauthorized(t, principal, auth.SessionControl("ses_2"))
	assertUnauthorized(t, principal, auth.SessionAdapter("ses_1"))
}

func TestAuthorizeViewAndAdapterAreNotInterchangeable(t *testing.T) {
	t.Parallel()

	viewer := auth.Principal{
		Subject: "viewer_1",
		Scopes:  []auth.Scope{auth.SessionView("ses_1")},
	}
	adapter := auth.Principal{
		Subject: "adapter_1",
		Scopes:  []auth.Scope{auth.SessionAdapter("ses_1")},
	}

	assertAuthorized(t, viewer, auth.SessionView("ses_1"))
	assertUnauthorized(t, viewer, auth.SessionControl("ses_1"))
	assertUnauthorized(t, viewer, auth.SessionAdapter("ses_1"))

	assertAuthorized(t, adapter, auth.SessionAdapter("ses_1"))
	assertUnauthorized(t, adapter, auth.SessionView("ses_1"))
	assertUnauthorized(t, adapter, auth.SessionControl("ses_1"))
	assertUnauthorized(t, adapter, auth.SessionAdapter("ses_2"))
}

func TestAuthorizeGroupAndAPIScopes(t *testing.T) {
	t.Parallel()

	groupClient := auth.Principal{
		Subject: "group_client_1",
		Scopes:  []auth.Scope{auth.GroupControl("grp_1")},
	}
	apiClient := auth.Principal{
		Subject: "api_client_1",
		Scopes:  []auth.Scope{auth.API()},
	}

	assertAuthorized(t, groupClient, auth.GroupControl("grp_1"))
	assertUnauthorized(t, groupClient, auth.GroupControl("grp_2"))
	assertUnauthorized(t, groupClient, auth.SessionControl("ses_1"))

	assertAuthorized(t, apiClient, auth.SessionControl("ses_1"))
	assertAuthorized(t, apiClient, auth.SessionView("ses_1"))
	assertAuthorized(t, apiClient, auth.GroupControl("grp_1"))
	assertUnauthorized(t, apiClient, auth.SessionAdapter("ses_1"))
}

func TestAuthorizeRejectsInvalidScope(t *testing.T) {
	t.Parallel()

	apiClient := auth.Principal{
		Subject: "api_client_1",
		Scopes:  []auth.Scope{auth.API()},
	}
	if err := auth.Authorize(apiClient, auth.Scope{}); !errors.Is(err, auth.ErrInvalidScope) {
		t.Fatalf("Authorize(invalid requested scope) error = %v, want ErrInvalidScope", err)
	}

	invalidGrant := auth.Principal{
		Subject: "broken_client_1",
		Scopes:  []auth.Scope{{Kind: auth.KindAPI}},
	}
	assertUnauthorized(t, invalidGrant, auth.SessionView("ses_1"))
}

func TestAuthenticatorInterface(t *testing.T) {
	t.Parallel()

	var authenticator auth.Authenticator = fakeAuthenticator{}
	principal, err := authenticator.Authenticate(context.Background(), "token")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if err := authenticator.Authorize(context.Background(), principal, auth.SessionView("ses_1")); err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
}

func assertAuthorized(t *testing.T, principal auth.Principal, scope auth.Scope) {
	t.Helper()

	if err := auth.Authorize(principal, scope); err != nil {
		t.Fatalf("Authorize(%+v, %s) error = %v", principal, scope, err)
	}
}

func assertUnauthorized(t *testing.T, principal auth.Principal, scope auth.Scope) {
	t.Helper()

	if err := auth.Authorize(principal, scope); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("Authorize(%+v, %s) error = %v, want ErrUnauthorized", principal, scope, err)
	}
}

type fakeAuthenticator struct{}

func (fakeAuthenticator) Authenticate(context.Context, string) (auth.Principal, error) {
	return auth.Principal{
		Subject: "fake",
		Scopes:  []auth.Scope{auth.SessionView("ses_1")},
	}, nil
}

func (fakeAuthenticator) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}
