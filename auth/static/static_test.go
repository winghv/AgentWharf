package static_test

import (
	"context"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/auth/static"
)

func TestAuthenticatorAuthenticatesConfiguredToken(t *testing.T) {
	t.Parallel()

	authenticator := static.New([]static.Token{{
		Token:   "client-token",
		Subject: "client_1",
		Scopes:  []auth.Scope{auth.SessionControl("ses_1")},
	}})

	principal, err := authenticator.Authenticate(context.Background(), "client-token")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if principal.Subject != "client_1" {
		t.Fatalf("subject = %q, want client_1", principal.Subject)
	}
	if err := authenticator.Authorize(context.Background(), principal, auth.SessionView("ses_1")); err != nil {
		t.Fatalf("Authorize(view) error = %v", err)
	}
	if err := authenticator.Authorize(context.Background(), principal, auth.SessionAdapter("ses_1")); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("Authorize(adapter) error = %v, want ErrUnauthorized", err)
	}
}

func TestAuthenticatorRejectsUnknownToken(t *testing.T) {
	t.Parallel()

	authenticator := static.New([]static.Token{{
		Token:   "known-token",
		Subject: "client_1",
		Scopes:  []auth.Scope{auth.SessionView("ses_1")},
	}})

	if _, err := authenticator.Authenticate(context.Background(), "missing-token"); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("Authenticate(missing) error = %v, want ErrInvalidToken", err)
	}
	if _, err := authenticator.Authenticate(context.Background(), ""); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("Authenticate(empty) error = %v, want ErrInvalidToken", err)
	}
}

func TestAuthenticatorCopiesConfiguredScopes(t *testing.T) {
	t.Parallel()

	configuredScopes := []auth.Scope{auth.SessionView("ses_1")}
	authenticator := static.New([]static.Token{{
		Token:   "client-token",
		Subject: "client_1",
		Scopes:  configuredScopes,
	}})
	configuredScopes[0] = auth.API()

	principal, err := authenticator.Authenticate(context.Background(), "client-token")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	principal.Scopes[0] = auth.API()

	principalAgain, err := authenticator.Authenticate(context.Background(), "client-token")
	if err != nil {
		t.Fatalf("Authenticate() again error = %v", err)
	}
	if got := principalAgain.Scopes[0].String(); got != "session:ses_1:view" {
		t.Fatalf("stored scope = %q, want session:ses_1:view", got)
	}
}
