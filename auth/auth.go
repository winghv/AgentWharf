package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidScope = errors.New("invalid scope")
	ErrInvalidToken = errors.New("invalid token")
	ErrUnauthorized = errors.New("unauthorized")
)

type Kind string

const (
	KindSession Kind = "session"
	KindGroup   Kind = "group"
	KindAPI     Kind = "api"
)

type Access string

const (
	AccessControl Access = "control"
	AccessView    Access = "view"
	AccessAdapter Access = "adapter"
	AccessAll     Access = "*"
)

type Scope struct {
	Kind   Kind
	ID     string
	Access Access
}

type Principal struct {
	Subject string
	Scopes  []Scope
}

type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Principal, error)
	Authorize(ctx context.Context, principal Principal, scope Scope) error
}

func ParseScope(raw string) (Scope, error) {
	parts := strings.Split(raw, ":")
	switch {
	case len(parts) == 2 && parts[0] == string(KindAPI) && parts[1] == string(AccessAll):
		return API(), nil
	case len(parts) == 3 && parts[0] == string(KindSession) && parts[1] != "":
		switch parts[2] {
		case string(AccessControl):
			return SessionControl(parts[1]), nil
		case string(AccessView):
			return SessionView(parts[1]), nil
		case string(AccessAdapter):
			return SessionAdapter(parts[1]), nil
		default:
			return Scope{}, fmt.Errorf("%w: %q", ErrInvalidScope, raw)
		}
	case len(parts) == 3 && parts[0] == string(KindGroup) && parts[1] != "" && parts[2] == string(AccessControl):
		return GroupControl(parts[1]), nil
	default:
		return Scope{}, fmt.Errorf("%w: %q", ErrInvalidScope, raw)
	}
}

func SessionControl(sessionID string) Scope {
	return Scope{Kind: KindSession, ID: sessionID, Access: AccessControl}
}

func SessionView(sessionID string) Scope {
	return Scope{Kind: KindSession, ID: sessionID, Access: AccessView}
}

func SessionAdapter(sessionID string) Scope {
	return Scope{Kind: KindSession, ID: sessionID, Access: AccessAdapter}
}

func GroupControl(groupID string) Scope {
	return Scope{Kind: KindGroup, ID: groupID, Access: AccessControl}
}

func API() Scope {
	return Scope{Kind: KindAPI, Access: AccessAll}
}

func (s Scope) String() string {
	if s.Kind == KindAPI && s.Access == AccessAll {
		return "api:*"
	}
	return strings.Join([]string{string(s.Kind), s.ID, string(s.Access)}, ":")
}

func (s Scope) Validate() error {
	switch s.Kind {
	case KindAPI:
		if s.ID == "" && s.Access == AccessAll {
			return nil
		}
	case KindSession:
		if s.ID != "" && (s.Access == AccessControl || s.Access == AccessView || s.Access == AccessAdapter) {
			return nil
		}
	case KindGroup:
		if s.ID != "" && s.Access == AccessControl {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrInvalidScope, s)
}

func Authorize(principal Principal, requested Scope) error {
	if err := requested.Validate(); err != nil {
		return err
	}
	for _, granted := range principal.Scopes {
		if err := granted.Validate(); err != nil {
			continue
		}
		if allows(granted, requested) {
			return nil
		}
	}
	return fmt.Errorf("%w: subject %q lacks %s", ErrUnauthorized, principal.Subject, requested)
}

func allows(granted Scope, requested Scope) bool {
	if granted.Kind == KindAPI && granted.Access == AccessAll {
		return requested.Access != AccessAdapter
	}

	if granted.Kind != requested.Kind || granted.ID != requested.ID {
		return false
	}

	if granted.Access == requested.Access {
		return true
	}

	return granted.Kind == KindSession &&
		granted.Access == AccessControl &&
		requested.Access == AccessView
}
