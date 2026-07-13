package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/winghv/agentwharf/store"
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

type SessionAdmissionMode string

const (
	SessionAdmissionAttachOnly SessionAdmissionMode = "attach_only"
	SessionAdmissionCurrent    SessionAdmissionMode = "current"
)

type SessionAdmissionAction string

const (
	SessionAdmissionAttach     SessionAdmissionAction = "attach"
	SessionAdmissionStatus     SessionAdmissionAction = "status"
	SessionAdmissionHistory    SessionAdmissionAction = "history"
	SessionAdmissionSend       SessionAdmissionAction = "send"
	SessionAdmissionSettings   SessionAdmissionAction = "settings"
	SessionAdmissionRunControl SessionAdmissionAction = "run_control"
	SessionAdmissionPermission SessionAdmissionAction = "permission"
	SessionAdmissionRotation   SessionAdmissionAction = "rotation"
)

type SessionAdmissionClaim struct {
	SessionID string
	Provider  string
	ExpiresAt time.Time
}

type SessionAdmissionRequest struct {
	Principal Principal
	Claim     SessionAdmissionClaim
	Truth     store.SessionAdmissionTruth
}

type SessionAdmissionDecision struct {
	Mode      SessionAdmissionMode
	MayMutate bool
}

// EvaluateSessionAdmission is limited to exact control scope, provider-bound
// Auth claim, and Store-owned Session truth. It never consults platform state.
func EvaluateSessionAdmission(request SessionAdmissionRequest) (SessionAdmissionDecision, error) {
	claim := request.Claim
	truth := request.Truth
	if claim.SessionID == "" || claim.Provider == "" || !claim.ExpiresAt.After(time.Now()) || claim.ExpiresAt.After(time.Now().Add(5*time.Minute)) || truth.SessionID != claim.SessionID || !hasExactSessionControl(request.Principal, claim.SessionID) {
		return SessionAdmissionDecision{}, ErrUnauthorized
	}
	if !truth.Exists {
		if truth.Complete || truth.Terminal || truth.Conflicting || truth.Live {
			return SessionAdmissionDecision{}, ErrUnauthorized
		}
		return SessionAdmissionDecision{Mode: SessionAdmissionAttachOnly}, nil
	}
	if !truth.Complete || truth.Terminal || truth.Conflicting || !truth.Live {
		return SessionAdmissionDecision{}, ErrUnauthorized
	}
	return SessionAdmissionDecision{Mode: SessionAdmissionCurrent, MayMutate: true}, nil
}

func (decision SessionAdmissionDecision) Allows(action SessionAdmissionAction) bool {
	switch decision.Mode {
	case SessionAdmissionAttachOnly:
		return action == SessionAdmissionAttach || action == SessionAdmissionStatus
	case SessionAdmissionCurrent:
		return decision.MayMutate && action != ""
	default:
		return false
	}
}

func hasExactSessionControl(principal Principal, sessionID string) bool {
	for _, scope := range principal.Scopes {
		if scope.Kind == KindSession && scope.ID == sessionID && scope.Access == AccessControl {
			return true
		}
	}
	return false
}

type SessionCredentialLineageKind string

const (
	SessionCredentialBootstrapInitial SessionCredentialLineageKind = "bootstrap_initial"
	SessionCredentialTargetAttach     SessionCredentialLineageKind = "target_attach"
)

type SessionCredentialLineage struct {
	Kind     SessionCredentialLineageKind
	AttachID string
	JTI      string
}

type SessionCredentialRequest struct {
	SessionID    string
	Lineage      SessionCredentialLineage
	Generation   int64
	RotationID   string
	RevocationID string
	ExpiresAt    time.Time
}

type PreparedSessionCredential struct {
	Bearer       string
	SessionID    string
	Lineage      SessionCredentialLineage
	Generation   int64
	RotationID   string
	RevocationID string
	ExpiresAt    time.Time
	Scope        Scope
}

// SessionCredentialIssuer prepares one in-memory Adapter bearer. It makes no
// persistent side effect; the caller must discard the result on transaction failure.
type SessionCredentialIssuer interface {
	PrepareSessionCredential(ctx context.Context, request SessionCredentialRequest) (PreparedSessionCredential, error)
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
