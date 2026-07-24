package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidSessionCredential         = errors.New("invalid session credential")
	ErrSessionCredentialRequired        = errors.New("session credential is required")
	ErrSessionCredentialMismatch        = errors.New("session credential session mismatch")
	ErrSessionCredentialExpired         = errors.New("session credential expired")
	ErrSessionCredentialNotSerializable = errors.New("session credential is not serializable")
)

// SessionCredentialLineage identifies one Hub-issued credential lineage
// without carrying bearer material or Provider data.
type SessionCredentialLineage struct {
	Kind     string
	AttachID string
	JTI      string
}

// SessionCredentialMetadata is the non-secret routing metadata owned by one
// SessionWorker. It is safe to copy, but never contains the bearer.
type SessionCredentialMetadata struct {
	SessionID  string
	Lineage    SessionCredentialLineage
	Generation int64
	ExpiresAt  time.Time
}

// SessionCredential keeps the bearer private to the trusted Adapter package.
// There is intentionally no getter, map key, JSON representation, or text
// representation that can move it across the Worker boundary.
type SessionCredential struct {
	bearer   string
	metadata SessionCredentialMetadata
}

func NewSessionCredential(bearer string, metadata SessionCredentialMetadata) (*SessionCredential, error) {
	if strings.TrimSpace(bearer) == "" || len(bearer) > 4096 || strings.ContainsAny(bearer, "\x00\r\n") {
		return nil, ErrInvalidSessionCredential
	}
	if metadata.SessionID == "" || metadata.Lineage.Kind == "" || metadata.Generation < 1 || !metadata.ExpiresAt.After(time.Now()) {
		return nil, ErrInvalidSessionCredential
	}
	return &SessionCredential{bearer: bearer, metadata: metadata}, nil
}

func (c *SessionCredential) Metadata() (SessionCredentialMetadata, error) {
	if c == nil {
		return SessionCredentialMetadata{}, ErrSessionCredentialRequired
	}
	return c.metadata, nil
}

func (c *SessionCredential) validate(sessionID string, now time.Time) error {
	if c == nil || c.bearer == "" || c.metadata.SessionID == "" || c.metadata.Lineage.Kind == "" || c.metadata.Generation < 1 {
		return ErrSessionCredentialRequired
	}
	if c.metadata.SessionID != sessionID {
		return ErrSessionCredentialMismatch
	}
	if !c.metadata.ExpiresAt.After(now) {
		return ErrSessionCredentialExpired
	}
	return nil
}

func (c SessionCredential) String() string {
	return "[session credential redacted]"
}

func (c SessionCredential) GoString() string {
	return c.String()
}

func (c SessionCredential) MarshalJSON() ([]byte, error) {
	return nil, ErrSessionCredentialNotSerializable
}

func (c SessionCredential) MarshalText() ([]byte, error) {
	return nil, ErrSessionCredentialNotSerializable
}

func (c SessionCredential) Format(state fmt.State, verb rune) {
	_, _ = state.Write([]byte(c.String()))
}

var _ json.Marshaler = SessionCredential{}
var _ fmt.Formatter = SessionCredential{}
