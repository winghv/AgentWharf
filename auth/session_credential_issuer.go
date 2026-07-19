package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"sync"
	"time"
)

const (
	sessionCredentialIssuerDomain = "agentwharf.session-credential.v1"
	maxLocalSessionCredentials    = 128
)

// LocalSessionCredentialIssuer keeps bounded pending and active bearer state only
// in memory. Callers must activate only credentials backed by durable commit truth.
type LocalSessionCredentialIssuer struct {
	key        []byte
	keyVersion int64

	mu       sync.Mutex
	prepared map[string]PreparedSessionCredential
	pending  map[string]struct{}
	active   map[string]string
}

func NewLocalSessionCredentialIssuer(key []byte, keyVersion int64) (*LocalSessionCredentialIssuer, error) {
	if len(key) == 0 || keyVersion < 1 {
		return nil, ErrUnauthorized
	}
	return &LocalSessionCredentialIssuer{
		key: append([]byte(nil), key...), keyVersion: keyVersion,
		prepared: make(map[string]PreparedSessionCredential), pending: make(map[string]struct{}), active: make(map[string]string),
	}, nil
}

func (issuer *LocalSessionCredentialIssuer) PrepareSessionCredential(ctx context.Context, request SessionCredentialRequest) (PreparedSessionCredential, error) {
	if err := ctx.Err(); err != nil {
		return PreparedSessionCredential{}, err
	}
	if issuer == nil || len(issuer.key) == 0 || issuer.keyVersion < 1 || !validSessionCredentialRequest(request) {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	mac := hmac.New(sha256.New, issuer.key)
	for _, field := range []string{
		sessionCredentialIssuerDomain,
		strconv.FormatInt(issuer.keyVersion, 10),
		request.SessionID,
		string(request.Lineage.Kind),
		request.Lineage.AttachID,
		request.Lineage.JTI,
		strconv.FormatInt(request.Generation, 10),
		request.RotationID,
		request.RevocationID,
		strconv.FormatInt(request.ExpiresAt.UTC().UnixNano(), 10),
	} {
		writeAttachCommitField(mac, field)
	}
	prepared := PreparedSessionCredential{
		Bearer:       "awsc_v1_" + strconv.FormatInt(issuer.keyVersion, 10) + "_" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
		SessionID:    request.SessionID,
		Lineage:      request.Lineage,
		Generation:   request.Generation,
		RotationID:   request.RotationID,
		RevocationID: request.RevocationID,
		ExpiresAt:    request.ExpiresAt,
		Scope:        SessionAdapter(request.SessionID),
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.discardExpiredLocked(time.Now())
	if known, ok := issuer.prepared[prepared.Bearer]; ok {
		return known, nil
	}
	if len(issuer.prepared) >= maxLocalSessionCredentials {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	issuer.prepared[prepared.Bearer] = prepared
	issuer.pending[prepared.Bearer] = struct{}{}
	return prepared, nil
}

// ActivateSessionCredential makes a prepared bearer usable only after its caller
// has committed the corresponding durable authorization tuple.
func (issuer *LocalSessionCredentialIssuer) ActivateSessionCredential(ctx context.Context, prepared PreparedSessionCredential) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if issuer == nil || prepared.Bearer == "" {
		return ErrUnauthorized
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.discardExpiredLocked(time.Now())
	stored, ok := issuer.prepared[prepared.Bearer]
	if !ok || stored != prepared {
		return ErrUnauthorized
	}
	lineage := localSessionCredentialLineage(SessionCredentialRequest{SessionID: prepared.SessionID, Lineage: prepared.Lineage})
	if current := issuer.active[lineage]; current != "" && current != prepared.Bearer {
		delete(issuer.prepared, current)
	}
	delete(issuer.pending, prepared.Bearer)
	issuer.active[lineage] = prepared.Bearer
	return nil
}

// DiscardSessionCredential removes a pending or active bearer when durable commit
// or delivery cannot prove that it remains safe to use.
func (issuer *LocalSessionCredentialIssuer) DiscardSessionCredential(_ context.Context, prepared PreparedSessionCredential) {
	if issuer == nil || prepared.Bearer == "" {
		return
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if stored, ok := issuer.prepared[prepared.Bearer]; !ok || stored != prepared {
		return
	}
	delete(issuer.prepared, prepared.Bearer)
	delete(issuer.pending, prepared.Bearer)
	lineage := localSessionCredentialLineage(SessionCredentialRequest{SessionID: prepared.SessionID, Lineage: prepared.Lineage})
	if issuer.active[lineage] == prepared.Bearer {
		delete(issuer.active, lineage)
	}
}

// AuthenticateSessionCredential accepts only a still-active, unexpired bearer
// prepared by this in-memory issuer. It never accepts client, group, or API scope.
func (issuer *LocalSessionCredentialIssuer) AuthenticateSessionCredential(ctx context.Context, bearer string) (Principal, error) {
	prepared, err := issuer.ActiveSessionCredential(ctx, bearer)
	if err != nil {
		return Principal{}, err
	}
	return Principal{Subject: "local-session-credential", Scopes: []Scope{prepared.Scope}}, nil
}

func (issuer *LocalSessionCredentialIssuer) ActiveSessionCredential(ctx context.Context, bearer string) (PreparedSessionCredential, error) {
	if err := ctx.Err(); err != nil {
		return PreparedSessionCredential{}, err
	}
	if issuer == nil || bearer == "" {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.discardExpiredLocked(time.Now())
	prepared, ok := issuer.prepared[bearer]
	if !ok || issuer.active[localSessionCredentialLineage(SessionCredentialRequest{SessionID: prepared.SessionID, Lineage: prepared.Lineage})] != bearer {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	return prepared, nil
}

func (issuer *LocalSessionCredentialIssuer) discardExpiredLocked(now time.Time) {
	for bearer, prepared := range issuer.prepared {
		if !prepared.ExpiresAt.After(now) {
			delete(issuer.prepared, bearer)
			delete(issuer.pending, bearer)
			if issuer.active[localSessionCredentialLineage(SessionCredentialRequest{SessionID: prepared.SessionID, Lineage: prepared.Lineage})] == bearer {
				delete(issuer.active, localSessionCredentialLineage(SessionCredentialRequest{SessionID: prepared.SessionID, Lineage: prepared.Lineage}))
			}
		}
	}
}

func localSessionCredentialLineage(request SessionCredentialRequest) string {
	return request.SessionID + "\x00" + string(request.Lineage.Kind)
}

func validSessionCredentialRequest(request SessionCredentialRequest) bool {
	if request.SessionID == "" || request.Generation < 1 || request.RotationID == "" || request.RevocationID == "" ||
		request.ExpiresAt.IsZero() || !request.ExpiresAt.After(time.Now()) ||
		!boundedAttachGrantStrings(request.SessionID, request.Lineage.AttachID, request.Lineage.JTI, request.RotationID, request.RevocationID) {
		return false
	}
	switch request.Lineage.Kind {
	case SessionCredentialBootstrapInitial:
		return request.Lineage.AttachID == "" && request.Lineage.JTI == ""
	case SessionCredentialTargetAttach:
		return request.Lineage.AttachID != "" && request.Lineage.JTI != ""
	default:
		return false
	}
}
