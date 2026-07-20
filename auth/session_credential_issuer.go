package auth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"
)

const (
	sessionCredentialIssuerDomain = "agentwharf.session-credential.v1"
	sealedSessionCredentialDomain = "agentwharf.session-credential.v2"
	maxLocalSessionCredentials    = 128
)

type sealedSessionCredential struct {
	KeyVersion   int64  `json:"key_version"`
	SessionID    string `json:"session_id"`
	LineageKind  string `json:"lineage_kind"`
	AttachID     string `json:"attach_id"`
	JTI          string `json:"jti"`
	Generation   int64  `json:"generation"`
	RotationID   string `json:"rotation_id"`
	RevocationID string `json:"revocation_id"`
	ExpiresAtNS  int64  `json:"expires_at_ns"`
}

// LocalSessionCredentialIssuer keeps bounded pending and active bearer state only
// in memory. Callers must activate only credentials backed by durable commit truth.
type LocalSessionCredentialIssuer struct {
	key        []byte
	keyVersion int64

	mu       sync.Mutex
	prepared map[string]PreparedSessionCredential
	pending  map[string]struct{}
	active   map[string]string
	retired  map[string]struct{}
}

func NewLocalSessionCredentialIssuer(key []byte, keyVersion int64) (*LocalSessionCredentialIssuer, error) {
	if len(key) == 0 || keyVersion < 1 {
		return nil, ErrUnauthorized
	}
	return &LocalSessionCredentialIssuer{
		key: append([]byte(nil), key...), keyVersion: keyVersion,
		prepared: make(map[string]PreparedSessionCredential), pending: make(map[string]struct{}), active: make(map[string]string), retired: make(map[string]struct{}),
	}, nil
}

func (issuer *LocalSessionCredentialIssuer) PrepareSessionCredential(ctx context.Context, request SessionCredentialRequest) (PreparedSessionCredential, error) {
	if err := ctx.Err(); err != nil {
		return PreparedSessionCredential{}, err
	}
	if issuer == nil || len(issuer.key) == 0 || issuer.keyVersion < 1 || !validSessionCredentialRequest(request) {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	bearer, err := issuer.sealSessionCredential(request)
	if err != nil {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	prepared := PreparedSessionCredential{
		Bearer:       bearer,
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
		issuer.retireLocked(current)
		delete(issuer.prepared, current)
	}
	delete(issuer.pending, prepared.Bearer)
	issuer.active[lineage] = prepared.Bearer
	return nil
}

// ValidateSessionCredentialActivation proves that the exact prepared bearer
// can be activated before a caller commits its durable Store tuple. A
// successful preflight makes the subsequent activation infallible for this
// issuer under the caller's non-cancellable context.
func (issuer *LocalSessionCredentialIssuer) ValidateSessionCredentialActivation(ctx context.Context, prepared PreparedSessionCredential) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if issuer == nil || prepared.Bearer == "" || !prepared.ExpiresAt.After(time.Now()) {
		return ErrUnauthorized
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.discardExpiredLocked(time.Now())
	stored, ok := issuer.prepared[prepared.Bearer]
	if !ok || stored != prepared {
		return ErrUnauthorized
	}
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
	issuer.retireLocked(prepared.Bearer)
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
	if prepared, ok := issuer.prepared[bearer]; ok {
		if issuer.active[localSessionCredentialLineage(SessionCredentialRequest{SessionID: prepared.SessionID, Lineage: prepared.Lineage})] == bearer {
			return prepared, nil
		}
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	if _, retired := issuer.retired[bearer]; retired {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	return issuer.openSessionCredential(bearer)
}

func (issuer *LocalSessionCredentialIssuer) retireLocked(bearer string) {
	if bearer == "" {
		return
	}
	if len(issuer.retired) >= maxLocalSessionCredentials {
		for retired := range issuer.retired {
			delete(issuer.retired, retired)
			break
		}
	}
	issuer.retired[bearer] = struct{}{}
}

func (issuer *LocalSessionCredentialIssuer) sealSessionCredential(request SessionCredentialRequest) (string, error) {
	sealed := sealedSessionCredential{
		KeyVersion: issuer.keyVersion, SessionID: request.SessionID, LineageKind: string(request.Lineage.Kind), AttachID: request.Lineage.AttachID,
		JTI: request.Lineage.JTI, Generation: request.Generation, RotationID: request.RotationID, RevocationID: request.RevocationID,
		ExpiresAtNS: request.ExpiresAt.UTC().UnixNano(),
	}
	payload, err := json.Marshal(sealed)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(sessionCredentialSealKey(issuer.key))
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, issuer.key)
	writeAttachCommitField(mac, sealedSessionCredentialDomain)
	_, _ = mac.Write(payload)
	nonce := mac.Sum(nil)[:aead.NonceSize()]
	ciphertext := aead.Seal(nil, nonce, payload, []byte(sealedSessionCredentialDomain))
	return "awsc_v2_" + base64.RawURLEncoding.EncodeToString(append(nonce, ciphertext...)), nil
}

func (issuer *LocalSessionCredentialIssuer) openSessionCredential(bearer string) (PreparedSessionCredential, error) {
	if len(bearer) <= len("awsc_v2_") || bearer[:len("awsc_v2_")] != "awsc_v2_" {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	encoded, err := base64.RawURLEncoding.DecodeString(bearer[len("awsc_v2_"):])
	if err != nil {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	block, err := aes.NewCipher(sessionCredentialSealKey(issuer.key))
	if err != nil {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(encoded) <= aead.NonceSize() {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	payload, err := aead.Open(nil, encoded[:aead.NonceSize()], encoded[aead.NonceSize():], []byte(sealedSessionCredentialDomain))
	if err != nil {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	var sealed sealedSessionCredential
	if json.Unmarshal(payload, &sealed) != nil || sealed.KeyVersion != issuer.keyVersion {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	request := SessionCredentialRequest{
		SessionID: sealed.SessionID, Lineage: SessionCredentialLineage{Kind: SessionCredentialLineageKind(sealed.LineageKind), AttachID: sealed.AttachID, JTI: sealed.JTI},
		Generation: sealed.Generation, RotationID: sealed.RotationID, RevocationID: sealed.RevocationID, ExpiresAt: time.Unix(0, sealed.ExpiresAtNS),
	}
	if !validSessionCredentialRequest(request) {
		return PreparedSessionCredential{}, ErrUnauthorized
	}
	return PreparedSessionCredential{Bearer: bearer, SessionID: request.SessionID, Lineage: request.Lineage, Generation: request.Generation,
		RotationID: request.RotationID, RevocationID: request.RevocationID, ExpiresAt: request.ExpiresAt, Scope: SessionAdapter(request.SessionID)}, nil
}

func sessionCredentialSealKey(key []byte) []byte {
	digest := sha256.Sum256(append([]byte(sealedSessionCredentialDomain+"\x00"), key...))
	return digest[:]
}

func (issuer *LocalSessionCredentialIssuer) SessionCredentialEvidence(ctx context.Context, bearer string) (SessionCredentialEvidence, error) {
	prepared, err := issuer.ActiveSessionCredential(ctx, bearer)
	if err != nil {
		return SessionCredentialEvidence{}, err
	}
	return SessionCredentialEvidence{
		SessionID: prepared.SessionID, Lineage: prepared.Lineage, Generation: prepared.Generation,
		RotationID: prepared.RotationID, RevocationID: prepared.RevocationID, ExpiresAt: prepared.ExpiresAt,
	}, nil
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
	case SessionCredentialTargetRotation:
		return request.Lineage.AttachID != "" && request.Lineage.JTI == ""
	default:
		return false
	}
}
