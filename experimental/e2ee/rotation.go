package e2ee

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// SealRotationThreshold rekeys a session before its content key reaches the
// hard per-key seal bound, leaving headroom for in-flight seals that complete
// under the outgoing epoch during the transition. Rotation is the only
// recovery: the per-key seal budget is durable and never resets within one
// key epoch, so a long-running session that seals past the hard bound would
// otherwise wedge its adapter in a permanent restart loop.
const SealRotationThreshold = MaxEndpointSealsPerKey - MaxEndpointSealsPerKey/4

// SealsUsed reports the durable seal usage of the session's current key epoch.
// A session without any seal usage reports zero; an unknown session fails.
func (v *SessionKeyVault) SealsUsed(ctx context.Context, session string) (int64, error) {
	if v == nil || !identifier.MatchString(session) {
		return 0, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var used int64
	err := v.db.QueryRowContext(ctx, `SELECT COALESCE((
		SELECT b.used FROM e2ee_seal_budget b
		JOIN e2ee_local_sessions s ON s.session=b.session AND s.key_id=b.key_id
		WHERE b.device=? AND b.session=?), 0)`, v.identity.Device, session).Scan(&used)
	if err != nil {
		return 0, ErrJournal
	}
	return used, nil
}

// RotateSessionKeysForBudget advances the session to a fresh key epoch with its
// current grants (plus the machine's own control grant) and resets the
// per-epoch command admission budget in the same durable transaction. The
// machine endpoint already seals every session event and holds every content
// key, so this local rekey grants it no new access; membership itself is
// unchanged and still requires a controller-signed membership change to edit.
// Historical keys remain decryptable for replay. ErrConflict means another
// rotation already advanced the epoch; callers reload the current key and
// continue.
func (v *SessionKeyVault) RotateSessionKeysForBudget(ctx context.Context, journal *CommandJournal, session string) (string, error) {
	if v == nil || journal == nil || journal.db != v.db || !identifier.MatchString(session) {
		return "", ErrInvalid
	}
	epoch, _, err := journal.SessionState(ctx, session)
	if err != nil {
		return "", err
	}
	grants, err := journal.sessionGrants(ctx, session)
	if err != nil {
		return "", err
	}
	grants, err = v.withMachineControlGrant(grants)
	if err != nil {
		return "", err
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", ErrJournal
	}
	newKeyID := hex.EncodeToString(raw)
	if err := v.TransitionSession(ctx, journal, session, newKeyID, epoch, grants); err != nil {
		return "", err
	}
	return newKeyID, nil
}
