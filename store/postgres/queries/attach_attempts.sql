-- name: AttachAttemptStoreNow :one
SELECT clock_timestamp()::TIMESTAMPTZ;

-- name: InsertAttachAttempt :one
INSERT INTO session_attach_attempts (
    attempt_jti_hash, attach_id, bootstrap_session_id, target_session_id, provider,
    fingerprint_domain, fingerprint_version, fingerprint_digest, fingerprint_key_version,
    expires_at, admission_outcome, issued_credential_generation
) VALUES (
    sqlc.arg(attempt_jti_hash), sqlc.arg(attach_id), sqlc.arg(bootstrap_session_id),
    sqlc.arg(target_session_id), sqlc.arg(provider), sqlc.arg(fingerprint_domain),
    sqlc.arg(fingerprint_version), sqlc.arg(fingerprint_digest), sqlc.arg(fingerprint_key_version),
    sqlc.arg(expires_at), sqlc.arg(admission_outcome), sqlc.narg(issued_credential_generation)
)
ON CONFLICT (attempt_jti_hash) DO NOTHING
RETURNING *;

-- name: AttachAttemptByJTIHash :one
SELECT * FROM session_attach_attempts WHERE attempt_jti_hash = $1;
