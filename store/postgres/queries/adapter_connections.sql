-- name: InitializeAdapterConnection :one
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at
)
SELECT sqlc.arg(session_id), 0, 0, sqlc.arg(active_generation),
       sqlc.arg(active_generation), sqlc.arg(active_expires_at)
WHERE sqlc.arg(active_expires_at)::TIMESTAMPTZ > clock_timestamp()
ON CONFLICT (session_id) DO NOTHING
RETURNING *;

-- name: AdapterConnectionByID :one
SELECT * FROM session_adapter_connections WHERE session_id = $1;

-- name: MatchingInitialAdapterConnection :one
SELECT * FROM session_adapter_connections
WHERE session_id = sqlc.arg(session_id)
  AND connection_epoch = 0 AND accepted_fence = 0
  AND active_credential_generation = sqlc.arg(active_generation)
  AND credential_generation_high_watermark = sqlc.arg(active_generation)
  AND active_credential_expires_at = sqlc.arg(active_expires_at)
  AND pending_credential_generation IS NULL AND prior_recovery_credential_generation IS NULL
  AND rotation_id IS NULL AND revoked_at IS NULL AND terminal_at IS NULL;

-- name: AcceptAdapterHello :one
UPDATE session_adapter_connections
SET connection_epoch = connection_epoch + 1,
    accepted_fence = nextval('session_adapter_connection_accepted_fence_seq'),
    updated_at = clock_timestamp()
WHERE session_id = sqlc.arg(session_id)
  AND active_credential_generation = sqlc.arg(credential_generation)
  AND active_credential_expires_at > clock_timestamp()
  AND revoked_at IS NULL
  AND terminal_at IS NULL
RETURNING *;

-- name: ValidateAdapterAdmission :one
SELECT * FROM session_adapter_connections
WHERE session_id = sqlc.arg(session_id)
  AND active_credential_generation = sqlc.arg(credential_generation)
  AND connection_epoch = sqlc.arg(connection_epoch)
  AND accepted_fence = sqlc.arg(accepted_fence)
  AND sqlc.arg(grant_fence)::BIGINT > accepted_fence
  AND active_credential_expires_at > clock_timestamp()
  AND revoked_at IS NULL
  AND terminal_at IS NULL;

-- name: PrepareAdapterCredentialRotation :one
UPDATE session_adapter_connections
SET pending_credential_generation = sqlc.arg(pending_generation),
    pending_credential_expires_at = sqlc.arg(pending_expires_at),
    rotation_id = sqlc.arg(rotation_id),
    credential_generation_high_watermark = sqlc.arg(pending_generation),
    updated_at = clock_timestamp()
WHERE session_id = sqlc.arg(session_id)
  AND active_credential_generation = sqlc.arg(expected_active_generation)
  AND connection_epoch = sqlc.arg(expected_epoch)
  AND pending_credential_generation IS NULL
  AND sqlc.arg(pending_generation)::BIGINT > credential_generation_high_watermark
  AND active_credential_expires_at > clock_timestamp()
  AND sqlc.arg(pending_expires_at)::TIMESTAMPTZ > clock_timestamp()
  AND revoked_at IS NULL
  AND terminal_at IS NULL
RETURNING *;

-- name: ActivateAdapterCredential :one
UPDATE session_adapter_connections
SET prior_recovery_credential_generation = active_credential_generation,
    active_credential_generation = pending_credential_generation,
    active_credential_expires_at = pending_credential_expires_at,
    pending_credential_generation = NULL,
    pending_credential_expires_at = NULL,
    rotation_id = NULL,
    connection_epoch = connection_epoch + 1,
    accepted_fence = nextval('session_adapter_connection_accepted_fence_seq'),
    updated_at = clock_timestamp()
WHERE session_id = sqlc.arg(session_id)
  AND active_credential_generation = sqlc.arg(expected_active_generation)
  AND connection_epoch = sqlc.arg(expected_epoch)
  AND pending_credential_generation = sqlc.arg(pending_generation)
  AND rotation_id = sqlc.arg(rotation_id)
  AND active_credential_expires_at > clock_timestamp()
  AND pending_credential_expires_at > clock_timestamp()
  AND revoked_at IS NULL
  AND terminal_at IS NULL
RETURNING *;
