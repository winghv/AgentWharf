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

-- name: RefreshAdapterCredentialBeforeHello :one
WITH locked AS MATERIALIZED (
  SELECT connection.* FROM session_adapter_connections connection
  WHERE connection.session_id = sqlc.arg(session_id)
  FOR UPDATE
), refreshed AS (
  UPDATE session_adapter_connections AS connection
  SET active_credential_expires_at = sqlc.arg(active_expires_at),
      updated_at = clock_timestamp()
  WHERE connection.session_id = sqlc.arg(session_id)
    AND EXISTS (SELECT 1 FROM locked)
    AND connection.active_credential_generation = sqlc.arg(expected_active_generation)
    AND connection.connection_epoch = 0 AND connection.accepted_fence = 0
    AND connection.pending_credential_generation IS NULL AND connection.prior_recovery_credential_generation IS NULL
    AND connection.rotation_id IS NULL AND connection.revoked_at IS NULL AND connection.terminal_at IS NULL
    AND connection.active_credential_expires_at <= clock_timestamp()
    AND sqlc.arg(active_expires_at)::TIMESTAMPTZ > clock_timestamp()
    AND sqlc.arg(active_expires_at)::TIMESTAMPTZ > connection.active_credential_expires_at
  RETURNING connection.*
)
SELECT * FROM refreshed
UNION ALL
SELECT locked.* FROM locked
WHERE locked.active_credential_generation = sqlc.arg(expected_active_generation)
  AND locked.connection_epoch = 0 AND locked.accepted_fence = 0
  AND locked.active_credential_expires_at = sqlc.arg(active_expires_at)
  AND locked.active_credential_expires_at > clock_timestamp()
  AND locked.pending_credential_generation IS NULL AND locked.prior_recovery_credential_generation IS NULL
  AND locked.rotation_id IS NULL AND locked.revoked_at IS NULL AND locked.terminal_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM refreshed)
LIMIT 1;

-- name: TerminateAdapterConnectionBeforeHello :one
WITH locked AS MATERIALIZED (
  SELECT connection.* FROM session_adapter_connections connection
  WHERE connection.session_id = sqlc.arg(session_id)
  FOR UPDATE
), terminated AS (
  UPDATE session_adapter_connections AS connection
  SET revoked_at = statement_timestamp(), terminal_at = statement_timestamp(),
      updated_at = statement_timestamp()
  WHERE connection.session_id = sqlc.arg(session_id)
    AND EXISTS (SELECT 1 FROM locked)
    AND connection.active_credential_generation = sqlc.arg(expected_active_generation)
    AND connection.connection_epoch = 0 AND connection.accepted_fence = 0
    AND connection.pending_credential_generation IS NULL AND connection.prior_recovery_credential_generation IS NULL
    AND connection.rotation_id IS NULL AND connection.revoked_at IS NULL AND connection.terminal_at IS NULL
  RETURNING connection.*
)
SELECT * FROM terminated
UNION ALL
SELECT locked.* FROM locked
WHERE locked.active_credential_generation = sqlc.arg(expected_active_generation)
  AND locked.connection_epoch = 0 AND locked.accepted_fence = 0
  AND locked.pending_credential_generation IS NULL AND locked.prior_recovery_credential_generation IS NULL
  AND locked.rotation_id IS NULL AND locked.revoked_at IS NOT NULL AND locked.terminal_at IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM terminated)
LIMIT 1;

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
  AND connection_epoch > 0 AND accepted_fence > 0
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
  AND connection_epoch > 0 AND accepted_fence > 0
  AND (pending_credential_generation IS NULL OR (pending_credential_expires_at IS NOT NULL AND pending_credential_expires_at <= clock_timestamp()))
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
  AND connection_epoch > 0 AND accepted_fence > 0
  AND pending_credential_generation = sqlc.arg(pending_generation)
  AND rotation_id = sqlc.arg(rotation_id)
  AND active_credential_expires_at > clock_timestamp()
  AND pending_credential_expires_at > clock_timestamp()
  AND revoked_at IS NULL
  AND terminal_at IS NULL
RETURNING *;
