-- name: LockCommandAuthority :one
SELECT true AS current
FROM session_adapter_connections AS authority
WHERE authority.session_id = sqlc.arg(session_id)
  AND authority.connection_epoch = sqlc.arg(connection_epoch)
  AND authority.active_credential_generation = sqlc.arg(credential_generation)
  AND authority.active_credential_expires_at > clock_timestamp()
  AND authority.revoked_at IS NULL
  AND authority.terminal_at IS NULL
FOR UPDATE;

-- name: CommandStoreNow :one
SELECT clock_timestamp()::TIMESTAMPTZ;

-- name: CommandAuthorityCurrent :one
SELECT EXISTS (
    SELECT 1
    FROM session_adapter_connections AS authority
    WHERE authority.session_id = sqlc.arg(session_id)
      AND authority.connection_epoch = sqlc.arg(connection_epoch)
      AND authority.active_credential_generation = sqlc.arg(credential_generation)
      AND authority.active_credential_expires_at > clock_timestamp()
      AND authority.revoked_at IS NULL
      AND authority.terminal_at IS NULL
) AS current;

-- name: PendingCommandByID :one
SELECT *
FROM session_pending_commands
WHERE session_id = $1 AND cmd_id = $2;

-- name: LockPendingCommandForClaim :one
SELECT *
FROM session_pending_commands
WHERE session_id = $1 AND cmd_id = $2
  AND expires_at > statement_timestamp()
FOR UPDATE;

-- name: ListPendingCommandsForDelivery :many
SELECT command.*
FROM session_pending_commands AS command
JOIN session_events AS event
  ON event.session_id = command.session_id AND event.seq = command.event_seq
WHERE command.session_id = $1
  AND command.status IN ('pending', 'received')
  AND command.expires_at > clock_timestamp()
  AND event.type = 'session.message'
  AND length(event.payload) BETWEEN 1 AND 65536
ORDER BY command.event_seq ASC;

-- name: LockPendingCommandForResolve :one
SELECT *
FROM session_pending_commands
WHERE session_id = $1 AND cmd_id = $2
FOR UPDATE;

-- name: InsertPendingCommand :one
INSERT INTO session_pending_commands (
    session_id, cmd_id, type, event_seq, status, expires_at
) SELECT
    sqlc.arg(session_id), sqlc.arg(cmd_id), sqlc.arg(type), sqlc.arg(event_seq), 'pending', sqlc.arg(expires_at)
FROM session_adapter_connections AS authority
WHERE authority.session_id = sqlc.arg(session_id)
  AND authority.connection_epoch = sqlc.arg(connection_epoch)
  AND authority.active_credential_generation = sqlc.arg(credential_generation)
  AND authority.active_credential_expires_at > clock_timestamp()
  AND authority.revoked_at IS NULL
  AND authority.terminal_at IS NULL
  AND sqlc.arg(expires_at)::TIMESTAMPTZ > clock_timestamp()
  AND sqlc.arg(expires_at)::TIMESTAMPTZ <= clock_timestamp() + interval '30 seconds'
RETURNING *;

-- name: UpdatePendingCommandStatus :one
UPDATE session_pending_commands AS command
SET status = sqlc.arg(status), updated_at = statement_timestamp()
WHERE command.session_id = sqlc.arg(session_id)
  AND command.cmd_id = sqlc.arg(cmd_id)
  AND command.status = sqlc.arg(expected_status)
  AND (NOT sqlc.arg(require_unexpired)::BOOLEAN OR command.expires_at > clock_timestamp())
  AND EXISTS (
      SELECT 1
      FROM session_adapter_connections AS authority
      WHERE authority.session_id = sqlc.arg(session_id)
        AND authority.connection_epoch = sqlc.arg(connection_epoch)
        AND authority.active_credential_generation = sqlc.arg(credential_generation)
        AND authority.active_credential_expires_at > clock_timestamp()
        AND authority.revoked_at IS NULL
        AND authority.terminal_at IS NULL
  )
RETURNING command.*;

-- name: ResolvePendingCommandUnknown :one
UPDATE session_pending_commands
SET status = 'outcome_unknown', updated_at = statement_timestamp()
WHERE session_id = $1 AND cmd_id = $2 AND status = 'received'
RETURNING *;
