-- name: LockCommandAuthority :one
SELECT true AS current
FROM session_adapter_connections
WHERE session_id = sqlc.arg(session_id)
  AND connection_epoch = sqlc.arg(connection_epoch)
  AND active_credential_generation = sqlc.arg(credential_generation)
  AND active_credential_expires_at > statement_timestamp()
  AND revoked_at IS NULL
  AND terminal_at IS NULL
FOR UPDATE;

-- name: CommandStoreNow :one
SELECT statement_timestamp()::TIMESTAMPTZ;

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

-- name: LockPendingCommandForResolve :one
SELECT *
FROM session_pending_commands
WHERE session_id = $1 AND cmd_id = $2
FOR UPDATE;

-- name: InsertPendingCommand :one
INSERT INTO session_pending_commands (
    session_id, cmd_id, type, event_seq, status, expires_at
) VALUES (
    sqlc.arg(session_id), sqlc.arg(cmd_id), sqlc.arg(type), sqlc.arg(event_seq), 'pending', sqlc.arg(expires_at)
)
RETURNING *;

-- name: UpdatePendingCommandStatus :one
UPDATE session_pending_commands
SET status = sqlc.arg(status), updated_at = statement_timestamp()
WHERE session_id = sqlc.arg(session_id)
  AND cmd_id = sqlc.arg(cmd_id)
  AND status = sqlc.arg(expected_status)
RETURNING *;
