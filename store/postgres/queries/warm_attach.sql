-- name: WarmAttachStoreNow :one
SELECT clock_timestamp()::TIMESTAMPTZ;

-- name: LockWarmAttachBootstrap :one
SELECT * FROM session_adapter_connections
WHERE session_id = sqlc.arg(session_id)
  AND active_credential_generation = sqlc.arg(credential_generation)
  AND connection_epoch = sqlc.arg(connection_epoch)
  AND accepted_fence = sqlc.arg(accepted_fence)
  AND connection_epoch > 0 AND accepted_fence > 0
  AND sqlc.arg(grant_fence)::BIGINT > accepted_fence
  AND active_credential_expires_at > clock_timestamp()
  AND revoked_at IS NULL
  AND terminal_at IS NULL
FOR UPDATE;

-- name: LockWarmAttachTarget :one
SELECT target.id
FROM agent_sessions AS target
WHERE target.id = sqlc.arg(target_session_id)
  AND NOT EXISTS (
      SELECT 1
      FROM session_attention_summaries AS summary
      WHERE summary.session_id = target.id
        AND (summary.state IN ('ended', 'error') OR summary.terminal_outcome IS NOT NULL)
  )
FOR UPDATE;

-- name: InsertWarmAttachPendingCommand :one
INSERT INTO session_pending_commands (
    session_id, cmd_id, type, event_seq, status, expires_at
) SELECT
    sqlc.arg(session_id), sqlc.arg(cmd_id), 'session.send', sqlc.arg(event_seq),
    'pending', sqlc.arg(expires_at)
WHERE sqlc.arg(expires_at)::TIMESTAMPTZ > clock_timestamp()
  AND sqlc.arg(expires_at)::TIMESTAMPTZ <= clock_timestamp() + interval '30 seconds'
RETURNING *;

-- name: WarmAttachEventByTargetSeq :one
SELECT session_id, seq, type, payload, created_at
FROM session_events
WHERE session_id = sqlc.arg(session_id) AND seq = sqlc.arg(event_seq);
