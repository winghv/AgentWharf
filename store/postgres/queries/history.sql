-- name: SessionEventHistoryState :one
SELECT
    COALESCE(MAX(latest_seq), 0)::BIGINT AS latest_seq,
    COALESCE(BOOL_OR(retention_gap), false)::BOOLEAN AS retention_gap
FROM session_event_streams
WHERE session_id = $1;

-- name: ReverseSessionEventPage :many
SELECT session_id, seq, type, payload, created_at
FROM session_events
WHERE session_id = sqlc.arg(session_id)
ORDER BY seq DESC
LIMIT sqlc.arg(page_limit);

-- name: ReverseSessionEventPageBefore :many
SELECT session_id, seq, type, payload, created_at
FROM session_events
WHERE session_id = sqlc.arg(session_id)
  AND seq < sqlc.arg(before_seq)
ORDER BY seq DESC
LIMIT sqlc.arg(page_limit);
