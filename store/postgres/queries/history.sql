-- name: SessionEventHistoryBounds :one
SELECT
    COALESCE(MIN(seq), 0)::BIGINT AS earliest_seq,
    COALESCE(MAX(seq), 0)::BIGINT AS latest_seq
FROM session_events
WHERE session_id = $1;

-- name: ReverseSessionEventPage :many
SELECT session_id, seq, type, payload, created_at
FROM session_events
WHERE session_id = sqlc.arg(session_id)
  AND (
      sqlc.narg(before_seq)::BIGINT IS NULL
      OR seq < sqlc.narg(before_seq)::BIGINT
  )
ORDER BY seq DESC
LIMIT sqlc.arg(page_limit);
