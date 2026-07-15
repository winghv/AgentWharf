-- name: LockSessionEventStream :exec
SELECT pg_advisory_xact_lock($1);

-- name: LatestSessionEventSeq :one
SELECT COALESCE((
    SELECT latest_seq
    FROM session_event_streams
    WHERE session_id = $1
), 0)::BIGINT;

-- name: InsertSessionEvent :exec
INSERT INTO session_events (session_id, seq, type, payload, created_at)
VALUES ($1, $2, $3, $4, $5);

-- name: NextSessionEvent :one
SELECT session_id, seq, type, payload, created_at
FROM session_events
WHERE session_id = $1 AND seq > $2
ORDER BY seq ASC
LIMIT 1;
