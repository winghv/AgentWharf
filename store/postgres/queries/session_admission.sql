-- name: SessionAdmissionTruth :one
SELECT id, provider, status, ended_at
FROM agent_sessions
WHERE id = sqlc.arg(session_id);

-- name: ProjectAgentSessionState :execrows
UPDATE agent_sessions
SET status = sqlc.arg(status)::text,
	ended_at = CASE
		WHEN sqlc.arg(terminal)::boolean THEN COALESCE(ended_at, sqlc.arg(observed_at)::timestamptz)
		ELSE ended_at
	END
WHERE id = sqlc.arg(session_id)::text
	AND status NOT IN ('ended', 'error');
