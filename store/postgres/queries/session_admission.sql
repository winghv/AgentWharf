-- name: SessionAdmissionTruth :one
SELECT id, provider, status, ended_at
FROM agent_sessions
WHERE id = sqlc.arg(session_id);
