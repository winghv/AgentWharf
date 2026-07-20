-- name: AttentionSnapshot :many
SELECT session_id, latest_seq, state, permission_id, permission_status,
       terminal_outcome, latest_change_seq, blocker_kind, blocker_reason,
       blocker_expires_at, blocking_session_id, blocker_operation,
       summary_version, last_durable_event_at, last_client_command_at,
       projection_state, created_at, updated_at
FROM session_attention_summaries
WHERE session_id = ANY(sqlc.arg(session_ids)::TEXT[])
ORDER BY session_id ASC;

-- name: AttentionSummaryPage :many
SELECT session_id, latest_seq, state, permission_id, permission_status,
       terminal_outcome, latest_change_seq, blocker_kind, blocker_reason,
       blocker_expires_at, blocking_session_id, blocker_operation,
       summary_version, last_durable_event_at, last_client_command_at,
       projection_state, created_at, updated_at
FROM session_attention_summaries
WHERE session_id > sqlc.arg(after_session_id)::TEXT
ORDER BY session_id ASC
LIMIT sqlc.arg(page_limit)::INT;

-- name: AttentionStoreNow :one
SELECT clock_timestamp()::TIMESTAMPTZ;

-- name: UpsertAttentionEvent :exec
INSERT INTO session_attention_summaries (
    session_id, latest_seq, state, permission_id, permission_status, terminal_outcome,
    latest_change_seq, last_durable_event_at, projection_state
) VALUES (
    sqlc.arg(session_id), sqlc.arg(latest_seq), COALESCE(sqlc.narg(event_state), 'starting'),
    sqlc.narg(permission_id)::TEXT, CASE WHEN sqlc.narg(permission_id)::TEXT IS NULL THEN NULL ELSE 'pending' END,
    sqlc.narg(terminal_outcome)::TEXT, sqlc.arg(latest_change_seq), sqlc.arg(event_time),
    CASE
		WHEN sqlc.arg(latest_seq)::BIGINT > 1 THEN 'incomplete'
        WHEN sqlc.arg(state_observed)::BOOLEAN AND sqlc.narg(event_state) IS NULL THEN 'incomplete'
        WHEN sqlc.narg(event_state) IS NULL THEN 'incomplete'
        ELSE 'complete'
    END
)
ON CONFLICT (session_id) DO UPDATE
SET latest_seq = EXCLUDED.latest_seq,
    state = COALESCE(sqlc.narg(event_state), session_attention_summaries.state),
    permission_id = CASE
        WHEN NOT sqlc.arg(permission_change)::BOOLEAN THEN session_attention_summaries.permission_id
        WHEN sqlc.narg(permission_id)::TEXT IS NOT NULL THEN sqlc.narg(permission_id)::TEXT
        WHEN session_attention_summaries.permission_id = sqlc.narg(permission_decision_id)::TEXT THEN NULL
        ELSE session_attention_summaries.permission_id
    END,
    permission_status = CASE
        WHEN NOT sqlc.arg(permission_change)::BOOLEAN THEN session_attention_summaries.permission_status
        WHEN sqlc.narg(permission_id)::TEXT IS NOT NULL THEN 'pending'
        WHEN session_attention_summaries.permission_id = sqlc.narg(permission_decision_id)::TEXT THEN NULL
        ELSE session_attention_summaries.permission_status
    END,
    terminal_outcome = COALESCE(session_attention_summaries.terminal_outcome, sqlc.narg(terminal_outcome)::TEXT),
    latest_change_seq = COALESCE(EXCLUDED.latest_change_seq, session_attention_summaries.latest_change_seq),
    last_durable_event_at = EXCLUDED.last_durable_event_at,
    projection_state = CASE
        WHEN session_attention_summaries.projection_state = 'incomplete' THEN 'incomplete'
        WHEN EXCLUDED.latest_seq <> session_attention_summaries.latest_seq + 1 THEN 'incomplete'
        WHEN sqlc.arg(projection_incomplete)::BOOLEAN THEN 'incomplete'
        WHEN sqlc.arg(state_observed)::BOOLEAN AND sqlc.narg(event_state) IS NULL THEN 'incomplete'
        ELSE session_attention_summaries.projection_state
    END,
    updated_at = statement_timestamp()
WHERE session_attention_summaries.latest_seq < EXCLUDED.latest_seq;

-- name: FenceAttentionTerminal :exec
UPDATE session_adapter_connections
SET revoked_at = clock_timestamp(), terminal_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE session_id = sqlc.arg(session_id)
  AND terminal_at IS NULL;

-- name: UpsertAttentionLedger :exec
INSERT INTO session_attention_summaries (
    session_id, state, blocker_kind, blocker_reason, blocker_expires_at,
    blocking_session_id, blocker_operation, summary_version,
    last_client_command_at, projection_state
) VALUES (
    sqlc.arg(session_id), 'starting', sqlc.narg(blocker_kind), sqlc.narg(blocker_reason),
    sqlc.narg(blocker_expires_at), sqlc.narg(blocking_session_id), sqlc.narg(blocker_operation),
    1, sqlc.narg(client_command_at), 'incomplete'
)
ON CONFLICT (session_id) DO UPDATE
SET blocker_kind = sqlc.narg(blocker_kind),
    blocker_reason = sqlc.narg(blocker_reason),
    blocker_expires_at = sqlc.narg(blocker_expires_at),
    blocking_session_id = sqlc.narg(blocking_session_id),
    blocker_operation = sqlc.narg(blocker_operation),
    summary_version = session_attention_summaries.summary_version + 1,
    last_client_command_at = COALESCE(sqlc.narg(client_command_at), session_attention_summaries.last_client_command_at),
    updated_at = statement_timestamp();
