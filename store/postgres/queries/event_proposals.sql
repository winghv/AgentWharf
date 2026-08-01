-- name: ProposedEventByID :one
SELECT session_id, seq, proposal_id,
       type = sqlc.arg(event_type)::TEXT
       AND payload = sqlc.arg(payload)::JSONB
       AND created_at = sqlc.arg(created_at)::TIMESTAMPTZ AS matches
FROM session_events
WHERE session_id = sqlc.arg(session_id)
  AND proposal_id = sqlc.arg(proposal_id);

-- name: InsertProposedEvent :one
INSERT INTO session_events (session_id, seq, type, payload, proposal_id, created_at)
SELECT sqlc.arg(session_id), sqlc.arg(seq), sqlc.arg(event_type), sqlc.arg(payload),
       sqlc.arg(proposal_id), sqlc.arg(created_at)
FROM session_adapter_connections AS authority
WHERE authority.session_id = sqlc.arg(session_id)
  AND authority.connection_epoch = sqlc.arg(connection_epoch)
  AND authority.active_credential_generation = sqlc.arg(credential_generation)
  AND authority.active_credential_expires_at > clock_timestamp()
  AND authority.revoked_at IS NULL
  AND authority.terminal_at IS NULL
RETURNING session_id, seq, proposal_id;
