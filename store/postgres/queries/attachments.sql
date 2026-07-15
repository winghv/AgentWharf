-- name: AttachmentStoreNow :one
SELECT clock_timestamp()::TIMESTAMPTZ;

-- name: AttachmentByID :one
SELECT * FROM session_attachments WHERE attach_id = $1;

-- name: AttachmentByTarget :one
SELECT * FROM session_attachments WHERE target_session_id = $1;

-- name: LockAttachment :one
SELECT * FROM session_attachments WHERE attach_id = $1 FOR UPDATE;

-- name: InsertAttachment :one
INSERT INTO session_attachments (
    attach_id, bootstrap_session_id, target_session_id, status, delivery_state,
    delivery_version, expires_at, target_credential_lineage_ref
) VALUES (
    sqlc.arg(attach_id), sqlc.arg(bootstrap_session_id), sqlc.arg(target_session_id),
    'join_pending', 'pending', 0, sqlc.arg(expires_at), sqlc.arg(target_credential_lineage_ref)
)
RETURNING *;

-- name: UpdateAttachment :one
UPDATE session_attachments
SET status = sqlc.arg(status),
    delivery_state = sqlc.arg(delivery_state),
    delivery_version = delivery_version + 1,
    queue_reason = sqlc.narg(queue_reason),
    expires_at = sqlc.narg(expires_at),
    canceled_at = CASE WHEN sqlc.arg(status) = 'canceled' THEN clock_timestamp() ELSE NULL END,
    blocking_session_id = sqlc.narg(blocking_session_id),
    updated_at = clock_timestamp()
WHERE attach_id = sqlc.arg(attach_id)
  AND delivery_version = sqlc.arg(expected_version)
RETURNING *;
