-- name: InsertWorkspaceLease :one
INSERT INTO session_workspace_leases (
    workspace_key, worker_id, session_id, connection_epoch, credential_generation, lease_id,
    child_parent_workspace_key, child_capability_digest, child_scope_expires_at,
    status, version, expires_at, reservation_expires_at, recovery_state
)
SELECT
    sqlc.arg(workspace_key), sqlc.arg(worker_id), sqlc.arg(session_id),
    sqlc.arg(connection_epoch), sqlc.arg(credential_generation), sqlc.arg(lease_id),
    sqlc.narg(child_parent_workspace_key), sqlc.narg(child_capability_digest), sqlc.narg(child_scope_expires_at),
    'reserved', 1, sqlc.arg(expires_at), sqlc.arg(expires_at), 'not_required'
WHERE sqlc.narg(child_scope_expires_at)::TIMESTAMPTZ IS NULL
   OR sqlc.narg(child_scope_expires_at)::TIMESTAMPTZ > clock_timestamp()
ON CONFLICT (workspace_key) DO NOTHING
RETURNING *;

-- name: WorkspaceLeaseByKey :one
SELECT *
FROM session_workspace_leases
WHERE workspace_key = $1;

-- name: ReserveReleasedWorkspaceLease :one
UPDATE session_workspace_leases AS lease
SET worker_id = sqlc.arg(worker_id),
    session_id = sqlc.arg(session_id),
    connection_epoch = sqlc.arg(connection_epoch),
    credential_generation = sqlc.arg(credential_generation),
    lease_id = sqlc.arg(lease_id),
    child_parent_workspace_key = sqlc.narg(child_parent_workspace_key),
    child_capability_digest = sqlc.narg(child_capability_digest),
    child_scope_expires_at = sqlc.narg(child_scope_expires_at),
    status = 'reserved', version = lease.version + 1,
    expires_at = sqlc.arg(expires_at), reservation_expires_at = sqlc.arg(expires_at), quarantine_reason = NULL,
    recovery_state = 'not_required', released_at = NULL
WHERE lease.workspace_key = sqlc.arg(workspace_key)
  AND lease.status = 'released'
  AND (sqlc.narg(child_scope_expires_at)::TIMESTAMPTZ IS NULL OR sqlc.narg(child_scope_expires_at)::TIMESTAMPTZ > clock_timestamp())
RETURNING lease.*;

-- name: RecordWorkspaceStartReceived :one
UPDATE session_workspace_leases AS lease
SET status = 'start_received', version = lease.version + 1, expires_at = NULL
FROM session_adapter_connections AS authority
JOIN session_attachments AS attachment ON attachment.target_session_id = authority.session_id
WHERE lease.workspace_key = sqlc.arg(workspace_key)
  AND lease.status = 'reserved'
  AND lease.version = sqlc.arg(expected_version)
  AND (lease.worker_id, lease.session_id, lease.connection_epoch, lease.credential_generation, lease.lease_id)
      = (sqlc.arg(worker_id), sqlc.arg(session_id), sqlc.arg(connection_epoch)::BIGINT, sqlc.arg(credential_generation)::BIGINT, sqlc.arg(lease_id))
  AND lease.expires_at > clock_timestamp()
  AND (lease.child_scope_expires_at IS NULL OR lease.child_scope_expires_at > clock_timestamp())
  AND authority.session_id = lease.session_id
  AND authority.connection_epoch = lease.connection_epoch
  AND authority.active_credential_generation = lease.credential_generation
  AND authority.active_credential_expires_at > clock_timestamp()
  AND authority.revoked_at IS NULL
  AND authority.terminal_at IS NULL
  AND attachment.target_session_id = lease.session_id
  AND attachment.status IN ('queued', 'start_received')
  AND (attachment.expires_at IS NULL OR attachment.expires_at > clock_timestamp())
RETURNING lease.*;

-- name: QuarantineWorkspaceLease :one
UPDATE session_workspace_leases AS lease
SET status = 'quarantined', version = lease.version + 1, expires_at = NULL,
    quarantine_reason = 'authority_superseded', recovery_state = 'pending'
WHERE lease.workspace_key = sqlc.arg(workspace_key)
  AND lease.version = sqlc.arg(expected_version)
  AND lease.status IN ('reserved', 'start_received')
RETURNING lease.*;

-- name: ReleaseWorkspaceLeaseAfterQuiescence :one
UPDATE session_workspace_leases AS lease
SET status = 'released', version = lease.version + 1,
    expires_at = CASE WHEN lease.expires_at > clock_timestamp() THEN lease.expires_at
                      ELSE clock_timestamp() + interval '5 minutes' END,
    quarantine_reason = NULL, recovery_state = 'quiescent', released_at = clock_timestamp()
WHERE lease.workspace_key = sqlc.arg(workspace_key)
  AND lease.version = sqlc.arg(expected_version)
  AND lease.status IN ('reserved', 'start_received', 'quarantined')
  AND (lease.worker_id, lease.session_id, lease.connection_epoch, lease.credential_generation, lease.lease_id)
      = (sqlc.arg(worker_id), sqlc.arg(session_id), sqlc.arg(connection_epoch)::BIGINT, sqlc.arg(credential_generation)::BIGINT, sqlc.arg(lease_id))
RETURNING lease.*;
