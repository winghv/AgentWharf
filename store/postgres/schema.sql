-- sqlc/self-hosted schema fixture for AgentWharf EventStore queries only.
-- It is not evidence of, or a replacement for, the platform production schema.
-- Production trigger/function behavior remains owned by platform migrations.
CREATE TABLE agent_sessions (
    id TEXT PRIMARY KEY
);

CREATE TABLE session_events (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    seq BIGINT NOT NULL CHECK (seq > 0),
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    proposal_id TEXT CHECK (proposal_id IS NULL OR char_length(proposal_id) BETWEEN 1 AND 255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, seq)
);

CREATE INDEX session_events_session_seq_idx ON session_events (session_id, seq);
CREATE UNIQUE INDEX session_events_proposal_id_idx
    ON session_events (session_id, proposal_id) WHERE proposal_id IS NOT NULL;

CREATE TABLE session_event_streams (
    session_id TEXT PRIMARY KEY CHECK (char_length(session_id) BETWEEN 1 AND 255),
    latest_seq BIGINT NOT NULL DEFAULT 0 CHECK (latest_seq >= 0),
    retention_gap BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
);

CREATE FUNCTION enforce_session_event_stream_monotonicity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.latest_seq < OLD.latest_seq THEN
        RAISE EXCEPTION 'session event stream latest_seq must not regress';
    END IF;
    IF OLD.retention_gap AND NOT NEW.retention_gap THEN
        RAISE EXCEPTION 'session event stream retention_gap must not clear';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER session_event_streams_enforce_monotonicity
BEFORE UPDATE ON session_event_streams FOR EACH ROW
EXECUTE FUNCTION enforce_session_event_stream_monotonicity();

CREATE FUNCTION track_session_event_stream_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO session_event_streams (
        session_id, latest_seq, retention_gap, created_at, updated_at
    ) VALUES (
        NEW.session_id, NEW.seq, NEW.seq <> 1, NEW.created_at, statement_timestamp()
    )
    ON CONFLICT (session_id) DO UPDATE
    SET latest_seq = GREATEST(session_event_streams.latest_seq, EXCLUDED.latest_seq),
        retention_gap = session_event_streams.retention_gap
            OR EXCLUDED.latest_seq > session_event_streams.latest_seq + 1,
        updated_at = statement_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER session_events_track_stream_insert
AFTER INSERT ON session_events FOR EACH ROW
EXECUTE FUNCTION track_session_event_stream_insert();

CREATE FUNCTION mark_session_event_stream_retention_gap()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE session_event_streams
    SET retention_gap = true, updated_at = statement_timestamp()
    WHERE session_id = OLD.session_id;
    RETURN OLD;
END;
$$;

CREATE TRIGGER session_events_mark_stream_retention_gap
AFTER DELETE ON session_events FOR EACH ROW
EXECUTE FUNCTION mark_session_event_stream_retention_gap();

CREATE TABLE session_attention_summaries (
    session_id TEXT PRIMARY KEY CHECK (char_length(session_id) BETWEEN 1 AND 255),
    latest_seq BIGINT NOT NULL DEFAULT 0 CHECK (latest_seq >= 0),
    state TEXT NOT NULL CHECK (state IN ('starting', 'ready', 'busy', 'waiting_permission', 'recovering', 'ended', 'error')),
    permission_id TEXT,
    permission_status TEXT,
    terminal_outcome TEXT,
    latest_change_seq BIGINT,
    blocker_kind TEXT,
    blocker_reason TEXT,
    blocker_expires_at TIMESTAMPTZ,
    blocking_session_id TEXT,
    blocker_operation TEXT,
    summary_version BIGINT NOT NULL DEFAULT 0 CHECK (summary_version >= 0),
    last_durable_event_at TIMESTAMPTZ,
    last_client_command_at TIMESTAMPTZ,
    projection_state TEXT NOT NULL DEFAULT 'incomplete' CHECK (projection_state IN ('complete', 'incomplete')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (latest_change_seq IS NULL OR (latest_change_seq > 0 AND latest_change_seq <= latest_seq)),
    CHECK ((permission_id IS NULL AND permission_status IS NULL) OR (char_length(permission_id) BETWEEN 1 AND 255 AND permission_status = 'pending')),
    CHECK (terminal_outcome IS NULL OR char_length(terminal_outcome) BETWEEN 1 AND 128),
    CHECK (blocking_session_id IS NULL OR char_length(blocking_session_id) BETWEEN 1 AND 255),
    CHECK (blocker_reason IS NULL OR char_length(blocker_reason) BETWEEN 1 AND 128),
    CHECK (blocker_operation IS NULL OR char_length(blocker_operation) BETWEEN 1 AND 128),
    CHECK (
        (blocker_kind IS NULL AND blocker_reason IS NULL AND blocker_expires_at IS NULL AND blocking_session_id IS NULL AND blocker_operation IS NULL)
        OR (blocker_kind = 'queued' AND blocker_operation IS NULL)
        OR (blocker_kind = 'outcome_unknown' AND blocker_reason IS NULL AND blocker_expires_at IS NULL AND blocking_session_id IS NULL)
        OR (blocker_kind IN ('reauthorization_required', 'new_run_required') AND blocker_reason IS NULL AND blocker_expires_at IS NULL AND blocking_session_id IS NULL AND blocker_operation IS NULL)
    )
);

CREATE INDEX session_attention_summaries_projection_state_session_idx
    ON session_attention_summaries (projection_state, session_id);

CREATE TABLE session_pending_commands (
    session_id TEXT NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 255) REFERENCES agent_sessions(id),
    cmd_id TEXT NOT NULL CHECK (char_length(cmd_id) BETWEEN 1 AND 256),
    type TEXT NOT NULL CHECK (type IN ('session.send')),
    event_seq BIGINT NOT NULL CHECK (event_seq > 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'received', 'completed', 'outcome_unknown')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (session_id, cmd_id),
    FOREIGN KEY (session_id, event_seq) REFERENCES session_events(session_id, seq),
    CHECK (expires_at > created_at),
    CHECK (expires_at <= created_at + interval '30 seconds')
);

CREATE INDEX session_pending_commands_status_expiry_idx
    ON session_pending_commands (status, expires_at);

CREATE TABLE session_attachments (
    attach_id TEXT PRIMARY KEY CHECK (char_length(attach_id) BETWEEN 1 AND 255),
    bootstrap_session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    target_session_id TEXT NOT NULL UNIQUE REFERENCES agent_sessions(id),
    status TEXT NOT NULL CHECK (status IN ('join_pending', 'queued', 'start_received', 'reauthorization_required', 'canceled')),
    delivery_state TEXT NOT NULL CHECK (delivery_state IN ('pending', 'received', 'completed', 'outcome_unknown')),
    delivery_version BIGINT NOT NULL DEFAULT 0 CHECK (delivery_version >= 0),
    queue_reason TEXT CHECK (queue_reason IS NULL OR char_length(queue_reason) BETWEEN 1 AND 128),
    expires_at TIMESTAMPTZ,
    canceled_at TIMESTAMPTZ,
    blocking_session_id TEXT REFERENCES agent_sessions(id),
    target_credential_lineage_ref TEXT NOT NULL CHECK (char_length(target_credential_lineage_ref) BETWEEN 1 AND 255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    CHECK (bootstrap_session_id <> target_session_id),
    CHECK (blocking_session_id IS NULL OR blocking_session_id <> target_session_id),
    CHECK (expires_at IS NULL OR expires_at > created_at),
    CHECK (canceled_at IS NULL OR canceled_at >= created_at),
    CHECK (
        (status = 'join_pending' AND delivery_state = 'pending' AND queue_reason IS NULL AND expires_at IS NOT NULL AND canceled_at IS NULL AND blocking_session_id IS NULL)
        OR (status = 'queued' AND delivery_state = 'pending' AND queue_reason IS NOT NULL AND expires_at IS NOT NULL AND canceled_at IS NULL AND blocking_session_id IS NOT NULL)
        OR (status = 'start_received' AND delivery_state IN ('received', 'completed', 'outcome_unknown') AND queue_reason IS NULL AND expires_at IS NULL AND canceled_at IS NULL AND blocking_session_id IS NULL)
        OR (status = 'reauthorization_required' AND delivery_state IN ('pending', 'outcome_unknown') AND queue_reason IS NULL AND expires_at IS NULL AND canceled_at IS NULL AND blocking_session_id IS NULL)
        OR (status = 'canceled' AND queue_reason IS NULL AND expires_at IS NULL AND canceled_at IS NOT NULL AND blocking_session_id IS NULL)
    )
);

CREATE INDEX session_attachments_status_expiry_idx
    ON session_attachments (status, expires_at);

CREATE TABLE session_adapter_connections (
    session_id TEXT PRIMARY KEY REFERENCES agent_sessions(id),
    connection_epoch BIGINT NOT NULL DEFAULT 0 CHECK (connection_epoch >= 0),
    accepted_fence BIGINT NOT NULL DEFAULT 0 CHECK (accepted_fence >= 0),
    active_credential_generation BIGINT NOT NULL CHECK (active_credential_generation > 0),
    credential_generation_high_watermark BIGINT NOT NULL CHECK (credential_generation_high_watermark > 0),
    active_credential_expires_at TIMESTAMPTZ NOT NULL,
    pending_credential_generation BIGINT CHECK (pending_credential_generation IS NULL OR pending_credential_generation > 0),
    pending_credential_expires_at TIMESTAMPTZ,
    prior_recovery_credential_generation BIGINT CHECK (prior_recovery_credential_generation IS NULL OR prior_recovery_credential_generation > 0),
    rotation_id TEXT CHECK (rotation_id IS NULL OR char_length(rotation_id) BETWEEN 1 AND 255),
    revoked_at TIMESTAMPTZ,
    terminal_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    CHECK (active_credential_expires_at > created_at),
    CHECK (pending_credential_expires_at IS NULL OR pending_credential_expires_at > created_at),
    CHECK ((pending_credential_generation IS NULL AND pending_credential_expires_at IS NULL AND rotation_id IS NULL) OR (pending_credential_generation IS NOT NULL AND pending_credential_expires_at IS NOT NULL AND rotation_id IS NOT NULL)),
    CHECK (pending_credential_generation IS NULL OR pending_credential_generation <> active_credential_generation),
    CHECK (prior_recovery_credential_generation IS NULL OR prior_recovery_credential_generation <> active_credential_generation),
    CHECK (pending_credential_generation IS NULL OR prior_recovery_credential_generation IS NULL OR pending_credential_generation <> prior_recovery_credential_generation),
    CHECK (active_credential_generation <= credential_generation_high_watermark),
    CHECK (pending_credential_generation IS NULL OR pending_credential_generation <= credential_generation_high_watermark),
    CHECK (prior_recovery_credential_generation IS NULL OR prior_recovery_credential_generation <= credential_generation_high_watermark),
    CHECK (revoked_at IS NULL OR revoked_at >= created_at),
    CHECK (terminal_at IS NULL OR terminal_at >= created_at)
);

CREATE INDEX session_adapter_connections_active_expiry_idx
    ON session_adapter_connections (active_credential_expires_at);

CREATE TABLE session_attach_attempts (
    attempt_jti_hash BYTEA PRIMARY KEY CHECK (octet_length(attempt_jti_hash) = 32),
    attach_id TEXT NOT NULL CHECK (char_length(attach_id) BETWEEN 1 AND 255),
    bootstrap_session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    target_session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    provider TEXT NOT NULL CHECK (char_length(provider) BETWEEN 1 AND 128),
    fingerprint_domain TEXT NOT NULL CHECK (fingerprint_domain = 'agentwharf.attach-request.v1'),
    fingerprint_version INTEGER NOT NULL CHECK (fingerprint_version = 1),
    fingerprint_digest BYTEA NOT NULL CHECK (octet_length(fingerprint_digest) = 32),
    fingerprint_key_version INTEGER NOT NULL CHECK (fingerprint_key_version > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    admission_outcome TEXT NOT NULL CHECK (admission_outcome IN ('accepted', 'rejected')),
    issued_credential_generation BIGINT CHECK (issued_credential_generation IS NULL OR issued_credential_generation > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    CHECK (bootstrap_session_id <> target_session_id),
    CHECK (expires_at > created_at),
    CHECK ((admission_outcome = 'accepted' AND issued_credential_generation IS NOT NULL) OR (admission_outcome = 'rejected' AND issued_credential_generation IS NULL))
);

CREATE INDEX session_attach_attempts_key_expiry_idx
    ON session_attach_attempts (fingerprint_key_version, expires_at);

CREATE TABLE session_workspace_leases (
    workspace_key TEXT PRIMARY KEY CHECK (char_length(workspace_key) BETWEEN 1 AND 255),
    worker_id TEXT NOT NULL CHECK (char_length(worker_id) BETWEEN 1 AND 255),
    session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    connection_epoch BIGINT NOT NULL CHECK (connection_epoch > 0),
    credential_generation BIGINT NOT NULL CHECK (credential_generation > 0),
    lease_id TEXT NOT NULL CHECK (char_length(lease_id) BETWEEN 1 AND 255),
    child_parent_workspace_key TEXT CHECK (char_length(child_parent_workspace_key) BETWEEN 1 AND 255),
    child_capability_digest BYTEA CHECK (octet_length(child_capability_digest) = 32),
    child_scope_expires_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('reserved', 'start_received', 'quarantined', 'released')),
    version BIGINT NOT NULL CHECK (version > 0),
    expires_at TIMESTAMPTZ,
    reservation_expires_at TIMESTAMPTZ NOT NULL,
    quarantine_reason TEXT CHECK (quarantine_reason IN ('authority_superseded', 'cleanup_uncertain', 'recovery_incomplete')),
    recovery_state TEXT NOT NULL CHECK (recovery_state IN ('not_required', 'pending', 'quiescent')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    released_at TIMESTAMPTZ,
    CHECK (
        (status = 'reserved' AND expires_at IS NOT NULL AND quarantine_reason IS NULL AND recovery_state = 'not_required' AND released_at IS NULL)
        OR (status = 'start_received' AND expires_at IS NULL AND quarantine_reason IS NULL AND recovery_state = 'not_required' AND released_at IS NULL)
        OR (status = 'quarantined' AND expires_at IS NULL AND quarantine_reason IS NOT NULL AND recovery_state = 'pending' AND released_at IS NULL)
        OR (status = 'released' AND expires_at IS NOT NULL AND quarantine_reason IS NULL AND recovery_state = 'quiescent' AND released_at IS NOT NULL)
    ),
    CHECK (
        (child_parent_workspace_key IS NULL AND child_capability_digest IS NULL AND child_scope_expires_at IS NULL)
        OR (
            child_parent_workspace_key IS NOT NULL
            AND child_capability_digest IS NOT NULL
            AND child_scope_expires_at IS NOT NULL
            AND child_parent_workspace_key <> workspace_key
            AND child_scope_expires_at > created_at
        )
    )
);

CREATE INDEX session_workspace_leases_owner_cas_idx
    ON session_workspace_leases (workspace_key, worker_id, session_id, connection_epoch, credential_generation, lease_id, version);
CREATE INDEX session_workspace_leases_released_cleanup_idx
    ON session_workspace_leases (expires_at) WHERE status = 'released';
