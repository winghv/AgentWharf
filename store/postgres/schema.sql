-- sqlc/self-hosted schema fixture for AgentWharf EventStore queries only.
-- It is not evidence of, or a replacement for, the platform production schema.
CREATE TABLE session_events (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    seq BIGINT NOT NULL CHECK (seq > 0),
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, seq)
);

CREATE INDEX session_events_session_seq_idx ON session_events (session_id, seq);
