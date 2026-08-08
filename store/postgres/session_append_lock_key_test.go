package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

// TestSessionAppendLockKeyIsTheKeyAppendActuallyTakes pins the cross-module
// contract that the platform's Session hard delete depends on.
//
// SessionAppendLockKey is exported so the platform deleter can take the SAME
// advisory lock that Append takes. session_events and session_event_streams
// carry no foreign key to agent_sessions, so a row-level FOR UPDATE on the
// Session row does not conflict with an append; this advisory lock is the only
// thing that serializes them. If the exported key ever stopped matching the key
// Append takes internally, both sides would still acquire a lock, both would
// still succeed, and the serialization would silently disappear -- taking with
// it the guarantee that a "permanent erasure" cannot leave tenant content
// behind as an unreachable orphan.
//
// The assertion is behavioral, not a hash comparison: an equality check against
// a recomputed FNV value would pass even if Append had been changed to lock a
// different key. Instead this holds pg_advisory_xact_lock on the exported key
// from an independent connection and requires that Append blocks.
func TestSessionAppendLockKeyIsTheKeyAppendActuallyTakes(t *testing.T) {
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_append_lock_key_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
	pool := openPool(t, dsn, schemaName, nil)
	t.Cleanup(pool.Close)
	resetSchema(t, pool)

	events := postgres.New(pool)
	ctx := context.Background()

	const sessionID = "ses_append_lock_key"
	if _, err := pool.Exec(ctx, `INSERT INTO agent_sessions (id, provider, status, started_at) VALUES ($1, 'claude-code', 'starting', clock_timestamp())`, sessionID); err != nil {
		t.Fatalf("seed session: %v", err)
	}

	key := postgres.SessionAppendLockKey(sessionID)

	// Hold the lock on a dedicated connection, outside the pool Append uses, so
	// the only thing that can make Append wait is the lock itself.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock holder: %v", err)
	}
	holderDone := false
	defer func() {
		if !holderDone {
			_ = holder.Rollback(ctx)
		}
	}()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
		t.Fatalf("hold advisory lock on exported key: %v", err)
	}

	appended := make(chan error, 1)
	go func() {
		_, err := events.Append(ctx, sessionID, []store.PendingEvent{
			{Type: "session.message", Time: time.Now(), Payload: []byte(`{"text":"x"}`)},
		})
		appended <- err
	}()

	select {
	case err := <-appended:
		t.Fatalf("Append() returned %v while the exported key was locked; Append does not take pg_advisory_xact_lock on SessionAppendLockKey, so the platform's Session delete no longer serializes against it", err)
	case <-time.After(600 * time.Millisecond):
		// Correct: Append is waiting.
	}

	// Stronger evidence than "it was slow": prove Append is blocked on this
	// exact advisory lock, not on something incidental. classid/objid is how
	// Postgres splits a bigint advisory key in pg_locks.
	var waiting bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_locks
    WHERE locktype = 'advisory'
      AND NOT granted
      AND classid = ($1::bigint >> 32)::int
      AND objid = ($1::bigint & 4294967295)::int
)`, key).Scan(&waiting); err != nil {
		t.Fatalf("inspect pg_locks: %v", err)
	}
	if !waiting {
		t.Fatalf("no ungranted advisory lock on classid/objid derived from SessionAppendLockKey(%q)=%d; Append is blocked on something else", sessionID, key)
	}

	// Releasing the lock must let the same Append through unchanged.
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("release advisory lock: %v", err)
	}
	holderDone = true

	select {
	case err := <-appended:
		if err != nil {
			t.Fatalf("Append() after lock release: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Append() never completed after the advisory lock was released")
	}

	var seq int64
	if err := pool.QueryRow(ctx, `SELECT seq FROM session_events WHERE session_id=$1`, sessionID).Scan(&seq); err != nil {
		t.Fatalf("read appended event: %v", err)
	}
	if seq != 1 {
		t.Fatalf("appended event seq = %d, want 1", seq)
	}
}

// TestSessionAppendLockKeyIsStableAndSessionScoped guards the two properties the
// platform side relies on when it computes the key independently: the same
// Session ID must always produce the same key (or the two modules would stop
// meeting), and different Session IDs must not collide onto one key (or
// unrelated deletes and appends would serialize against each other).
func TestSessionAppendLockKeyIsStableAndSessionScoped(t *testing.T) {
	const id = "ses_stable"
	first := postgres.SessionAppendLockKey(id)
	for i := 0; i < 3; i++ {
		if got := postgres.SessionAppendLockKey(id); got != first {
			t.Fatalf("SessionAppendLockKey(%q) call %d = %d, want %d; the key is not deterministic, so the platform deleter cannot reproduce it", id, i, got, first)
		}
	}
	if other := postgres.SessionAppendLockKey("ses_other"); other == first {
		t.Fatalf("SessionAppendLockKey collided for distinct sessions (both %d)", other)
	}
	if empty := postgres.SessionAppendLockKey(""); empty == first {
		t.Fatalf("SessionAppendLockKey(\"\") collided with %q", id)
	}
}

// assertNoErr keeps the errors import honest if the file is trimmed later.
var _ = errors.Is

var _ *pgxpool.Pool
