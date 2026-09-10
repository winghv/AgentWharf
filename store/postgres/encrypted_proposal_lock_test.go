package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestEncryptedProposalLocksStreamBeforeAuthority(t *testing.T) {
	h := newPostgresProposalHarness(t)
	tracer := newQueryStartSignal("pg_advisory_xact_lock")
	h.pool.Close()
	h.reopenWithTracer(t, tracer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocker := openPool(t, h.dsn, h.schemaName, nil)
	defer blocker.Close()
	stream, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Rollback(context.Background())
	if _, err = stream.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, commandAdvisoryLockKey("ses_proposal_1")); err != nil {
		t.Fatal(err)
	}
	proposal := encryptedProposal(t, "ordered")
	result := make(chan error, 1)
	go func() {
		_, err := h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, proposal)
		result <- err
	}()
	select {
	case <-tracer.started:
	case <-ctx.Done():
		t.Fatal("proposal did not reach stream lock")
	}
	// NOWAIT proves the waiting proposal has not locked the authority row.
	authority, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var one int
	lockErr := authority.QueryRow(ctx, `SELECT 1 FROM session_adapter_connections WHERE session_id='ses_proposal_1' FOR UPDATE NOWAIT`).Scan(&one)
	_ = authority.Rollback(context.Background())
	_ = stream.Rollback(context.Background())
	commitErr := <-result
	if lockErr != nil {
		t.Fatalf("proposal held authority before stream: %v", lockErr)
	}
	if commitErr != nil {
		t.Fatal(commitErr)
	}
}
