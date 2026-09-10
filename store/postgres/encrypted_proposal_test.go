package postgres_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/store"
)

func encryptedProposal(t *testing.T, id string) store.ProposedEventRequest {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal(err)
	}
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "event", Session: "ses_proposal_1", Sender: "machine", KeyID: "key", MessageID: id, Type: "session.state"}, key, private, e2ee.PublicMetadata{State: "ready"}, json.RawMessage(`{"state":"ready","reason":"synthetic-private-reason"}`))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(e2ee.EventWire{Version: 1, Scope: "event", Sender: "machine", KeyID: "key", MessageID: id, Type: "session.state", Packet: packet})
	if err != nil {
		t.Fatal(err)
	}
	return store.ProposedEventRequest{ProposalID: id, Event: store.PendingEvent{Type: "session.state", Time: time.Now().UTC().Truncate(time.Microsecond), Payload: payload}}
}

func TestEncryptedProposalRetryConflictRestartAndAuthority(t *testing.T) {
	h := newPostgresProposalHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	proposal := encryptedProposal(t, "encrypted-proposal")
	type result struct {
		receipt store.ProposedEventReceipt
		err     error
	}
	results := make(chan result, 8)
	for i := 0; i < 8; i++ {
		go func() {
			receipt, err := h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", authority, proposal)
			results <- result{receipt, err}
		}()
	}
	for i := 0; i < 8; i++ {
		got := <-results
		if got.err != nil || got.receipt.Seq != 1 {
			t.Errorf("concurrent proposal=%+v %v", got.receipt, got.err)
		}
	}
	first, err := h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", authority, proposal)
	if err != nil || first.Seq != 1 {
		t.Fatalf("commit=%+v %v", first, err)
	}
	snapshot, err := h.AttentionSnapshot(ctx, []string{"ses_proposal_1"})
	if err != nil || len(snapshot) != 1 || snapshot[0].State != "ready" || snapshot[0].LatestSeq != 1 {
		t.Fatalf("projection=%+v %v", snapshot, err)
	}
	for i := 0; i < 2; i++ {
		if i == 1 {
			h.pool.Close()
			h.reopen(t)
		}
		duplicate, err := h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", authority, proposal)
		if err != nil || duplicate.Seq != first.Seq {
			t.Fatalf("duplicate=%+v %v", duplicate, err)
		}
	}
	conflict := encryptedProposal(t, proposal.ProposalID)
	if _, err = h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", authority, conflict); err == nil {
		t.Fatal("same proposal ID with new ciphertext accepted")
	}
	plaintext := proposal
	plaintext.ProposalID = "plaintext"
	plaintext.Event.Payload = []byte(`{"state":"ready"}`)
	if _, err = h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", authority, plaintext); err == nil {
		t.Fatal("plaintext accepted")
	}
	if latest, err := h.LatestSeq(ctx, "ses_proposal_1"); err != nil || latest != 1 {
		t.Fatalf("failed retry changed seq=%d %v", latest, err)
	}
	if _, err = h.pool.Exec(ctx, `UPDATE session_adapter_connections SET revoked_at=clock_timestamp() WHERE session_id='ses_proposal_1'`); err != nil {
		t.Fatal(err)
	}
	if _, err = h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", authority, proposal); err == nil {
		t.Fatal("revoked authority accepted duplicate")
	}
}

func TestEncryptedAttentionBackfillPreservesProjectionAndRejectsMixedStream(t *testing.T) {
	h := newPostgresProposalHarness(t)
	ctx := context.Background()
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	if _, err := h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", authority, encryptedProposal(t, "backfill")); err != nil {
		t.Fatal(err)
	}
	command := encryptedPendingEventOfType(t, "approval-command", "permission.respond")
	if _, err := h.CommitEncryptedPendingCommand(ctx, "ses_proposal_1", authority, command, store.PendingCommandRequest{CommandID: "approval-command", Type: "permission.respond", ExpiresAt: time.Now().Add(20 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	before, err := h.AttentionSnapshot(ctx, []string{"ses_proposal_1"})
	if err != nil || len(before) != 1 {
		t.Fatal("missing initial snapshot", err)
	}
	if _, err := h.BackfillEncryptedAttentionSession(ctx, "ses_proposal_1"); err != nil {
		t.Fatal(err)
	}
	after, err := h.AttentionSnapshot(ctx, []string{"ses_proposal_1"})
	if err != nil || len(after) != 1 || after[0].State != "ready" || after[0].LatestSeq != 2 {
		t.Fatalf("backfill lost public state: %+v %v", after, err)
	}
	// Simulate a historical mixed stream without changing the live projection.
	if _, err := h.pool.Exec(ctx, `INSERT INTO session_events(session_id,seq,type,payload,created_at) VALUES('ses_proposal_1',3,'session.state','{"state":"busy"}',clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.BackfillEncryptedAttentionSession(ctx, "ses_proposal_1"); err == nil {
		t.Fatal("mixed stream accepted")
	}
	failed, err := h.AttentionSnapshot(ctx, []string{"ses_proposal_1"})
	if err != nil || len(failed) != 1 || failed[0].State != after[0].State || failed[0].LatestSeq != after[0].LatestSeq {
		t.Fatalf("failed rebuild changed projection: %+v %v", failed, err)
	}
}

func TestEncryptedProposalInsertFailureRollsBack(t *testing.T) {
	h := newPostgresProposalHarness(t)
	ctx := context.Background()
	if _, err := h.pool.Exec(ctx, `CREATE FUNCTION reject_encrypted_proposal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic insert failure'; END; $$;
 CREATE TRIGGER reject_encrypted_proposal BEFORE INSERT ON session_events FOR EACH ROW EXECUTE FUNCTION reject_encrypted_proposal();`); err != nil {
		t.Fatal(err)
	}
	_, err := h.CommitEncryptedProposedEvent(ctx, "ses_proposal_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, encryptedProposal(t, "failure"))
	if err == nil {
		t.Fatal("insert failure accepted")
	}
	assertProposedEventRollback(t, h, "ses_proposal_1")
}
