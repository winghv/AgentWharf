package hub

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestProposalRecoveryRetainsSelectedCommitAndCarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	request := store.ProposedEventRequest{ProposalID: "proposal", Event: store.PendingEvent{Type: "session.message", Payload: []byte(`{"opaque":"ciphertext"}`)}}
	calls := 0
	expected := store.ProposedEventReceipt{SessionID: "session", ProposalID: "proposal", Seq: 1, Status: store.ProposedEventAccepted}
	commit := func(attempt context.Context, session string, authority store.CommandAuthority, got store.ProposedEventRequest) (store.ProposedEventReceipt, error) {
		calls++
		if session != "session" || got.ProposalID != request.ProposalID || !bytes.Equal(got.Event.Payload, request.Event.Payload) {
			t.Fatal("recovery changed request")
		}
		if calls == 1 {
			cancel()
			return store.ProposedEventReceipt{}, context.DeadlineExceeded
		}
		if attempt.Err() != nil {
			t.Fatal("recovery reused cancelled context")
		}
		if _, ok := attempt.Deadline(); !ok {
			t.Fatal("unbounded recovery")
		}
		return expected, nil
	}
	receipt, err := commitProposalWithRecovery(ctx, commit, "session", store.CommandAuthority{}, request)
	if err != nil || receipt != expected || calls != 2 {
		t.Fatalf("recovery failed: %v calls=%d", err, calls)
	}
	failure := errors.New("permanent store failure")
	calls = 0
	_, err = commitProposalWithRecovery(context.Background(), func(context.Context, string, store.CommandAuthority, store.ProposedEventRequest) (store.ProposedEventReceipt, error) {
		calls++
		return store.ProposedEventReceipt{}, failure
	}, "session", store.CommandAuthority{}, request)
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatal("permanent failure retried")
	}
}

// Embedding the legacy interface deliberately provides no encrypted capability.
// Calling its nil methods would panic, so this also proves no legacy fallback.
type legacyOnlyProposalStore struct{ store.ProposedEventStore }

func TestRequiredProposalRejectsLegacyStoreBeforeSideEffects(t *testing.T) {
	handler := &webSocketHandler{events: legacyOnlyProposalStore{}}
	adapter := &adapterConnection{contentMode: protocol.ContentModeRequired}
	err := handler.commitAdapterProposal(context.Background(), adapter, protocol.Event{SessionID: "session", Type: "session.message"}, "proposal", nil)
	if err == nil || !strings.Contains(err.Error(), "encrypted proposed event store") {
		t.Fatalf("required proposal result: %v", err)
	}
}
