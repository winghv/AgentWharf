package core

import (
	"github.com/winghv/agentwharf/protocol"
	"testing"
)

func TestMaskedEventPreservesProposalIdentity(t *testing.T) {
	seq := int64(7)
	event := protocol.Event{Type: "session.state", SessionID: "session", ProposalID: "ready-proposal", Seq: &seq, Payload: []byte(`{"state":"ready"}`)}
	masked, err := NewEventMasker(nil).MaskEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if masked.ProposalID != event.ProposalID || masked.Seq == event.Seq || *masked.Seq != seq {
		t.Fatal("masking changed transport identity or aliased seq")
	}
	masked.Payload[0] = ' '
	if event.Payload[0] != '{' {
		t.Fatal("masking aliased caller payload")
	}
}
