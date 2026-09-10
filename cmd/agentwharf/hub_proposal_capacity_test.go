package main

import (
	"fmt"
	"github.com/winghv/agentwharf/protocol"
	"testing"
)

func TestHubProposalAcknowledgementsReleaseOrderStorage(t *testing.T) {
	connection := &hubConnection{pending: make(map[string]*protocol.Event)}
	for i := 0; i < 2*maxPendingHubProposals; i++ {
		id := fmt.Sprintf("proposal-%d", i)
		if err := connection.trackProposal(&protocol.Event{ProposalID: id}); err != nil {
			t.Fatal(err)
		}
		connection.ackProposal(id)
		if len(connection.pending) != 0 || len(connection.pendingOrder) != 0 {
			t.Fatal("ack retained queue metadata")
		}
	}
	for i := 0; i < maxPendingHubProposals; i++ {
		if err := connection.trackProposal(&protocol.Event{ProposalID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := connection.trackProposal(&protocol.Event{ProposalID: "overflow"}); err == nil {
		t.Fatal("unbounded proposal queue")
	}
	if len(connection.proposals()) != maxPendingHubProposals {
		t.Fatal("overflow discarded queued events")
	}
	connection.ackProposal("0")
	if err := connection.trackProposal(&protocol.Event{ProposalID: "after-ack"}); err != nil {
		t.Fatal(err)
	}
	proposals := connection.proposals()
	if proposals[0].ProposalID != "1" || proposals[len(proposals)-1].ProposalID != "after-ack" {
		t.Fatal("ack changed replay ordering")
	}
}
