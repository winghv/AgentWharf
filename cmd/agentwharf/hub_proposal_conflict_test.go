package main

import (
	"github.com/winghv/agentwharf/protocol"
	"testing"
)

func TestHubProposalConflictPreservesOriginal(t *testing.T) {
	connection := &hubConnection{pending: make(map[string]*protocol.Event)}
	original := protocol.Event{ProposalID: "proposal", SessionID: "session", Type: "session.state", Time: 1, Payload: []byte(`{"opaque":"original"}`)}
	if err := connection.trackProposal(&original); err != nil {
		t.Fatal(err)
	}
	if err := connection.trackProposal(&original); err != nil {
		t.Fatal("exact retry rejected", err)
	}
	for _, field := range []string{"payload", "session", "type", "time"} {
		changed := original
		switch field {
		case "payload":
			changed.Payload = []byte(`{"opaque":"changed"}`)
		case "session":
			changed.SessionID = "other"
		case "type":
			changed.Type = "session.message"
		case "time":
			changed.Time++
		}
		if err := connection.trackProposal(&changed); err == nil {
			t.Fatalf("%s conflict accepted", field)
		}
		pending := connection.proposals()
		if len(pending) != 1 || string(pending[0].Payload) != string(original.Payload) || pending[0].SessionID != original.SessionID || pending[0].Type != original.Type || pending[0].Time != original.Time {
			t.Fatal("original replay changed")
		}
	}
}
