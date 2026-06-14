package core_test

import (
	"errors"
	"testing"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
)

func TestAdapterConnectionStateBuildsInitialAndResumeHello(t *testing.T) {
	state, err := core.NewAdapterConnectionState(core.AdapterConnectionConfig{
		SessionID: "ses_1",
		Provider:  "claude-code",
		Token:     "adapter-token",
	})
	if err != nil {
		t.Fatalf("NewAdapterConnectionState() error = %v", err)
	}

	initial := state.Hello()
	if initial.ProtocolVersion != protocol.ProtocolVersion ||
		initial.Role != protocol.RoleAdapter ||
		initial.SessionID != "ses_1" ||
		initial.Provider != "claude-code" ||
		initial.Token != "adapter-token" ||
		initial.Resume {
		t.Fatalf("initial hello = %+v", initial)
	}

	beforeAck := state.Hello()
	if beforeAck.Resume {
		t.Fatalf("hello before ack resume = true, want false")
	}

	summary, err := state.MarkAccepted(protocol.HelloAck{
		ProtocolVersion: protocol.ProtocolVersion,
		Sessions: []protocol.SessionSummary{{
			SessionID:  "ses_1",
			State:      "ready",
			Provider:   "claude-code",
			LatestSeq:  9,
			ReplayFrom: 10,
		}},
	})
	if err != nil {
		t.Fatalf("MarkAccepted() error = %v", err)
	}
	if summary.LatestSeq != 9 || summary.ReplayFrom != 10 {
		t.Fatalf("accepted summary = %+v", summary)
	}

	resume := state.Hello()
	if !resume.Resume {
		t.Fatalf("resume hello = %+v, want resume=true", resume)
	}
	if resume.SessionID != initial.SessionID || resume.Provider != initial.Provider || resume.Token != initial.Token {
		t.Fatalf("resume hello changed identity: initial=%+v resume=%+v", initial, resume)
	}
}

func TestAdapterConnectionStateRejectsInvalidConfigAndAck(t *testing.T) {
	if _, err := core.NewAdapterConnectionState(core.AdapterConnectionConfig{}); !errors.Is(err, core.ErrInvalidAdapterConnectionConfig) {
		t.Fatalf("NewAdapterConnectionState(empty) error = %v, want ErrInvalidAdapterConnectionConfig", err)
	}

	state, err := core.NewAdapterConnectionState(core.AdapterConnectionConfig{
		SessionID: "ses_1",
		Provider:  "claude-code",
		Token:     "adapter-token",
	})
	if err != nil {
		t.Fatalf("NewAdapterConnectionState() error = %v", err)
	}

	badAcks := []protocol.HelloAck{
		{ProtocolVersion: protocol.ProtocolVersion},
		{ProtocolVersion: 999, Sessions: []protocol.SessionSummary{{SessionID: "ses_1"}}},
		{ProtocolVersion: protocol.ProtocolVersion, Sessions: []protocol.SessionSummary{{SessionID: "ses_other"}}},
		{ProtocolVersion: protocol.ProtocolVersion, Sessions: []protocol.SessionSummary{{SessionID: "ses_1", Provider: "other"}}},
	}
	for _, ack := range badAcks {
		if _, err := state.MarkAccepted(ack); !errors.Is(err, core.ErrInvalidHelloAck) {
			t.Fatalf("MarkAccepted(%+v) error = %v, want ErrInvalidHelloAck", ack, err)
		}
	}

	if state.Hello().Resume {
		t.Fatalf("invalid ack enabled resume")
	}
}
