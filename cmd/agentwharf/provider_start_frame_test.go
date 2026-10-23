package main

import (
	"context"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

// The Adapter publishes a durable event (the discovered provider commands)
// before it asks the Hub to admit the Provider start. The Hub answers that
// event with a receipt, and the receipt arrives on the same socket the
// admission reads.
//
// The admission's first exchange owns the socket reader directly, so it used
// to treat that receipt as the answer to its own question and reject the
// preparation. The Session then never started: the Adapter restarted, the Hub
// rejected the next attempt too, and the Session sat in `starting` until its
// claim expired.
//
// The lifecycle frames must be waited for, not assumed to be the next frame.

func TestProviderStartAdmissionSkipsUnrelatedFramesBeforePrepare(t *testing.T) {
	frames := []protocol.Frame{
		&protocol.EventReceipt{ProposalID: "proposal-1", Status: protocol.EventReceiptAccepted},
		&protocol.Ping{Nonce: "n"},
		&protocol.ProviderStartPrepare{Attempt: 1},
	}
	index := 0
	admission := newProviderStartAdmission(protocol.ProtocolVersionV2, func(context.Context) (protocol.Frame, error) {
		if index >= len(frames) {
			return nil, errors.New("no more frames")
		}
		frame := frames[index]
		index++
		return frame, nil
	}, func(protocol.Frame) error { return nil }, nil)

	if err := admission.PrepareProcessStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareProcessStart() = %v", err)
	}
	if index != 3 {
		t.Fatalf("frames read = %d, want 3", index)
	}
}

func TestProviderStartAdmissionSkipsUnrelatedFramesBeforeAck(t *testing.T) {
	frames := []protocol.Frame{
		&protocol.ProviderStartPrepare{Attempt: 1},
		&protocol.EventReceipt{ProposalID: "proposal-2", Status: protocol.EventReceiptAccepted},
		&protocol.ProviderStartAck{Attempt: 1, Status: protocol.ProviderStartAdmitted, RecoveryHandle: "handle"},
	}
	index := 0
	admission := newProviderStartAdmission(protocol.ProtocolVersionV2, func(context.Context) (protocol.Frame, error) {
		if index >= len(frames) {
			return nil, errors.New("no more frames")
		}
		frame := frames[index]
		index++
		return frame, nil
	}, func(protocol.Frame) error { return nil }, nil)

	if err := admission.PrepareProcessStart(context.Background(), 1); err != nil {
		t.Fatalf("PrepareProcessStart() = %v", err)
	}
	if err := admission.ConfirmProcessStarted(context.Background(), 1); err != nil {
		t.Fatalf("ConfirmProcessStarted() = %v", err)
	}
}

func TestProviderStartAdmissionStillRejectsAWrongAttempt(t *testing.T) {
	frames := []protocol.Frame{
		&protocol.ProviderStartPrepare{Attempt: 7},
	}
	index := 0
	admission := newProviderStartAdmission(protocol.ProtocolVersionV2, func(context.Context) (protocol.Frame, error) {
		if index >= len(frames) {
			return nil, errors.New("no more frames")
		}
		frame := frames[index]
		index++
		return frame, nil
	}, func(protocol.Frame) error { return nil }, nil)

	if err := admission.PrepareProcessStart(context.Background(), 1); err == nil {
		t.Fatal("a prepare for another attempt was accepted")
	}
}

func TestProviderStartAdmissionFailsOnAnErrorFrame(t *testing.T) {
	frames := []protocol.Frame{
		&protocol.Error{Code: "credential_rotation_rejected", Message: "credential rotation rejected"},
	}
	index := 0
	admission := newProviderStartAdmission(protocol.ProtocolVersionV2, func(context.Context) (protocol.Frame, error) {
		if index >= len(frames) {
			return nil, errors.New("no more frames")
		}
		frame := frames[index]
		index++
		return frame, nil
	}, func(protocol.Frame) error { return nil }, nil)

	if err := admission.PrepareProcessStart(context.Background(), 1); err == nil {
		t.Fatal("an error frame was ignored")
	}
}
