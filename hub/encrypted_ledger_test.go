package hub

import (
	"context"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"testing"
)

type opaqueLedgerProbe struct {
	store.EncryptedCommandLedgerStore
	calls []string
}

func (p *opaqueLedgerProbe) CommitEncryptedPendingCommand(context.Context, string, store.CommandAuthority, store.PendingEvent, store.PendingCommandRequest) (store.PendingCommandCommit, error) {
	p.calls = append(p.calls, "commit")
	return store.PendingCommandCommit{}, nil
}

func (p *opaqueLedgerProbe) ListEncryptedPendingCommands(context.Context, string, store.CommandAuthority) ([]store.PendingCommand, error) {
	p.calls = append(p.calls, "list")
	return nil, nil
}
func (p *opaqueLedgerProbe) ClaimEncryptedPendingCommand(context.Context, string, store.CommandAuthority, string) (store.PendingCommandClaim, error) {
	p.calls = append(p.calls, "claim")
	return store.PendingCommandClaim{}, nil
}
func (p *opaqueLedgerProbe) ResolveEncryptedPendingCommand(context.Context, string, store.CommandAuthority, string, store.PendingCommandStatus) (store.PendingCommand, error) {
	p.calls = append(p.calls, "resolve")
	return store.PendingCommand{}, nil
}
func (p *opaqueLedgerProbe) ResolveEncryptedPendingCommandUnknown(context.Context, string, string) (store.PendingCommand, error) {
	p.calls = append(p.calls, "unknown")
	return store.PendingCommand{}, nil
}
func TestDurableSendRejectsMixedContentModesBeforePersistence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, encrypted := range []bool{false, true} {
		adapterMode := protocol.ContentModeRequired
		if encrypted {
			adapterMode = "legacy"
		}
		probe := &opaqueLedgerProbe{}
		handler := &webSocketHandler{adapters: map[string]*adapterConnection{"session": {sessionID: "session", contentMode: adapterMode}}}
		err := handler.handleDurableSessionSendMode(ctx, nil, &clientConnection{}, &protocol.Command{SessionID: "session", CommandID: "command", Type: protocol.CommandSessionSend}, probe, encrypted)
		if err == nil || err.Error() != "client and adapter content modes differ" {
			t.Fatalf("mixed mode result: %v", err)
		}
		if len(probe.calls) != 0 {
			t.Fatal("mixed-mode command reached store")
		}
	}
}

func TestRequiredCompletedDuplicateAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		status protocol.AckStatus
		reason string
		want   bool
	}{
		{protocol.ContentModeRequired, protocol.AckAccepted, "", true},
		{protocol.ContentModeRequired, protocol.AckDuplicate, "", true},
		{protocol.ContentModeRequired, protocol.AckRejected, "", false},
		{protocol.ContentModeRequired, protocol.AckDuplicate, "outcome_unknown", false},
		{"legacy", protocol.AckDuplicate, "", false},
		{"legacy", protocol.AckAccepted, "", true},
	} {
		if got := pendingCommandAckCompleted(tc.mode, &protocol.CommandAck{Status: tc.status, Reason: tc.reason}); got != tc.want {
			t.Fatalf("ack mode=%s status=%s: %v", tc.mode, tc.status, got)
		}
	}
	if pendingCommandAckCompleted(protocol.ContentModeRequired, nil) {
		t.Fatal("nil acknowledgement completed command")
	}
}

func TestRequiredTerminalRejectionReasonAllowlist(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		kind   protocol.CommandType
		status protocol.AckStatus
		reason string
		want   bool
	}{
		{protocol.ContentModeRequired, protocol.CommandFileRead, protocol.AckRejected, "invalid_file_request", true},
		{protocol.ContentModeRequired, protocol.CommandFileList, protocol.AckRejected, "file_unavailable", true},
		{protocol.ContentModeRequired, protocol.CommandFileRead, protocol.AckRejected, "private_adapter_detail", false},
		{protocol.ContentModeRequired, protocol.CommandSessionSend, protocol.AckRejected, "invalid_file_request", false},
		{protocol.ContentModeRequired, protocol.CommandFileRead, protocol.AckAccepted, "invalid_file_request", false},
		{"legacy", protocol.CommandFileRead, protocol.AckRejected, "invalid_file_request", false},
	} {
		got, ok := pendingCommandAckTerminalRejection(tc.mode, tc.kind, &protocol.CommandAck{Status: tc.status, Reason: tc.reason})
		if ok != tc.want || (ok && got != tc.reason) || (!ok && got != "") {
			t.Fatalf("ack mode=%s status=%s reason=%q: reason=%q allowed=%v", tc.mode, tc.status, tc.reason, got, ok)
		}
	}
	if reason, ok := pendingCommandAckTerminalRejection(protocol.ContentModeRequired, protocol.CommandFileRead, nil); ok || reason != "" {
		t.Fatal("nil acknowledgement exposed a rejection reason")
	}
}

func TestUnknownOutcomeRetainsRequiredModeAfterDisconnect(t *testing.T) {
	probe := &opaqueLedgerProbe{}
	handler := &webSocketHandler{events: probe}
	// No Adapter registration remains. Mode comes from the pending delivery.
	handler.resolvePendingCommandUnknown("session", "command", protocol.ContentModeRequired)
	if len(probe.calls) != 1 || probe.calls[0] != "unknown" {
		t.Fatal("unknown resolution used legacy ledger")
	}
}

func TestEncryptedLedgerLifecycleCannotUseLegacyMethods(t *testing.T) {
	probe := &opaqueLedgerProbe{}
	var ledger store.CommandLedgerStore = encryptedCommandLedger{probe}
	ctx := context.Background()
	_, _ = ledger.CommitPendingCommand(ctx, "session", store.CommandAuthority{}, store.PendingEvent{}, store.PendingCommandRequest{})
	_, _ = ledger.ListPendingCommands(ctx, "session", store.CommandAuthority{})
	_, _ = ledger.ClaimPendingCommand(ctx, "session", store.CommandAuthority{}, "command")
	_, _ = ledger.ResolvePendingCommand(ctx, "session", store.CommandAuthority{}, "command", store.PendingCommandCompleted)
	_, _ = ledger.ResolvePendingCommandUnknown(ctx, "session", "command")
	if len(probe.calls) != 5 {
		t.Fatal("missing encrypted lifecycle call")
	}
	for i, want := range []string{"commit", "list", "claim", "resolve", "unknown"} {
		if probe.calls[i] != want {
			t.Fatal("incorrect encrypted lifecycle")
		}
	}
}
