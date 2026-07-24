package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func rotationCredential(t *testing.T, sessionID string, generation int64) *SessionCredential {
	t.Helper()
	credential, err := NewSessionCredential("rotation-bearer", SessionCredentialMetadata{
		SessionID:  sessionID,
		Lineage:    SessionCredentialLineage{Kind: "target_attach", AttachID: "attach_" + sessionID, JTI: "jti_" + sessionID},
		Generation: generation,
		ExpiresAt:  time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("NewSessionCredential() error = %v", err)
	}
	return credential
}

func rotationWorker(t *testing.T, sessionID string, receipts *testDurableReceiptGate) *SessionWorker {
	t.Helper()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       sessionID,
		Credential:      rotationCredential(t, sessionID, 1),
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	return worker
}

func TestCredentialRotationKeepsPendingCredentialNonAuthorizing(t *testing.T) {
	receipts := &testDurableReceiptGate{
		commandAck:    CommandRoutingReceipt{CommandID: "cmd_active", Status: CommandRoutingAccepted},
		ledgerReceipt: LedgerOperationReceipt{OperationID: "op_active", Version: 1, Status: LedgerOperationPending},
	}
	worker := rotationWorker(t, "ses_rotation_pending", receipts)
	pending := rotationCredential(t, worker.SessionID(), 2)
	if err := worker.PrepareCredentialRotation("rot_2", pending); err != nil {
		t.Fatalf("PrepareCredentialRotation() error = %v", err)
	}
	if receipt, err := worker.AcknowledgeCredentialPossession("rot_2", 1); err != nil || receipt.Generation != 2 || receipt.Status != CredentialRotationPending {
		t.Fatalf("AcknowledgeCredentialPossession() = %+v, %v", receipt, err)
	}
	active, err := worker.rotation.ActiveReceipt()
	if err != nil || active.Generation != 1 {
		t.Fatalf("active receipt = %+v, %v; pending credential became active", active, err)
	}
	var applied atomic.Int32
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_active", Type: "session.send"}, func(context.Context) error {
		applied.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("RouteCommand() error = %v", err)
	}
	if applied.Load() != 1 {
		t.Fatalf("applied = %d, want 1", applied.Load())
	}
}

func TestCredentialRotationFencesEpochGenerationAndSession(t *testing.T) {
	rotation, err := NewCredentialRotation("ses_rotation_fence", rotationCredential(t, "ses_rotation_fence", 1), 3)
	if err != nil {
		t.Fatalf("NewCredentialRotation() error = %v", err)
	}
	if err := rotation.Prepare("rot_stale", rotationCredential(t, "ses_rotation_fence", 1)); !errors.Is(err, ErrCredentialRotationStale) {
		t.Fatalf("same generation error = %v, want stale", err)
	}
	if err := rotation.Prepare("rot_2", rotationCredential(t, "ses_rotation_fence", 2)); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, err := rotation.PossessionAck("rot_2", 2); !errors.Is(err, ErrCredentialRotationStale) {
		t.Fatalf("wrong epoch ack error = %v, want stale", err)
	}
	if _, err := rotation.PossessionAck("rot_other", 3); !errors.Is(err, ErrCredentialRotationStale) {
		t.Fatalf("wrong rotation ack error = %v, want stale", err)
	}
	if err := rotation.Prepare("rot_other", rotationCredential(t, "ses_rotation_fence", 3)); !errors.Is(err, ErrCredentialRotationConflict) {
		t.Fatalf("duplicate pending rotation error = %v, want conflict", err)
	}
	if err := rotation.Prepare("rot_cross", rotationCredential(t, "ses_other", 3)); !errors.Is(err, ErrSessionCredentialMismatch) {
		t.Fatalf("cross-session prepare error = %v, want mismatch", err)
	}
	receipt, err := rotation.PossessionAck("rot_2", 3)
	if err != nil {
		t.Fatalf("current epoch ack error = %v", err)
	}
	if err := rotation.Activate(receipt); err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	active, err := rotation.RetryActivation("rot_2")
	if err != nil || active.Status != CredentialRotationActive || active.Generation != 2 {
		t.Fatalf("RetryActivation() = %+v, %v", active, err)
	}
	if err := rotation.Activate(active); err != nil {
		t.Fatalf("idempotent Activate() error = %v", err)
	}
	if err := rotation.Reconnect(3, 2); !errors.Is(err, ErrCredentialRotationStale) {
		t.Fatalf("same epoch reconnect error = %v, want stale", err)
	}
	if err := rotation.Reconnect(4, 1); !errors.Is(err, ErrCredentialRotationStale) {
		t.Fatalf("stale generation reconnect error = %v, want stale", err)
	}
	if err := rotation.Reconnect(4, 2); err != nil {
		t.Fatalf("current generation reconnect error = %v", err)
	}
}

func TestCredentialRotationAuthorityLossRevokeExpiryAndTerminalFailClosed(t *testing.T) {
	receipts := &testDurableReceiptGate{
		commandAck:    CommandRoutingReceipt{CommandID: "cmd_fence", Status: CommandRoutingAccepted},
		ledgerReceipt: LedgerOperationReceipt{OperationID: "op_fence", Version: 1, Status: LedgerOperationPending},
	}
	worker := rotationWorker(t, "ses_rotation_fence_states", receipts)
	apply := func(context.Context) error { return nil }
	worker.MarkCredentialAuthorityLost()
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_fence", Type: "session.send"}, apply); !errors.Is(err, ErrCredentialAuthorityLost) {
		t.Fatalf("authority loss route error = %v, want authority lost", err)
	}
	worker = rotationWorker(t, "ses_rotation_revoke", receipts)
	worker.RevokeCredential()
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_fence", Type: "session.send"}, apply); !errors.Is(err, ErrCredentialAuthorityLost) {
		t.Fatalf("revoke route error = %v, want authority lost", err)
	}
	worker = rotationWorker(t, "ses_rotation_terminal", receipts)
	worker.TerminalCredential()
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_fence", Type: "session.send"}, apply); !errors.Is(err, ErrCredentialTerminal) {
		t.Fatalf("terminal route error = %v, want terminal", err)
	}
	worker = rotationWorker(t, "ses_rotation_expired", receipts)
	worker.credential.metadata.ExpiresAt = time.Now().Add(-time.Second)
	if _, err := worker.RouteCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_fence", Type: "session.send"}, apply); !errors.Is(err, ErrSessionCredentialExpired) {
		t.Fatalf("expired route error = %v, want expired", err)
	}
}

func TestCredentialRotationPriorGenerationIsRecoveryOnlyAndWorkerLocal(t *testing.T) {
	first, err := NewCredentialRotation("ses_rotation_first", rotationCredential(t, "ses_rotation_first", 1), 1)
	if err != nil {
		t.Fatalf("first rotation error = %v", err)
	}
	if err := first.Prepare("rot_2", rotationCredential(t, "ses_rotation_first", 2)); err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	receipt, err := first.PossessionAck("rot_2", 1)
	if err != nil {
		t.Fatalf("first ack error = %v", err)
	}
	if err := first.Activate(receipt); err != nil {
		t.Fatalf("first Activate() error = %v", err)
	}
	permit, err := first.RecoveryPermit()
	if err != nil || permit.SessionID != "ses_rotation_first" || permit.Generation != 1 {
		t.Fatalf("recovery permit = %+v, %v", permit, err)
	}
	active, err := first.ActiveReceipt()
	if err != nil || active.Generation != 2 {
		t.Fatalf("active receipt = %+v, %v", active, err)
	}
	second, err := NewCredentialRotation("ses_rotation_second", rotationCredential(t, "ses_rotation_second", 1), 1)
	if err != nil {
		t.Fatalf("second rotation error = %v", err)
	}
	second.Terminal()
	if _, err := first.ActiveReceipt(); err != nil {
		t.Fatalf("terminal second Worker affected first Worker: %v", err)
	}
}

func TestCredentialRotationRejectsAuthorityLostRecoveryAndForgedIdempotentReceipt(t *testing.T) {
	rotation, err := NewCredentialRotation("ses_rotation_receipts", rotationCredential(t, "ses_rotation_receipts", 1), 1)
	if err != nil {
		t.Fatalf("NewCredentialRotation() error = %v", err)
	}
	if err := rotation.Prepare("rot_2", rotationCredential(t, "ses_rotation_receipts", 2)); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	receipt, err := rotation.PossessionAck("rot_2", 1)
	if err != nil {
		t.Fatalf("PossessionAck() error = %v", err)
	}
	if err := rotation.Activate(receipt); err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	active, err := rotation.RetryActivation("rot_2")
	if err != nil {
		t.Fatalf("RetryActivation() error = %v", err)
	}
	forged := active
	forged.SessionID = "ses_other"
	if err := rotation.Activate(forged); !errors.Is(err, ErrCredentialRotationStale) {
		t.Fatalf("forged idempotent receipt error = %v, want stale", err)
	}
	rotation.MarkAuthorityLost()
	if _, err := rotation.RetryActivation("rot_2"); !errors.Is(err, ErrCredentialAuthorityLost) {
		t.Fatalf("authority-lost retry error = %v, want authority lost", err)
	}
	if _, err := rotation.RecoveryPermit(); !errors.Is(err, ErrCredentialAuthorityLost) {
		t.Fatalf("authority-lost recovery permit error = %v, want authority lost", err)
	}
}

type authorityLossAfterPrepareGate struct {
	worker    *SessionWorker
	finalized []CommandOutcome
}

func (g *authorityLossAfterPrepareGate) PrepareProviderStart(context.Context, string, int) error {
	return nil
}

func (g *authorityLossAfterPrepareGate) ConfirmProviderStarted(context.Context, string, int) error {
	return nil
}

func (g *authorityLossAfterPrepareGate) PrepareCommand(context.Context, string, SessionWorkerCommand) (CommandRoutingReceipt, LedgerOperationReceipt, error) {
	g.worker.MarkCredentialAuthorityLost()
	return CommandRoutingReceipt{CommandID: "cmd_authority_lost", Status: CommandRoutingAccepted}, LedgerOperationReceipt{
		OperationID: "op_authority_lost", Version: 1, Status: LedgerOperationPending,
	}, nil
}

func (g *authorityLossAfterPrepareGate) FinalizeCommand(_ context.Context, _ string, _ SessionWorkerCommand, _ LedgerOperationReceipt, outcome CommandOutcome) error {
	g.finalized = append(g.finalized, outcome)
	return nil
}

func (g *authorityLossAfterPrepareGate) CommitEventProposal(context.Context, string, string) (EventProposalReceipt, error) {
	return EventProposalReceipt{}, nil
}

func TestSessionWorkerFinalizesPendingCommandWhenAuthorityIsLostAfterPrepare(t *testing.T) {
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:  "ses_authority_lost_after_prepare",
		Credential: rotationCredential(t, "ses_authority_lost_after_prepare", 1),
		Provider:   ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	gate := &authorityLossAfterPrepareGate{worker: worker}
	worker.receipts = gate
	var applied atomic.Int32
	_, err = worker.DeliverCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_authority_lost", Type: "session.send"}, func(context.Context) error {
		applied.Add(1)
		return nil
	})
	if !errors.Is(err, ErrUnsafeCommandReplay) || applied.Load() != 0 {
		t.Fatalf("DeliverCommand() = %v, applied=%d; want outcome_unknown without side effect", err, applied.Load())
	}
	if len(gate.finalized) != 1 || gate.finalized[0] != CommandOutcomeOutcomeUnknown {
		t.Fatalf("finalized outcomes = %+v, want one outcome_unknown", gate.finalized)
	}
}
