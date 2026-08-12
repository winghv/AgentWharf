package core

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSessionWorkerWaitsForDurableStartReceiptBeforeProviderSpawn(t *testing.T) {
	receipts := &testDurableReceiptGate{prepareStartErr: errors.New("start receipt unavailable")}
	runner := newFakeProcessRunner()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_receipt_start",
		DurableReceipts: receipts,
		Provider: ProcessConfig{
			Command: ProcessCommand{Path: "provider"},
		},
	}, runner)
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	if err := worker.Run(context.Background()); !errors.Is(err, receipts.prepareStartErr) {
		t.Fatalf("Run() error = %v, want start receipt error", err)
	}
	if got := runner.startCount(); got != 0 {
		t.Fatalf("provider starts = %d, want zero before durable start receipt", got)
	}
}

func TestSessionWorkerDurableStartReceiptOrdersEachProviderAttempt(t *testing.T) {
	receipts := &testDurableReceiptGate{}
	runner := newFakeProcessRunner()
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_receipt_start_order",
		DurableReceipts: receipts,
		Provider: ProcessConfig{
			Command:     ProcessCommand{Path: "provider"},
			GracePeriod: time.Millisecond,
		},
	}, runner)
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(context.Background()) }()
	_ = waitEvent(t, worker.Events(), ProcessEventStarted)
	if got := runner.startCount(); got != 1 {
		t.Fatalf("provider starts = %d, want one", got)
	}
	if got := receipts.calls(); !reflect.DeepEqual(got, []string{"start:prepare", "start:confirmed"}) {
		t.Fatalf("start receipt order = %v", got)
	}
	if err := worker.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("Run() after Stop() error = %v", err)
	}
}

func TestSessionWorkerCommandUsesDistinctRoutingAndLedgerReceipts(t *testing.T) {
	receipts := &testDurableReceiptGate{
		commandAck: CommandRoutingReceipt{CommandID: "cmd_receipt_1", Status: CommandRoutingAccepted},
		ledgerReceipt: LedgerOperationReceipt{
			OperationID: "op_receipt_1",
			Version:     7,
			Status:      LedgerOperationPending,
		},
	}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_receipt_command",
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}

	var applied bool
	ack, err := worker.DeliverCommand(context.Background(), SessionWorkerCommand{
		CommandID: "cmd_receipt_1",
		Type:      "session.send",
	}, func(context.Context) error {
		applied = true
		receipts.mu.Lock()
		receipts.seen = append(receipts.seen, "apply")
		receipts.mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("DeliverCommand() error = %v", err)
	}
	if !applied || ack.Status != CommandRoutingAccepted {
		t.Fatalf("command result = applied:%v ack:%+v", applied, ack)
	}
	wantCalls := []string{"command", "apply", "finalize:completed"}
	if got := receipts.calls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("receipt call order = %v, want %v", got, wantCalls)
	}
}

func TestSessionWorkerAcceptsSettingsChangeThroughTheDurableCommandGate(t *testing.T) {
	receipts := &testDurableReceiptGate{
		commandAck: CommandRoutingReceipt{CommandID: "cmd_settings", Status: CommandRoutingAccepted},
		ledgerReceipt: LedgerOperationReceipt{
			OperationID: "op_settings",
			Version:     1,
			Status:      LedgerOperationPending,
		},
	}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_settings",
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	var applied bool
	ack, err := worker.DeliverCommand(context.Background(), SessionWorkerCommand{
		CommandID: "cmd_settings",
		Type:      "session.settings.change",
	}, func(context.Context) error {
		applied = true
		return nil
	})
	if err != nil {
		t.Fatalf("DeliverCommand(settings) error = %v", err)
	}
	if !applied || ack.Status != CommandRoutingAccepted {
		t.Fatalf("settings command result = applied:%v ack:%+v", applied, ack)
	}
}

func TestSessionWorkerRejectsUnknownCommandAndStaleReceiptWithoutSideEffect(t *testing.T) {
	receipts := &testDurableReceiptGate{
		commandAck:    CommandRoutingReceipt{CommandID: "other", Status: CommandRoutingAccepted},
		ledgerReceipt: LedgerOperationReceipt{OperationID: "op_receipt_2", Version: 1, Status: LedgerOperationPending},
	}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_receipt_reject",
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}

	var applied bool
	if _, err := worker.DeliverCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_unknown", Type: "session.unknown"}, func(context.Context) error {
		applied = true
		return nil
	}); !errors.Is(err, ErrUnsupportedSessionWorkerCommand) {
		t.Fatalf("unknown command error = %v, want ErrUnsupportedSessionWorkerCommand", err)
	}
	if _, err := worker.DeliverCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_stale", Type: "session.send"}, func(context.Context) error {
		applied = true
		return nil
	}); !errors.Is(err, ErrInvalidDurableReceipt) {
		t.Fatalf("stale receipt error = %v, want ErrInvalidDurableReceipt", err)
	}
	if applied {
		t.Fatal("unsafe command side effect ran after unknown or stale receipt")
	}
}

func TestSessionWorkerProposalRetryReturnsOriginalSequence(t *testing.T) {
	receipts := &testDurableReceiptGate{
		proposalReceipt: EventProposalReceipt{ProposalID: "proposal_receipt_1", Seq: 41, Status: EventProposalAccepted},
	}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_receipt_proposal",
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}

	var published []int64
	publish := func(seq int64) error {
		published = append(published, seq)
		return nil
	}
	first, err := worker.ProposeEvent(context.Background(), "proposal_receipt_1", publish)
	if err != nil {
		t.Fatalf("first ProposeEvent() error = %v", err)
	}
	second, err := worker.ProposeEvent(context.Background(), "proposal_receipt_1", publish)
	if err != nil {
		t.Fatalf("retry ProposeEvent() error = %v", err)
	}
	if first.Seq != 41 || second.Seq != first.Seq || !reflect.DeepEqual(published, []int64{41, 41}) {
		t.Fatalf("proposal retry = first:%+v second:%+v published:%v", first, second, published)
	}
}

func TestSessionWorkerMarksAmbiguousCommandUnknownAndNeverReplays(t *testing.T) {
	receipts := &testDurableReceiptGate{
		commandAck: CommandRoutingReceipt{CommandID: "cmd_ambiguous", Status: CommandRoutingAccepted},
		ledgerReceipt: LedgerOperationReceipt{
			OperationID: "op_ambiguous",
			Version:     3,
			Status:      LedgerOperationPending,
		},
	}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_receipt_ambiguous",
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	var applied int
	_, err = worker.DeliverCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_ambiguous", Type: "session.send"}, func(context.Context) error {
		applied++
		return errors.New("provider write outcome unknown")
	})
	if !errors.Is(err, ErrUnsafeCommandReplay) || applied != 1 {
		t.Fatalf("ambiguous command error = %v, applied = %d", err, applied)
	}
	receipts.commandAck = CommandRoutingReceipt{CommandID: "cmd_ambiguous", Status: CommandRoutingDuplicate}
	if _, err := worker.DeliverCommand(context.Background(), SessionWorkerCommand{CommandID: "cmd_ambiguous", Type: "session.send"}, func(context.Context) error {
		applied++
		return nil
	}); err != nil {
		t.Fatalf("duplicate command retry error = %v", err)
	}
	if applied != 1 {
		t.Fatalf("duplicate command replayed provider side effect: applied = %d", applied)
	}
	if got := receipts.calls(); !reflect.DeepEqual(got, []string{"command", "finalize:outcome_unknown", "command"}) {
		t.Fatalf("ambiguous receipt calls = %v", got)
	}
}

func TestSessionWorkerRejectsMismatchedProposalReceiptBeforePublish(t *testing.T) {
	receipts := &testDurableReceiptGate{
		proposalReceipt: EventProposalReceipt{ProposalID: "different", Seq: 9, Status: EventProposalAccepted},
	}
	worker, err := newSessionWorker(SessionWorkerConfig{
		SessionID:       "ses_receipt_stale_proposal",
		DurableReceipts: receipts,
		Provider:        ProcessConfig{Command: ProcessCommand{Path: "provider"}},
	}, newFakeProcessRunner())
	if err != nil {
		t.Fatalf("newSessionWorker() error = %v", err)
	}
	published := 0
	if _, err := worker.ProposeEvent(context.Background(), "proposal_expected", func(int64) error {
		published++
		return nil
	}); !errors.Is(err, ErrInvalidDurableReceipt) {
		t.Fatalf("mismatched proposal error = %v, want ErrInvalidDurableReceipt", err)
	}
	if published != 0 {
		t.Fatalf("mismatched proposal was published: %d", published)
	}
}

type testDurableReceiptGate struct {
	prepareStartErr error
	commandAck      CommandRoutingReceipt
	ledgerReceipt   LedgerOperationReceipt
	proposalReceipt EventProposalReceipt

	mu   sync.Mutex
	seen []string
}

func (g *testDurableReceiptGate) PrepareProviderStart(context.Context, string, int) error {
	g.mu.Lock()
	g.seen = append(g.seen, "start:prepare")
	g.mu.Unlock()
	if g.prepareStartErr != nil {
		return g.prepareStartErr
	}
	return nil
}

func (g *testDurableReceiptGate) ConfirmProviderStarted(context.Context, string, int) error {
	g.mu.Lock()
	g.seen = append(g.seen, "start:confirmed")
	g.mu.Unlock()
	return nil
}

func (g *testDurableReceiptGate) PrepareCommand(context.Context, string, SessionWorkerCommand) (CommandRoutingReceipt, LedgerOperationReceipt, error) {
	g.mu.Lock()
	g.seen = append(g.seen, "command")
	g.mu.Unlock()
	return g.commandAck, g.ledgerReceipt, nil
}

func (g *testDurableReceiptGate) FinalizeCommand(_ context.Context, _ string, _ SessionWorkerCommand, _ LedgerOperationReceipt, outcome CommandOutcome) error {
	g.mu.Lock()
	g.seen = append(g.seen, "finalize:"+string(outcome))
	g.mu.Unlock()
	return nil
}

func (g *testDurableReceiptGate) CommitEventProposal(context.Context, string, string) (EventProposalReceipt, error) {
	return g.proposalReceipt, nil
}

func (g *testDurableReceiptGate) calls() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.seen...)
}
