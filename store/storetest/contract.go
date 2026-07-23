package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

type Harness func(t *testing.T) store.EventStore

type HistoryHarness struct {
	Open        func(t *testing.T) store.HistoryStore
	Reopen      func(t *testing.T, current store.HistoryStore) store.HistoryStore
	PruneBefore func(t *testing.T, current store.HistoryStore, sessionID string, beforeSeq int64)
}

type PendingCommandHarness struct {
	Open       func(t *testing.T) store.CommandLedgerStore
	Reopen     func(t *testing.T, current store.CommandLedgerStore) store.CommandLedgerStore
	Authority  func(t *testing.T, ledger store.CommandLedgerStore) store.CommandAuthority
	Invalidate func(t *testing.T, ledger store.CommandLedgerStore, kind CommandAuthorityFailure)
}

type SettingsCommandHarness struct {
	Open                    func(t *testing.T) store.SettingsCommandStore
	Reopen                  func(t *testing.T, current store.SettingsCommandStore) store.SettingsCommandStore
	ExpireOperationDeadline func(t *testing.T, ledger store.SettingsCommandStore, sessionID, commandID string)
	RevokeWriter            func(t *testing.T, ledger store.SettingsCommandStore, sessionID string)
}

// SettingsCommandContract fixes the durable reservation state machine shared by
// SQLite and PostgreSQL. Callers use only opaque IDs, fingerprints and writer
// routing metadata; the contract deliberately has no Provider, credential or
// content fixtures.
func SettingsCommandContract(t *testing.T, harness SettingsCommandHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.ExpireOperationDeadline == nil || harness.RevokeWriter == nil {
		t.Fatal("settings-command harness is incomplete")
	}
	ledgerForRunControl := harness.Open(t)
	runControl, ok := ledgerForRunControl.(store.RunControlStore)
	if !ok {
		t.Fatal("settings-command backend does not implement the run-control ledger")
	}
	RunControlContract(t, runControl, harness)
	ctx := context.Background()
	capability := store.SettingsCapabilityUpdate{
		Fingerprint:      "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EffectiveModelID: "reasoning", EffectivePermissionModeID: "ask",
	}
	request := store.SettingsCommandRequest{
		CommandID: "cmd_settings_1", RequestFingerprint: capability.Fingerprint,
		RequestedModelID: settingsString("reasoning"),
	}
	ledger := harness.Open(t)
	writer := bindSettingsWriter(t, ledger, "ses_settings_1", "lease_current")
	capability.Writer = writer
	request.Writer = writer
	capability.EventSeq = appendSettingsCapabilityEvent(t, ledger, "ses_settings_1")
	published, err := ledger.PublishSettingsCapability(ctx, "ses_settings_1", capability)
	if err != nil || published.Version != 1 || published.Writer != writer {
		t.Fatalf("PublishSettingsCapability() = %+v, %v", published, err)
	}
	reserved, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", request)
	if err != nil || reserved.Duplicate || reserved.Command.Status != store.SettingsCommandDeliveryPending || reserved.Command.ReservationVersion != 1 || !reserved.Command.DeliveryDeadline.After(time.Now()) {
		t.Fatalf("SettingsCommandReserve() = %+v, %v", reserved, err)
	}
	retry, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", request)
	if err != nil || !retry.Duplicate || !reflect.DeepEqual(retry.Command, reserved.Command) {
		t.Fatalf("exact SettingsCommandReserve retry = %+v, %v; want %+v", retry, err, reserved.Command)
	}
	changed := request
	changed.RequestedModelID = settingsString("balanced")
	if _, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", changed); err == nil {
		t.Fatal("changed cmd_id retry unexpectedly succeeded")
	}
	second := request
	second.CommandID = "cmd_settings_2"
	if _, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", second); err == nil {
		t.Fatal("second nonterminal Session reservation unexpectedly succeeded")
	}
	secondCapability := capability
	secondWriter := bindSettingsWriter(t, ledger, "ses_settings_2", "lease_second")
	secondCapability.Writer = secondWriter
	secondCapability.EventSeq = appendSettingsCapabilityEvent(t, ledger, "ses_settings_2")
	if _, err := ledger.PublishSettingsCapability(ctx, "ses_settings_2", secondCapability); err != nil {
		t.Fatalf("publish second Session capability: %v", err)
	}
	secondRequest := request
	secondRequest.Writer = secondWriter
	if _, err := ledger.SettingsCommandReserve(ctx, "ses_settings_2", secondRequest); err != nil {
		t.Fatalf("cross-Session reservation unexpectedly blocked: %v", err)
	}
	stale := writer
	stale.LeaseID = "lease_stale"
	if _, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, "ses_settings_1", request.CommandID, reserved.Command.ReservationVersion, stale); err == nil {
		t.Fatal("stale writer delivery acknowledgement unexpectedly succeeded")
	}
	acknowledged, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, "ses_settings_1", request.CommandID, reserved.Command.ReservationVersion, writer)
	if err != nil || acknowledged.Status != store.SettingsCommandPending || acknowledged.OperationDeadline == nil {
		t.Fatalf("AcknowledgeSettingsCommandDelivery() = %+v, %v", acknowledged, err)
	}
	ledger = harness.Reopen(t, ledger)
	pending, err := ledger.PendingSettingsCommands(ctx, "ses_settings_1")
	if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], acknowledged) {
		t.Fatalf("restart pending settings recovery = %+v, %v; want %+v", pending, err, acknowledged)
	}
	changedCapability := capability
	changedCapability.EventSeq = appendSettingsCapabilityEvent(t, ledger, "ses_settings_1")
	changedCapability.EffectivePermissionModeID = "workspace"
	changedPublished, err := ledger.PublishSettingsCapability(ctx, "ses_settings_1", changedCapability)
	if err != nil {
		t.Fatalf("publish unrequested control mutation: %v", err)
	}
	if _, err := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", request.CommandID, store.SettingsCommandFinalize{
		ReservationVersion: acknowledged.ReservationVersion, ExpectedStatus: store.SettingsCommandPending,
		Writer: &writer, Outcome: store.SettingsCommandApplied, EffectiveCapability: changedPublished,
	}); err == nil {
		t.Fatal("applied finalization accepted an unrequested control mutation")
	}
	capability.EventSeq = appendSettingsCapabilityEvent(t, ledger, "ses_settings_1")
	published, err = ledger.PublishSettingsCapability(ctx, "ses_settings_1", capability)
	if err != nil || published.Writer != writer {
		t.Fatalf("restore reserved capability: %+v, %v", published, err)
	}
	finalized, err := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", request.CommandID, store.SettingsCommandFinalize{
		ReservationVersion: acknowledged.ReservationVersion, ExpectedStatus: store.SettingsCommandPending,
		Writer: &writer, Outcome: store.SettingsCommandApplied, EffectiveCapability: published,
	})
	if err != nil || finalized.Status != store.SettingsCommandApplied || finalized.TerminalEventSeq == nil || *finalized.TerminalEventSeq < 1 {
		t.Fatalf("FinalizeSettingsCommand() = %+v, %v", finalized, err)
	}
	if _, err := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", request.CommandID, store.SettingsCommandFinalize{
		ReservationVersion: acknowledged.ReservationVersion, ExpectedStatus: store.SettingsCommandPending,
		Writer: &writer, Outcome: store.SettingsCommandApplied, EffectiveCapability: published,
	}); err == nil {
		t.Fatal("second terminal settings finalize unexpectedly succeeded")
	}
	if commands, err := ledger.PendingSettingsCommands(ctx, "ses_settings_1"); err != nil || len(commands) != 0 {
		t.Fatalf("terminal reservation remained pending: %+v, %v", commands, err)
	}
	timeoutRequest := request
	timeoutRequest.CommandID = "cmd_settings_operation_deadline"
	timeout, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", timeoutRequest)
	if err != nil || timeout.Duplicate {
		t.Fatalf("operation-deadline reservation = %+v, %v", timeout, err)
	}
	timeoutPending, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, "ses_settings_1", timeoutRequest.CommandID, timeout.Command.ReservationVersion, writer)
	if err != nil || timeoutPending.OperationDeadline == nil {
		t.Fatalf("operation-deadline acknowledgement = %+v, %v", timeoutPending, err)
	}
	timeoutReason := "operation_timeout"
	timeoutFinalize := store.SettingsCommandFinalize{ReservationVersion: timeoutPending.ReservationVersion, ExpectedStatus: store.SettingsCommandPending, Outcome: store.SettingsCommandTimeout, ReasonCode: &timeoutReason, EffectiveCapability: published}
	if _, err := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", timeoutRequest.CommandID, timeoutFinalize); err == nil {
		t.Fatal("pre-deadline timeout unexpectedly finalized")
	}
	if current, err := ledger.SettingsCommand(ctx, "ses_settings_1", timeoutRequest.CommandID); err != nil || current.Status != store.SettingsCommandPending || current.TerminalEventSeq != nil {
		t.Fatalf("pre-deadline timeout mutated command = %+v, %v", current, err)
	}
	harness.ExpireOperationDeadline(t, ledger, "ses_settings_1", timeoutRequest.CommandID)
	timedOut, err := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", timeoutRequest.CommandID, timeoutFinalize)
	if err != nil || timedOut.Status != store.SettingsCommandTimeout || timedOut.TerminalEventSeq == nil {
		t.Fatalf("elapsed operation timeout = %+v, %v", timedOut, err)
	}
	recoveryRequest := request
	recoveryRequest.CommandID = "cmd_settings_recovery"
	recovery, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", recoveryRequest)
	if err != nil || recovery.Duplicate {
		t.Fatalf("recovery SettingsCommandReserve() = %+v, %v", recovery, err)
	}
	if _, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, "ses_settings_1", recoveryRequest.CommandID, recovery.Command.ReservationVersion, writer); err != nil {
		t.Fatalf("acknowledge recovery command: %v", err)
	}
	newWriter := replaceSettingsWriter(t, ledger, "ses_settings_1", "lease_recovered")
	refreshedUpdate := capability
	refreshedUpdate.Fingerprint = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	refreshedUpdate.Writer = newWriter
	refreshedUpdate.EventSeq = appendSettingsCapabilityEvent(t, ledger, "ses_settings_1")
	refreshed, err := ledger.PublishSettingsCapability(ctx, "ses_settings_1", refreshedUpdate)
	if err != nil || refreshed.Version != 4 || refreshed.Writer != newWriter {
		t.Fatalf("refresh settings capability = %+v, %v", refreshed, err)
	}
	assertRevokedSettingsWriter(t, ledger, harness, refreshed)
	stalePublish := refreshedUpdate
	stalePublish.EventSeq = appendSettingsCapabilityEvent(t, ledger, "ses_settings_1")
	stalePublish.Writer = writer
	if _, err := ledger.PublishSettingsCapability(ctx, "ses_settings_1", stalePublish); err == nil {
		t.Fatal("replaced writer republished a Settings capability")
	}
	staleReserve := request
	staleReserve.CommandID = "cmd_settings_stale_writer"
	if _, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", staleReserve); err == nil {
		t.Fatal("replaced writer reserved a Settings command")
	}
	if _, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, "ses_settings_1", recoveryRequest.CommandID, recovery.Command.ReservationVersion, writer); err == nil {
		t.Fatal("replaced writer acknowledged a Settings command")
	}
	fenced, err := ledger.RecoverSettingsCommand(ctx, "ses_settings_1", recoveryRequest.CommandID, writer)
	if err != nil || fenced.Status != store.SettingsCommandRecoveryPending {
		t.Fatalf("RecoverSettingsCommand() = %+v, %v", fenced, err)
	}
	if _, err := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", recoveryRequest.CommandID, store.SettingsCommandFinalize{
		ReservationVersion: fenced.ReservationVersion, ExpectedStatus: store.SettingsCommandRecoveryPending,
		Writer: &writer, Outcome: store.SettingsCommandApplied, EffectiveCapability: published,
	}); err == nil {
		t.Fatal("recovered command accepted fenced old writer")
	}
	retryAfterWriterRefresh := request
	retryAfterWriterRefresh.Writer = newWriter
	if retry, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", retryAfterWriterRefresh); err != nil || !retry.Duplicate {
		t.Fatalf("retry after writer refresh = %+v, %v; want duplicate", retry, err)
	}
	recoveryReason := "recovery_unconfirmed"
	unknown, err := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", recoveryRequest.CommandID, store.SettingsCommandFinalize{
		ReservationVersion: fenced.ReservationVersion, ExpectedStatus: store.SettingsCommandRecoveryPending,
		Outcome: store.SettingsCommandOutcomeUnknown, ReasonCode: &recoveryReason, EffectiveCapability: refreshed,
	})
	if err != nil || unknown.Status != store.SettingsCommandOutcomeUnknown || unknown.TerminalEventSeq == nil {
		t.Fatalf("recovery finalization = %+v, %v", unknown, err)
	}
	raceRequest := request
	raceRequest.CommandID = "cmd_settings_race"
	raceRequest.RequestFingerprint = refreshed.Fingerprint
	raceRequest.Writer = newWriter
	race, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", raceRequest)
	if err != nil || race.Duplicate {
		t.Fatalf("race SettingsCommandReserve() = %+v, %v", race, err)
	}
	racePending, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, "ses_settings_1", raceRequest.CommandID, race.Command.ReservationVersion, newWriter)
	if err != nil {
		t.Fatalf("acknowledge race command: %v", err)
	}
	finalizeResult := make(chan error, 2)
	for range 2 {
		go func() {
			_, finalizeErr := ledger.FinalizeSettingsCommand(ctx, "ses_settings_1", raceRequest.CommandID, store.SettingsCommandFinalize{
				ReservationVersion: racePending.ReservationVersion, ExpectedStatus: store.SettingsCommandPending,
				Writer: &newWriter, Outcome: store.SettingsCommandApplied, EffectiveCapability: refreshed,
			})
			finalizeResult <- finalizeErr
		}()
	}
	successes := 0
	for range 2 {
		if err := <-finalizeResult; err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent settings finalization successes = %d, want 1", successes)
	}
	deliveryDeadline := raceRequest
	deliveryDeadline.CommandID = "cmd_settings_delivery_deadline"
	delivery, err := ledger.SettingsCommandReserve(ctx, "ses_settings_1", deliveryDeadline)
	if err != nil || delivery.Duplicate {
		t.Fatalf("delivery-deadline reservation = %+v, %v", delivery, err)
	}
	time.Sleep(5100 * time.Millisecond)
	expired, err := ledger.RecoverSettingsCommand(ctx, "ses_settings_1", deliveryDeadline.CommandID, newWriter)
	if err != nil || expired.Status != store.SettingsCommandRejected || expired.TerminalEventSeq == nil {
		t.Fatalf("delivery deadline recovery = %+v, %v", expired, err)
	}
	terminalEvents := 0
	if err := ledger.Replay(ctx, "ses_settings_1", 0, func(event store.Event) error {
		if event.Type == "session.settings.effective" {
			terminalEvents++
		}
		return nil
	}); err != nil || terminalEvents != 5 {
		t.Fatalf("settings terminal events = %d, %v; want exactly five", terminalEvents, err)
	}
}

// RunControlContract fixes the v2 durable run-control state machine. It uses
// the existing backend harness solely to reopen the same durable Store; no
// Settings behavior is asserted here.
func RunControlContract(t *testing.T, ledger store.RunControlStore, harness SettingsCommandHarness) {
	t.Helper()
	ctx := context.Background()
	writer := bindRunControlWriter(t, ledger, "ses_run_control_1", "lease_run_control_1")
	stateSeq := appendRunControlEvent(t, ledger, "ses_run_control_1", "session.state", `{"state":"busy"}`)
	capabilitySeq := appendRunControlEvent(t, ledger, "ses_run_control_1", "session.run.capabilities", `{}`)
	capability, err := ledger.PublishRunControlCapability(ctx, "ses_run_control_1", store.RunControlCapabilityUpdate{
		EventSeq: capabilitySeq, InterruptSupported: true, StopSupported: true, Writer: writer,
	})
	if err != nil || capability.Version != capabilitySeq || capability.Writer != writer {
		t.Fatalf("PublishRunControlCapability() = %+v, %v", capability, err)
	}
	request := store.RunControlRequest{CommandID: "cmd_interrupt_1", Operation: store.RunControlInterrupt, PreControlState: "busy", PreControlStateSeq: stateSeq, Writer: writer}
	reserved, err := ledger.RunControlReserve(ctx, "ses_run_control_1", request)
	if err != nil || reserved.Duplicate || reserved.Reservation.Outcome != store.RunControlPending || reserved.Reservation.CapabilityVersion != capability.Version || reserved.Reservation.ReservationVersion != 1 || !reserved.Reservation.Deadline.After(time.Now()) {
		t.Fatalf("RunControlReserve() = %+v, %v", reserved, err)
	}
	if retry, err := ledger.RunControlReserve(ctx, "ses_run_control_1", request); err != nil || !retry.Duplicate || !reflect.DeepEqual(retry.Reservation, reserved.Reservation) {
		t.Fatalf("exact RunControlReserve retry = %+v, %v", retry, err)
	}
	changed := request
	changed.Operation = store.RunControlStop
	if _, err := ledger.RunControlReserve(ctx, "ses_run_control_1", changed); err == nil {
		t.Fatal("different-operation cmd_id retry unexpectedly succeeded")
	}
	second := request
	second.CommandID = "cmd_interrupt_2"
	if _, err := ledger.RunControlReserve(ctx, "ses_run_control_1", second); err == nil {
		t.Fatal("second nonterminal run control unexpectedly succeeded")
	}
	stale := writer
	stale.LeaseID = "lease_stale"
	if _, err := ledger.RunControlFinalize(ctx, "ses_run_control_1", request.CommandID, store.RunControlFinalize{ReservationVersion: reserved.Reservation.ReservationVersion, Writer: &stale, Outcome: store.RunControlCompleted}); err == nil {
		t.Fatal("stale writer completed run control")
	}
	completed, err := ledger.RunControlFinalize(ctx, "ses_run_control_1", request.CommandID, store.RunControlFinalize{ReservationVersion: reserved.Reservation.ReservationVersion, Writer: &writer, Outcome: store.RunControlCompleted})
	if err != nil || completed.Outcome != store.RunControlCompleted || completed.TerminalEventSeq == nil || *completed.TerminalEventSeq != capabilitySeq+2 {
		t.Fatalf("RunControlFinalize(completed interrupt) = %+v, %v", completed, err)
	}
	assertRunControlCompletionOrder(t, ledger, "ses_run_control_1", capabilitySeq+1, *completed.TerminalEventSeq, "ready", request)
	if _, err := ledger.RunControlFinalize(ctx, "ses_run_control_1", request.CommandID, store.RunControlFinalize{ReservationVersion: reserved.Reservation.ReservationVersion, Writer: &writer, Outcome: store.RunControlCompleted}); err == nil {
		t.Fatal("second terminal run-control finalize unexpectedly succeeded")
	}

	stopStateSeq := appendRunControlEvent(t, ledger, "ses_run_control_1", "session.state", `{"state":"ready"}`)
	stopRequest := store.RunControlRequest{CommandID: "cmd_stop_1", Operation: store.RunControlStop, PreControlState: "ready", PreControlStateSeq: stopStateSeq, Writer: writer}
	stopReserved, err := ledger.RunControlReserve(ctx, "ses_run_control_1", stopRequest)
	if err != nil || stopReserved.Duplicate {
		t.Fatalf("stop RunControlReserve() = %+v, %v", stopReserved, err)
	}
	settingsLedger, ok := ledger.(store.SettingsCommandStore)
	if !ok {
		t.Fatal("run-control backend cannot use the durable reopen harness")
	}
	reopened, ok := harness.Reopen(t, settingsLedger).(store.RunControlStore)
	if !ok {
		t.Fatal("reopened backend does not implement the run-control ledger")
	}
	pending, err := reopened.PendingRunControls(ctx, "ses_run_control_1")
	if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], stopReserved.Reservation) {
		t.Fatalf("restart pending run-control recovery = %+v, %v", pending, err)
	}
	recovered, err := reopened.RecoverRunControl(ctx, "ses_run_control_1", stopRequest.CommandID, "recovery_unconfirmed")
	if err != nil || recovered.Outcome != store.RunControlOutcomeUnknown || recovered.TerminalEventSeq == nil {
		t.Fatalf("RecoverRunControl() = %+v, %v", recovered, err)
	}
	if commands, err := reopened.PendingRunControls(ctx, "ses_run_control_1"); err != nil || len(commands) != 0 {
		t.Fatalf("recovered run control remained pending: %+v, %v", commands, err)
	}

	unsupportedWriter := bindRunControlWriter(t, reopened, "ses_run_control_unsupported", "lease_run_control_unsupported")
	unsupportedState := appendRunControlEvent(t, reopened, "ses_run_control_unsupported", "session.state", `{"state":"busy"}`)
	unsupportedCapability := appendRunControlEvent(t, reopened, "ses_run_control_unsupported", "session.run.capabilities", `{}`)
	if _, err := reopened.PublishRunControlCapability(ctx, "ses_run_control_unsupported", store.RunControlCapabilityUpdate{EventSeq: unsupportedCapability, Writer: unsupportedWriter}); err != nil {
		t.Fatalf("publish unsupported run-control capability: %v", err)
	}
	if _, err := reopened.RunControlReserve(ctx, "ses_run_control_unsupported", store.RunControlRequest{CommandID: "cmd_interrupt_unsupported", Operation: store.RunControlInterrupt, PreControlState: "busy", PreControlStateSeq: unsupportedState, Writer: unsupportedWriter}); err == nil {
		t.Fatal("unsupported run-control capability created a reservation")
	}

	replacementWriter := bindRunControlWriter(t, reopened, "ses_run_control_replacement", "lease_run_control_old")
	replacementState := appendRunControlEvent(t, reopened, "ses_run_control_replacement", "session.state", `{"state":"busy"}`)
	replacementCapability := appendRunControlEvent(t, reopened, "ses_run_control_replacement", "session.run.capabilities", `{}`)
	if _, err := reopened.PublishRunControlCapability(ctx, "ses_run_control_replacement", store.RunControlCapabilityUpdate{EventSeq: replacementCapability, InterruptSupported: true, Writer: replacementWriter}); err != nil {
		t.Fatalf("publish replacement capability: %v", err)
	}
	replacementRequest := store.RunControlRequest{CommandID: "cmd_interrupt_replacement", Operation: store.RunControlInterrupt, PreControlState: "busy", PreControlStateSeq: replacementState, Writer: replacementWriter}
	if _, err := reopened.RunControlReserve(ctx, "ses_run_control_replacement", replacementRequest); err != nil {
		t.Fatalf("reserve replacement fixture: %v", err)
	}
	_ = bindRunControlWriter(t, reopened, "ses_run_control_replacement", "lease_run_control_new")
	if pending, err := reopened.PendingRunControls(ctx, "ses_run_control_replacement"); err != nil || len(pending) != 0 {
		t.Fatalf("writer replacement left a pending control: %+v, %v", pending, err)
	}
	if recovered, err := reopened.RunControl(ctx, "ses_run_control_replacement", replacementRequest.CommandID); err != nil || recovered.Outcome != store.RunControlOutcomeUnknown || recovered.TerminalEventSeq == nil {
		t.Fatalf("writer replacement did not finalize unknown: %+v, %v", recovered, err)
	}
}

func bindRunControlWriter(t *testing.T, ledger store.RunControlStore, sessionID, leaseID string) store.RunControlWriter {
	t.Helper()
	connections, ok := ledger.(store.AdapterConnectionStore)
	if !ok {
		t.Fatal("run-control backend lacks trusted adapter connection storage")
	}
	current, err := connections.AdapterConnection(context.Background(), sessionID)
	if err != nil {
		if _, err := connections.InitializeAdapterConnection(context.Background(), store.AdapterConnectionInitialize{SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			t.Fatalf("initialize run-control writer: %v", err)
		}
		current, err = connections.AdapterConnection(context.Background(), sessionID)
		if err != nil {
			t.Fatalf("read initialized run-control writer connection: %v", err)
		}
	}
	connection, err := connections.AcceptAdapterHello(context.Background(), sessionID, store.AdapterHello{CredentialGeneration: current.ActiveCredentialGeneration, WriterLeaseID: leaseID})
	if err != nil {
		t.Fatalf("accept run-control writer hello: %v", err)
	}
	return store.RunControlWriter{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: connection.ActiveCredentialGeneration, LeaseID: leaseID}
}

func appendRunControlEvent(t *testing.T, ledger store.EventStore, sessionID, eventType, payload string) int64 {
	t.Helper()
	seq, err := ledger.Append(context.Background(), sessionID, []store.PendingEvent{{Type: eventType, Time: time.Now(), Payload: json.RawMessage(payload)}})
	if err != nil {
		t.Fatalf("append %s: %v", eventType, err)
	}
	return seq
}

func assertRunControlCompletionOrder(t *testing.T, ledger store.EventStore, sessionID string, stateSeq, outcomeSeq int64, state string, request store.RunControlRequest) {
	t.Helper()
	var events []store.Event
	if err := ledger.Replay(context.Background(), sessionID, stateSeq-1, func(event store.Event) error {
		events = append(events, event)
		return nil
	}); err != nil || len(events) != 2 || events[0].Seq != stateSeq || events[0].Type != "session.state" || events[1].Seq != outcomeSeq || events[1].Type != "session.run.outcome" {
		t.Fatalf("run-control completion replay = %+v, %v", events, err)
	}
	var gotState struct {
		State string `json:"state"`
	}
	var outcome struct {
		CommandID       string                    `json:"cmd_id"`
		Operation       store.RunControlOperation `json:"operation"`
		Outcome         store.RunControlOutcome   `json:"outcome"`
		CompletionState *string                   `json:"completion_state"`
		ReasonCode      *string                   `json:"reason_code"`
	}
	if json.Unmarshal(events[0].Payload, &gotState) != nil || gotState.State != state || json.Unmarshal(events[1].Payload, &outcome) != nil || outcome.CommandID != request.CommandID || outcome.Operation != request.Operation || outcome.Outcome != store.RunControlCompleted || outcome.CompletionState == nil || *outcome.CompletionState != state || outcome.ReasonCode != nil {
		t.Fatalf("run-control completion payload = state=%s outcome=%+v", string(events[0].Payload), outcome)
	}
}

func settingsString(value string) *string { return &value }

func bindSettingsWriter(t *testing.T, ledger store.SettingsCommandStore, sessionID, leaseID string) store.SettingsWriter {
	t.Helper()
	connections, ok := ledger.(store.AdapterConnectionStore)
	if !ok {
		t.Fatal("settings Store does not expose Adapter connection authority")
	}
	if _, err := connections.AdapterConnection(context.Background(), sessionID); err != nil {
		if _, err := connections.InitializeAdapterConnection(context.Background(), store.AdapterConnectionInitialize{SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			t.Fatalf("initialize Settings writer = %v", err)
		}
	}
	return replaceSettingsWriter(t, ledger, sessionID, leaseID)
}

func replaceSettingsWriter(t *testing.T, ledger store.SettingsCommandStore, sessionID, leaseID string) store.SettingsWriter {
	t.Helper()
	connections, ok := ledger.(store.AdapterConnectionStore)
	if !ok {
		t.Fatal("settings Store does not expose Adapter connection authority")
	}
	current, err := connections.AdapterConnection(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("read Settings writer connection = %v", err)
	}
	connection, err := connections.AcceptAdapterHello(context.Background(), sessionID, store.AdapterHello{CredentialGeneration: current.ActiveCredentialGeneration, WriterLeaseID: leaseID})
	if err != nil {
		t.Fatalf("accept Settings writer hello = %v", err)
	}
	return store.SettingsWriter{ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: connection.ActiveCredentialGeneration, LeaseID: leaseID}
}

func appendSettingsCapabilityEvent(t *testing.T, ledger store.EventStore, sessionID string) int64 {
	t.Helper()
	seq, err := ledger.Append(context.Background(), sessionID, []store.PendingEvent{{
		Type: "session.settings.capabilities", Time: time.Now(), Payload: json.RawMessage(`{}`),
	}})
	if err != nil {
		t.Fatalf("append settings capability event: %v", err)
	}
	return seq
}

func assertRevokedSettingsWriter(t *testing.T, ledger store.SettingsCommandStore, harness SettingsCommandHarness, capability store.SettingsCapability) {
	t.Helper()
	ctx := context.Background()
	for _, sessionID := range []string{"ses_settings_revoked_ack", "ses_settings_revoked_finalize"} {
		current := bindSettingsWriter(t, ledger, sessionID, "lease_"+sessionID)
		update := store.SettingsCapabilityUpdate{Fingerprint: capability.Fingerprint, EffectiveModelID: capability.EffectiveModelID, EffectivePermissionModeID: capability.EffectivePermissionModeID}
		update.Writer, update.EventSeq = current, appendSettingsCapabilityEvent(t, ledger, sessionID)
		published, err := ledger.PublishSettingsCapability(ctx, sessionID, update)
		if err != nil {
			t.Fatalf("publish revoked-writer fixture %s: %v", sessionID, err)
		}
		request := store.SettingsCommandRequest{CommandID: "cmd_" + sessionID, RequestFingerprint: published.Fingerprint, RequestedModelID: settingsString("reasoning"), Writer: current}
		reserved, err := ledger.SettingsCommandReserve(ctx, sessionID, request)
		if err != nil || reserved.Duplicate {
			t.Fatalf("reserve revoked-writer fixture %s: %+v, %v", sessionID, reserved, err)
		}
		if sessionID == "ses_settings_revoked_finalize" {
			if _, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, sessionID, request.CommandID, reserved.Command.ReservationVersion, current); err != nil {
				t.Fatalf("acknowledge revoked-finalize fixture: %v", err)
			}
		}
		harness.RevokeWriter(t, ledger, sessionID)
		if connection, err := ledger.(store.AdapterConnectionStore).AdapterConnection(ctx, sessionID); err != nil || connection.ConnectionEpoch != current.ConnectionEpoch || connection.ActiveCredentialGeneration != current.CredentialGeneration || connection.RevokedAt == nil {
			t.Fatalf("revoked writer changed trusted tuple for %s: %+v, %v", sessionID, connection, err)
		}
		update.EventSeq = appendSettingsCapabilityEvent(t, ledger, sessionID)
		if _, err := ledger.PublishSettingsCapability(ctx, sessionID, update); err == nil {
			t.Fatalf("revoked unchanged-tuple writer published for %s", sessionID)
		}
		if _, err := ledger.SettingsCommandReserve(ctx, sessionID, store.SettingsCommandRequest{CommandID: "cmd_second_" + sessionID, RequestFingerprint: published.Fingerprint, RequestedModelID: settingsString("reasoning"), Writer: current}); err == nil {
			t.Fatalf("revoked unchanged-tuple writer reserved for %s", sessionID)
		}
		if sessionID == "ses_settings_revoked_ack" {
			if _, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, sessionID, request.CommandID, reserved.Command.ReservationVersion, current); err == nil {
				t.Fatal("revoked unchanged-tuple writer acknowledged delivery")
			}
			continue
		}
		reason := "recovery_unconfirmed"
		if _, err := ledger.FinalizeSettingsCommand(ctx, sessionID, request.CommandID, store.SettingsCommandFinalize{ReservationVersion: reserved.Command.ReservationVersion, ExpectedStatus: store.SettingsCommandPending, Writer: &current, Outcome: store.SettingsCommandOutcomeUnknown, ReasonCode: &reason, EffectiveCapability: published}); err == nil {
			t.Fatal("revoked unchanged-tuple writer finalized")
		}
	}
}

type AttachmentHarness struct {
	Open   func(t *testing.T) store.AttachmentStore
	Reopen func(t *testing.T, current store.AttachmentStore) store.AttachmentStore
}

type ProposalHarness struct {
	Open       func(t *testing.T) store.ProposedEventStore
	Reopen     func(t *testing.T, current store.ProposedEventStore) store.ProposedEventStore
	Authority  func(t *testing.T, proposals store.ProposedEventStore) store.CommandAuthority
	Invalidate func(t *testing.T, proposals store.ProposedEventStore, kind CommandAuthorityFailure)
}

type ConnectionHarness struct {
	Open       func(t *testing.T) store.AdapterConnectionStore
	Invalidate func(t *testing.T, connections store.AdapterConnectionStore, terminal bool)
}

type AttachAttemptHarness struct {
	Open   func(t *testing.T) store.AttachAttemptStore
	Reopen func(t *testing.T, current store.AttachAttemptStore) store.AttachAttemptStore
}

type WarmAttachFailure string

const (
	WarmAttachFailureAttempt    WarmAttachFailure = "attempt"
	WarmAttachFailureAttachment WarmAttachFailure = "attachment"
	WarmAttachFailureOutbox     WarmAttachFailure = "outbox"
	WarmAttachFailureSummary    WarmAttachFailure = "summary"
)

type WarmAttachHarness struct {
	Open   func(t *testing.T) store.WarmAttachStore
	Fail   func(t *testing.T, warm store.WarmAttachStore, failure WarmAttachFailure)
	Expire func(t *testing.T, warm store.WarmAttachStore)
	Absent func(t *testing.T, warm store.WarmAttachStore, request store.WarmAttachRequest)
}

// WarmAttachContract proves that admission and the initial reference-only
// delivery and target credential activation are indivisible Store truth.
// Post-commit credential delivery remains outside this contract.
func WarmAttachContract(t *testing.T, harness WarmAttachHarness) {
	t.Helper()
	if harness.Open == nil || harness.Fail == nil || harness.Expire == nil || harness.Absent == nil {
		t.Fatal("warm-attach harness must provide open, fail, expire, and absent")
	}
	for _, shape := range []reflect.Type{
		reflect.TypeOf(store.WarmAttachFirstDelivery{}),
		reflect.TypeOf(store.WarmAttachTargetActivation{}),
		reflect.TypeOf(store.WarmAttachRequest{}),
		reflect.TypeOf(store.WarmAttachOutbox{}),
		reflect.TypeOf(store.WarmAttachCommit{}),
	} {
		for _, forbidden := range []string{"Grant", "Bearer", "Credential", "Content", "Payload", "Message", "Token", "Task", "Run", "VM", "Provider", "TargetState", "TargetTerminal", "TargetConflict", "TargetTruth"} {
			if _, found := shape.FieldByName(forbidden); found {
				t.Fatalf("warm-attach type %s leaks %s", shape.Name(), forbidden)
			}
		}
	}
	request := warmAttachRequest()
	ctx := context.Background()
	t.Run("expired_grant_rolls_back", func(t *testing.T) {
		warm := harness.Open(t)
		expired := request
		expired.Attempt.ExpiresAt = time.Now().Add(-time.Second)
		if _, err := warm.CommitWarmAttach(ctx, expired); err == nil {
			t.Fatal("expired grant committed warm attach")
		}
		harness.Absent(t, warm, expired)
	})
	for _, test := range []struct {
		name   string
		mutate func(*store.WarmAttachRequest)
	}{
		{name: "expired_delivery_deadline", mutate: func(item *store.WarmAttachRequest) {
			expiresAt := time.Now().Add(-time.Second)
			item.Attachment.ExpiresAt = expiresAt
			item.FirstDelivery.ExpiresAt = expiresAt
		}},
		{name: "delivery_deadline_after_grant", mutate: func(item *store.WarmAttachRequest) {
			expiresAt := item.Attempt.ExpiresAt.Add(time.Second)
			item.Attachment.ExpiresAt = expiresAt
			item.FirstDelivery.ExpiresAt = expiresAt
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			warm := harness.Open(t)
			invalid := request
			test.mutate(&invalid)
			if _, err := warm.CommitWarmAttach(ctx, invalid); err == nil {
				t.Fatal("invalid delivery deadline committed warm attach")
			}
			harness.Absent(t, warm, invalid)
		})
	}
	for _, failure := range []WarmAttachFailure{
		WarmAttachFailureAttempt, WarmAttachFailureAttachment, WarmAttachFailureOutbox, WarmAttachFailureSummary,
	} {
		t.Run("rollback_"+string(failure), func(t *testing.T) {
			warm := harness.Open(t)
			harness.Fail(t, warm, failure)
			if _, err := warm.CommitWarmAttach(ctx, request); err == nil {
				t.Fatalf("%s failpoint committed warm attach", failure)
			}
			harness.Absent(t, warm, request)
		})
	}

	warm := harness.Open(t)
	committed, err := warm.CommitWarmAttach(ctx, request)
	if err != nil || committed.Duplicate {
		t.Fatalf("commit warm attach = %+v, %v", committed, err)
	}
	assertWarmAttachCommit(t, committed, request, 1)
	snapshot, err := warm.AttentionSnapshot(ctx, []string{request.Attachment.Identity.TargetSessionID})
	if err != nil || len(snapshot) != 1 || !reflect.DeepEqual(snapshot[0], committed.Summary) {
		t.Fatalf("committed attention snapshot = %+v, %v; want %+v", snapshot, err, committed.Summary)
	}
	retry, err := warm.CommitWarmAttach(ctx, request)
	expectedRetry := committed
	expectedRetry.Duplicate = true
	if err != nil || !retry.Duplicate || !reflect.DeepEqual(retry, expectedRetry) {
		t.Fatalf("exact warm-attach retry = %+v, %v; want original duplicate", retry, err)
	}
	changed := request
	changed.FirstDelivery.ReferenceDigest[0]++
	if _, err := warm.CommitWarmAttach(ctx, changed); err == nil {
		t.Fatal("changed warm-attach retry unexpectedly committed")
	}
	snapshot, err = warm.AttentionSnapshot(ctx, []string{request.Attachment.Identity.TargetSessionID})
	if err != nil || len(snapshot) != 1 || !reflect.DeepEqual(snapshot[0], committed.Summary) {
		t.Fatalf("changed retry mutated summary = %+v, %v", snapshot, err)
	}
	t.Run("concurrent_exact_retry", func(t *testing.T) {
		warm := harness.Open(t)
		start := make(chan struct{})
		results := make(chan store.WarmAttachCommit, 2)
		errors := make(chan error, 2)
		var group sync.WaitGroup
		for range 2 {
			group.Add(1)
			go func() {
				defer group.Done()
				<-start
				commit, err := warm.CommitWarmAttach(ctx, request)
				results <- commit
				errors <- err
			}()
		}
		close(start)
		group.Wait()
		close(results)
		close(errors)
		var created, duplicate int
		for err := range errors {
			if err != nil {
				t.Fatalf("concurrent warm attach: %v", err)
			}
		}
		for result := range results {
			if result.Duplicate {
				duplicate++
			} else {
				created++
			}
		}
		if created != 1 || duplicate != 1 {
			t.Fatalf("concurrent warm-attach outcomes created=%d duplicate=%d", created, duplicate)
		}
	})
	if _, err := warm.ExpireWarmAttach(ctx, request.Attachment.Identity.AttachID, committed.Attachment.DeliveryVersion+1); err == nil {
		t.Fatal("stale warm-attach expiry unexpectedly committed")
	}
	harness.Expire(t, warm)
	expired, err := warm.ExpireWarmAttach(ctx, request.Attachment.Identity.AttachID, committed.Attachment.DeliveryVersion)
	if err != nil {
		t.Fatalf("expire warm attach: %v", err)
	}
	if expired.Attachment.Status != store.AttachmentReauthorizationRequired || expired.Attachment.DeliveryVersion != committed.Attachment.DeliveryVersion+1 || expired.Summary.Blocker == nil || expired.Summary.Blocker.Kind != store.AttentionBlockerReauthorizationRequired || expired.Summary.SummaryVersion != committed.Summary.SummaryVersion+1 || expired.Summary.LatestSeq != committed.Summary.LatestSeq || !sameWarmAttachTime(expired.Summary.LastDurableEventAt, committed.Summary.LastDurableEventAt) || !sameWarmAttachTime(expired.Summary.LastClientCommandAt, committed.Summary.LastClientCommandAt) {
		t.Fatalf("expired warm attach = %+v; committed = %+v", expired, committed)
	}
}

func warmAttachRequest() store.WarmAttachRequest {
	grantExpiresAt := time.Now().Add(3 * time.Minute)
	deliveryExpiresAt := time.Now().Add(20 * time.Second)
	return store.WarmAttachRequest{
		Attempt: store.AttachAttemptRequest{
			Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{1}, AttachID: "att_warm", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
			Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{2}, KeyVersion: 1},
			ExpiresAt:   grantExpiresAt, Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: attachAttemptInt64(1),
		},
		Attachment:         store.AttachmentCreate{Identity: store.AttachmentIdentity{AttachID: "att_warm", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", TargetCredentialLineageRef: "lineage_target"}, ExpiresAt: deliveryExpiresAt},
		TargetActivation:   store.WarmAttachTargetActivation{Generation: 1, ExpiresAt: deliveryExpiresAt},
		BootstrapAdmission: store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: 1, AcceptedFence: 1, GrantFence: 2},
		FirstDelivery:      store.WarmAttachFirstDelivery{CommandID: "cmd_warm", ReferenceID: "ref_warm", ReferenceDigest: [32]byte{3}, ExpiresAt: deliveryExpiresAt},
	}
}

func assertWarmAttachCommit(t *testing.T, got store.WarmAttachCommit, request store.WarmAttachRequest, summaryVersion int64) {
	t.Helper()
	if got.Attempt.Identity != request.Attempt.Identity || got.Attempt.Fingerprint != request.Attempt.Fingerprint || got.Attachment.Identity != request.Attachment.Identity || got.Attachment.Status != store.AttachmentJoinPending || got.Attachment.DeliveryState != store.AttachmentDeliveryPending || !sameWarmAttachTime(got.Attachment.ExpiresAt, &request.Attachment.ExpiresAt) || got.TargetActivation != request.TargetActivation || got.Outbox.TargetSessionID != request.Attachment.Identity.TargetSessionID || got.Outbox.CommandID != request.FirstDelivery.CommandID || got.Outbox.ReferenceID != request.FirstDelivery.ReferenceID || got.Outbox.ReferenceDigest != request.FirstDelivery.ReferenceDigest || !got.Outbox.ExpiresAt.Equal(request.FirstDelivery.ExpiresAt) || got.Outbox.EventSeq < 1 || got.Summary.SessionID != request.Attachment.Identity.TargetSessionID || got.Summary.Blocker == nil || got.Summary.Blocker.Kind != store.AttentionBlockerQueued || got.Summary.SummaryVersion != summaryVersion || got.Summary.LastDurableEventAt == nil || got.Summary.LastClientCommandAt == nil || got.Summary.StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("warm-attach commit = %+v", got)
	}
	if got.Summary.Blocker.Reason == nil || *got.Summary.Blocker.Reason != "join_pending" || got.Summary.Blocker.ExpiresAt == nil || !got.Summary.Blocker.ExpiresAt.Equal(request.Attachment.ExpiresAt) || got.Summary.Blocker.BlockingSessionID != nil || got.Summary.Blocker.Operation != nil || got.Summary.LatestSeq != got.Outbox.EventSeq {
		t.Fatalf("warm-attach summary = %+v; outbox = %+v", got.Summary, got.Outbox)
	}
}

func sameWarmAttachTime(left, right *time.Time) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.Equal(*right))
}

type attentionSummarySurface interface {
	AttentionSnapshot(context.Context, []string) ([]store.SessionAttentionSummary, error)
}

// AttentionSummaryContract fixes the v2 projection's provider-neutral surface.
// Backend transaction, rebuild, and delivery behavior is deliberately exercised
// by T36 and T37 once those stores implement the contract.
func AttentionSummaryContract(t *testing.T) {
	t.Helper()
	var _ attentionSummarySurface = (store.AttentionSummaryStore)(nil)
	assertAttentionFields(t, reflect.TypeOf(store.AttentionBlocker{}), []string{
		"Kind", "Reason", "ExpiresAt", "BlockingSessionID", "Operation",
	})
	assertAttentionFields(t, reflect.TypeOf(store.AttentionPermission{}), []string{"ID", "Status"})
	assertAttentionFields(t, reflect.TypeOf(store.SessionAttentionSummary{}), []string{
		"SessionID", "LatestSeq", "State", "Permission", "TerminalOutcome", "LatestChangeSeq", "Blocker", "SummaryVersion", "LastDurableEventAt", "LastClientCommandAt", "StateOfProjection",
	})
	stringType := reflect.TypeOf("")
	int64Type := reflect.TypeOf(int64(0))
	int64PointerType := reflect.TypeOf((*int64)(nil))
	timePointerType := reflect.TypeOf((*time.Time)(nil))
	assertAttentionFieldTypes(t, reflect.TypeOf(store.AttentionBlocker{}), []reflect.Type{stringType, stringPointerType(), timePointerType, stringPointerType(), stringPointerType()})
	assertAttentionFieldTypes(t, reflect.TypeOf(store.AttentionPermission{}), []reflect.Type{stringType, stringType})
	assertAttentionFieldTypes(t, reflect.TypeOf(store.SessionAttentionSummary{}), []reflect.Type{
		stringType, int64Type, stringType, reflect.TypeOf((*store.AttentionPermission)(nil)), stringPointerType(), int64PointerType, reflect.TypeOf((*store.AttentionBlocker)(nil)), int64Type, timePointerType, timePointerType, stringType,
	})
	for _, value := range []struct {
		got, want string
	}{
		{store.AttentionBlockerQueued, "queued"},
		{store.AttentionBlockerReauthorizationRequired, "reauthorization_required"},
		{store.AttentionBlockerNewRunRequired, "new_run_required"},
		{store.AttentionBlockerOutcomeUnknown, "outcome_unknown"},
		{store.AttentionPermissionPending, "pending"},
		{store.AttentionProjectionComplete, "complete"},
		{store.AttentionProjectionIncomplete, "incomplete"},
	} {
		if value.got != value.want {
			t.Fatalf("attention contract value = %q, want %q", value.got, value.want)
		}
	}
	for _, shape := range []reflect.Type{
		reflect.TypeOf(store.AttentionBlocker{}),
		reflect.TypeOf(store.AttentionPermission{}),
		reflect.TypeOf(store.SessionAttentionSummary{}),
	} {
		for _, forbidden := range []string{"Task", "Run", "Provider", "Content", "Credential", "Token", "Transcript", "Message", "File", "Diff"} {
			if _, found := shape.FieldByName(forbidden); found {
				t.Fatalf("attention type %s leaks %s", shape.Name(), forbidden)
			}
		}
	}
}

func assertAttentionFields(t *testing.T, typ reflect.Type, want []string) {
	t.Helper()
	if typ.NumField() != len(want) {
		t.Fatalf("%s field count = %d, want %d", typ.Name(), typ.NumField(), len(want))
	}
	for index, name := range want {
		if got := typ.Field(index).Name; got != name {
			t.Fatalf("%s field %d = %q, want %q", typ.Name(), index, got, name)
		}
	}
}

func assertAttentionFieldTypes(t *testing.T, typ reflect.Type, want []reflect.Type) {
	t.Helper()
	for index, expected := range want {
		if got := typ.Field(index).Type; got != expected {
			t.Fatalf("%s field %d type = %s, want %s", typ.Name(), index, got, expected)
		}
	}
}

func stringPointerType() reflect.Type {
	return reflect.TypeOf((*string)(nil))
}

func AttachAttemptContract(t *testing.T, harness AttachAttemptHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil {
		t.Fatal("attach-attempt harness must provide open and reopen")
	}
	ctx := context.Background()
	attempts := harness.Open(t)
	request := store.AttachAttemptRequest{
		Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{1}, AttachID: "att_contract", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
		Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{2}, KeyVersion: 1},
		ExpiresAt:   time.Now().Add(time.Minute), Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: attachAttemptInt64(1),
	}
	committed, err := attempts.CommitAttachAttempt(ctx, request)
	if err != nil || committed.Duplicate {
		t.Fatalf("commit attach attempt = %+v, %v", committed, err)
	}
	assertAttachAttempt(t, committed.Attempt, request)
	retry, err := attempts.CommitAttachAttempt(ctx, request)
	if err != nil || !retry.Duplicate {
		t.Fatalf("exact retry = %+v, %v", retry, err)
	}
	assertAttachAttempt(t, retry.Attempt, request)
	for _, mutate := range []func(*store.AttachAttemptRequest){
		func(item *store.AttachAttemptRequest) { item.Identity.JTIHash = [32]byte{} },
		func(item *store.AttachAttemptRequest) { item.Fingerprint.Domain = "wrong-domain" },
		func(item *store.AttachAttemptRequest) { item.Fingerprint.Version++ },
		func(item *store.AttachAttemptRequest) { item.Fingerprint.KeyVersion = 0 },
		func(item *store.AttachAttemptRequest) { item.ExpiresAt = time.Now() },
		func(item *store.AttachAttemptRequest) { item.ExpiresAt = time.Now().Add(6 * time.Minute) },
		func(item *store.AttachAttemptRequest) { item.IssuedCredentialGeneration = nil },
		func(item *store.AttachAttemptRequest) { item.Outcome = store.AttachAttemptRejected },
	} {
		invalid := request
		invalid.IssuedCredentialGeneration = attachAttemptInt64(*request.IssuedCredentialGeneration)
		mutate(&invalid)
		if _, err := attempts.CommitAttachAttempt(ctx, invalid); err == nil {
			t.Fatal("invalid attach attempt unexpectedly succeeded")
		}
		current, err := attempts.AttachAttempt(ctx, request.Identity.JTIHash)
		if err != nil {
			t.Fatalf("invalid attempt mutated result = %+v, %v", current, err)
		}
		assertAttachAttempt(t, current, request)
	}
	for _, shape := range []reflect.Type{reflect.TypeOf(store.AttachAttemptIdentity{}), reflect.TypeOf(store.AttachAttemptRequest{}), reflect.TypeOf(store.AttachAttempt{})} {
		for _, forbidden := range []string{"JTI", "Grant", "Bearer", "Token", "RawKey", "Content", "Payload", "Credential"} {
			if _, found := shape.FieldByName(forbidden); found {
				t.Fatalf("attach-attempt type %s leaks %s", shape.Name(), forbidden)
			}
		}
	}
	for _, mutate := range []func(*store.AttachAttemptRequest){
		func(item *store.AttachAttemptRequest) { item.Identity.AttachID = "att_other" },
		func(item *store.AttachAttemptRequest) { item.Identity.BootstrapSessionID = "ses_other" },
		func(item *store.AttachAttemptRequest) { item.Identity.TargetSessionID = "ses_other" },
		func(item *store.AttachAttemptRequest) { item.Identity.Provider = "other" },
		func(item *store.AttachAttemptRequest) { item.Fingerprint.Digest[0]++ },
		func(item *store.AttachAttemptRequest) { item.Fingerprint.KeyVersion++ },
		func(item *store.AttachAttemptRequest) { item.ExpiresAt = item.ExpiresAt.Add(time.Second) },
		func(item *store.AttachAttemptRequest) { *item.IssuedCredentialGeneration++ },
		func(item *store.AttachAttemptRequest) {
			item.Outcome = store.AttachAttemptRejected
			item.IssuedCredentialGeneration = nil
		},
	} {
		changed := request
		changed.IssuedCredentialGeneration = attachAttemptInt64(*request.IssuedCredentialGeneration)
		mutate(&changed)
		if _, err := attempts.CommitAttachAttempt(ctx, changed); err == nil {
			t.Fatal("changed attach attempt unexpectedly succeeded")
		}
		current, err := attempts.AttachAttempt(ctx, request.Identity.JTIHash)
		if err != nil {
			t.Fatalf("changed retry mutated attempt = %+v, %v", current, err)
		}
		assertAttachAttempt(t, current, request)
	}
	newAttempt := request
	newAttempt.Identity.JTIHash = [32]byte{3}
	newAttempt.Fingerprint.KeyVersion = 2
	newAttempt.IssuedCredentialGeneration = attachAttemptInt64(2)
	start := make(chan struct{})
	results := make(chan store.AttachAttemptCommit, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, item := range []store.AttachAttemptRequest{request, newAttempt} {
		wg.Add(1)
		go func(item store.AttachAttemptRequest) {
			defer wg.Done()
			<-start
			result, err := attempts.CommitAttachAttempt(ctx, item)
			results <- result
			errs <- err
		}(item)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent old/new attempt: %v", err)
		}
	}
	var duplicate, created int
	for result := range results {
		if result.Duplicate {
			duplicate++
		} else {
			created++
		}
	}
	if duplicate != 1 || created != 1 {
		t.Fatalf("concurrent old/new outcomes duplicate=%d created=%d", duplicate, created)
	}
	rejected := request
	rejected.Identity.JTIHash = [32]byte{4}
	rejected.Fingerprint.KeyVersion = 3
	rejected.Outcome = store.AttachAttemptRejected
	rejected.IssuedCredentialGeneration = nil
	result, err := attempts.CommitAttachAttempt(ctx, rejected)
	if err != nil {
		t.Fatalf("commit rejected attempt = %+v, %v", result, err)
	}
	assertAttachAttempt(t, result.Attempt, rejected)
	attempts = harness.Reopen(t, attempts)
	old, err := attempts.AttachAttempt(ctx, request.Identity.JTIHash)
	if err != nil {
		t.Fatalf("retired-key old attempt = %+v, %v", old, err)
	}
	assertAttachAttempt(t, old, request)
	newer, err := attempts.AttachAttempt(ctx, newAttempt.Identity.JTIHash)
	if err != nil {
		t.Fatalf("new attempt = %+v, %v", newer, err)
	}
	assertAttachAttempt(t, newer, newAttempt)
}

func attachAttemptInt64(value int64) *int64 { return &value }

func assertAttachAttempt(t *testing.T, got store.AttachAttempt, want store.AttachAttemptRequest) {
	t.Helper()
	if got.Identity != want.Identity || got.Fingerprint != want.Fingerprint || !got.ExpiresAt.Equal(want.ExpiresAt) || got.Outcome != want.Outcome || (got.IssuedCredentialGeneration == nil) != (want.IssuedCredentialGeneration == nil) {
		t.Fatalf("attach attempt = %+v, want %+v", got, want)
	}
	if got.IssuedCredentialGeneration != nil && *got.IssuedCredentialGeneration != *want.IssuedCredentialGeneration {
		t.Fatalf("attach attempt generation = %d, want %d", *got.IssuedCredentialGeneration, *want.IssuedCredentialGeneration)
	}
}

func ConnectionContract(t *testing.T, harness ConnectionHarness) {
	t.Helper()
	if harness.Open == nil || harness.Invalidate == nil {
		t.Fatal("connection contract harness must provide open and invalidate")
	}
	connections := harness.Open(t)
	transactions, ok := connections.(store.AdapterConnectionTransactor)
	if !ok {
		t.Fatal("connection store does not expose transaction-bound initialization")
	}
	rollback := errors.New("rollback connection initialization")
	transactionalInit := store.AdapterConnectionInitialize{SessionID: "ses_connection_transaction", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if err := transactions.WithAdapterConnectionTransaction(context.Background(), func(tx store.AdapterConnectionStore) error {
		if _, err := tx.InitializeAdapterConnection(context.Background(), transactionalInit); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("rollback transaction error = %v, want %v", err, rollback)
	}
	if _, err := connections.AdapterConnection(context.Background(), transactionalInit.SessionID); err == nil {
		t.Fatal("rolled back initializer left connection lineage")
	}
	if err := transactions.WithAdapterConnectionTransaction(context.Background(), func(tx store.AdapterConnectionStore) error {
		_, err := tx.InitializeAdapterConnection(context.Background(), transactionalInit)
		return err
	}); err != nil {
		t.Fatalf("commit transaction initialization: %v", err)
	}
	if record, err := connections.AdapterConnection(context.Background(), transactionalInit.SessionID); err != nil || record.ConnectionEpoch != 0 || record.AcceptedFence != 0 {
		t.Fatalf("transactional initialize = %+v, %v", record, err)
	}
	init := store.AdapterConnectionInitialize{SessionID: "ses_connection", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	record, err := connections.InitializeAdapterConnection(context.Background(), init)
	if err != nil || record.ConnectionEpoch != 0 || record.AcceptedFence != 0 {
		t.Fatalf("initialize = %+v, %v", record, err)
	}
	record, err = connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil || record.ConnectionEpoch != 1 || record.AcceptedFence < 1 {
		t.Fatalf("hello = %+v, %v", record, err)
	}
	firstHello := record
	record, err = connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil || record.ConnectionEpoch != firstHello.ConnectionEpoch+1 || record.AcceptedFence <= firstHello.AcceptedFence {
		t.Fatalf("second hello = %+v, %v", record, err)
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: record.ActiveCredentialGeneration, ConnectionEpoch: record.ConnectionEpoch, AcceptedFence: record.AcceptedFence, GrantFence: record.AcceptedFence + 1}
	if _, err := connections.ValidateAdapterAdmission(context.Background(), init.SessionID, admission); err != nil {
		t.Fatalf("validate current admission: %v", err)
	}
	rotation := store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: record.ActiveCredentialGeneration, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_connection"}
	record, err = connections.PrepareAdapterCredentialRotation(context.Background(), init.SessionID, rotation)
	if err != nil || record.PendingCredentialGeneration == nil || *record.PendingCredentialGeneration != 2 {
		t.Fatalf("prepare rotation = %+v, %v", record, err)
	}
	prepared := record
	record, err = connections.ActivateAdapterCredential(context.Background(), init.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: 2, PendingGeneration: 2, RotationID: "rot_connection"})
	if err != nil || record.ActiveCredentialGeneration != 2 || record.PriorRecoveryGeneration == nil || *record.PriorRecoveryGeneration != 1 || record.ConnectionEpoch != prepared.ConnectionEpoch+1 || record.AcceptedFence <= prepared.AcceptedFence {
		t.Fatalf("activate = %+v, %v", record, err)
	}
	if _, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1}); err == nil {
		t.Fatal("prior recovery normal hello unexpectedly succeeded")
	}
	if _, err := connections.ValidateAdapterAdmission(context.Background(), init.SessionID, admission); err == nil {
		t.Fatal("activation failed to fence prior grant admission")
	}
	currentAdmission := store.AdapterConnectionAdmission{CredentialGeneration: record.ActiveCredentialGeneration, ConnectionEpoch: record.ConnectionEpoch, AcceptedFence: record.AcceptedFence, GrantFence: record.AcceptedFence + 1}
	if _, err := connections.ValidateAdapterAdmission(context.Background(), init.SessionID, currentAdmission); err != nil {
		t.Fatalf("validate post-activation admission: %v", err)
	}
	connections = harness.Open(t)
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("initialize stale activation: %v", err)
	}
	if _, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1}); err != nil {
		t.Fatalf("hello stale activation: %v", err)
	}
	staleRotation := store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: 1, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_connection"}
	if _, err := connections.PrepareAdapterCredentialRotation(context.Background(), init.SessionID, staleRotation); err != nil {
		t.Fatalf("prepare stale activation: %v", err)
	}
	if _, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1}); err != nil {
		t.Fatalf("second hello stale activation: %v", err)
	}
	if _, err := connections.ActivateAdapterCredential(context.Background(), init.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: 1, PendingGeneration: 2, RotationID: "rot_connection"}); err == nil {
		t.Fatal("stale activation unexpectedly succeeded")
	}
	if record, err := connections.AdapterConnection(context.Background(), init.SessionID); err != nil || record.ActiveCredentialGeneration != 1 {
		t.Fatalf("stale activation mutated active generation = %+v, %v", record, err)
	}
	if record, err := connections.AdapterConnection(context.Background(), init.SessionID); err != nil || record.PendingCredentialGeneration == nil || *record.PendingCredentialGeneration != 2 {
		t.Fatalf("stale activation mutated pending lineage = %+v, %v", record, err)
	}
	connections = harness.Open(t)
	expiringInit := store.AdapterConnectionInitialize{SessionID: "ses_connection_expired", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(50 * time.Millisecond)}
	if _, err := connections.InitializeAdapterConnection(context.Background(), expiringInit); err != nil {
		t.Fatalf("initialize expiring connection: %v", err)
	}
	time.Sleep(75 * time.Millisecond)
	if _, err := connections.AcceptAdapterHello(context.Background(), expiringInit.SessionID, store.AdapterHello{CredentialGeneration: 1}); err == nil {
		t.Fatal("expired hello unexpectedly succeeded")
	}
	if record, err := connections.AdapterConnection(context.Background(), expiringInit.SessionID); err != nil || record.ConnectionEpoch != 0 || record.AcceptedFence != 0 {
		t.Fatalf("expired hello mutated connection = %+v, %v", record, err)
	}
	expired := mustAdapterConnection(t, connections, expiringInit.SessionID)
	expiredAdmission := store.AdapterConnectionAdmission{CredentialGeneration: expired.ActiveCredentialGeneration, ConnectionEpoch: expired.ConnectionEpoch, AcceptedFence: expired.AcceptedFence, GrantFence: expired.AcceptedFence + 1}
	assertConnectionNoWrite(t, connections, expiringInit.SessionID, expired, func() error {
		_, err := connections.ValidateAdapterAdmission(context.Background(), expiringInit.SessionID, expiredAdmission)
		return err
	})
	expiredRotation := store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: expired.ActiveCredentialGeneration, ExpectedEpoch: expired.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_active_expired"}
	assertConnectionNoWrite(t, connections, expiringInit.SessionID, expired, func() error {
		_, err := connections.PrepareAdapterCredentialRotation(context.Background(), expiringInit.SessionID, expiredRotation)
		return err
	})
	connections = harness.Open(t)
	activeExpiryInit := store.AdapterConnectionInitialize{SessionID: "ses_connection_active_expiry", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(50 * time.Millisecond)}
	record = initializeAndHello(t, connections, activeExpiryInit)
	activeExpiryRotation := store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: record.ActiveCredentialGeneration, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_active_expiry"}
	if _, err := connections.PrepareAdapterCredentialRotation(context.Background(), activeExpiryInit.SessionID, activeExpiryRotation); err != nil {
		t.Fatalf("prepare active expiry rotation: %v", err)
	}
	time.Sleep(75 * time.Millisecond)
	expired = mustAdapterConnection(t, connections, activeExpiryInit.SessionID)
	assertConnectionNoWrite(t, connections, activeExpiryInit.SessionID, expired, func() error {
		_, err := connections.ActivateAdapterCredential(context.Background(), activeExpiryInit.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, RotationID: activeExpiryRotation.RotationID})
		return err
	})
	connections = harness.Open(t)
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("initialize pending expiry: %v", err)
	}
	record, err = connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("hello pending expiry: %v", err)
	}
	expiringRotation := store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: record.ActiveCredentialGeneration, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(50 * time.Millisecond), RotationID: "rot_expired"}
	if _, err := connections.PrepareAdapterCredentialRotation(context.Background(), init.SessionID, expiringRotation); err != nil {
		t.Fatalf("prepare expiring rotation: %v", err)
	}
	time.Sleep(75 * time.Millisecond)
	if _, err := connections.ActivateAdapterCredential(context.Background(), init.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, RotationID: "rot_expired"}); err == nil {
		t.Fatal("expired pending credential unexpectedly activated")
	}
	if current, err := connections.AdapterConnection(context.Background(), init.SessionID); err != nil || current.ActiveCredentialGeneration != 1 || current.PendingCredentialGeneration == nil || *current.PendingCredentialGeneration != 2 || current.ConnectionEpoch != record.ConnectionEpoch || current.AcceptedFence != record.AcceptedFence {
		t.Fatalf("expired activation mutated connection = %+v, %v", current, err)
	}
	connections = harness.Open(t)
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("initialize hello admission race: %v", err)
	}
	record, err = connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("hello admission race: %v", err)
	}
	admission = store.AdapterConnectionAdmission{CredentialGeneration: record.ActiveCredentialGeneration, ConnectionEpoch: record.ConnectionEpoch, AcceptedFence: record.AcceptedFence, GrantFence: record.AcceptedFence + 1}
	assertAdmissionFenceRace(t, connections, init.SessionID, admission, func() error {
		_, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
		return err
	})
	if _, err := connections.ValidateAdapterAdmission(context.Background(), init.SessionID, admission); err == nil {
		t.Fatal("post-hello stale admission unexpectedly succeeded")
	}
	connections = harness.Open(t)
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("initialize activation admission race: %v", err)
	}
	record, err = connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("hello activation race: %v", err)
	}
	rotation = store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: record.ActiveCredentialGeneration, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_admission_race"}
	if _, err := connections.PrepareAdapterCredentialRotation(context.Background(), init.SessionID, rotation); err != nil {
		t.Fatalf("prepare activation admission race: %v", err)
	}
	admission = store.AdapterConnectionAdmission{CredentialGeneration: record.ActiveCredentialGeneration, ConnectionEpoch: record.ConnectionEpoch, AcceptedFence: record.AcceptedFence, GrantFence: record.AcceptedFence + 1}
	assertAdmissionFenceRace(t, connections, init.SessionID, admission, func() error {
		_, err := connections.ActivateAdapterCredential(context.Background(), init.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, RotationID: rotation.RotationID})
		return err
	})
	if _, err := connections.ValidateAdapterAdmission(context.Background(), init.SessionID, admission); err == nil {
		t.Fatal("post-activation stale admission unexpectedly succeeded")
	}
	connections = harness.Open(t)
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("initialize concurrent hello: %v", err)
	}
	start := make(chan struct{})
	epochs := make(chan int64, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			record, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
			epochs <- record.ConnectionEpoch
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(epochs)
	close(errs)
	seen := make(map[int64]bool)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent hello error = %v", err)
		}
	}
	for epoch := range epochs {
		if epoch < 1 || seen[epoch] {
			t.Fatalf("duplicate or invalid hello epoch %d", epoch)
		}
		seen[epoch] = true
	}
	if len(seen) != 8 {
		t.Fatalf("hello epochs = %v", seen)
	}
	for _, terminal := range []bool{false, true} {
		connections = harness.Open(t)
		record = initializeAndHello(t, connections, init)
		admission = store.AdapterConnectionAdmission{CredentialGeneration: record.ActiveCredentialGeneration, ConnectionEpoch: record.ConnectionEpoch, AcceptedFence: record.AcceptedFence, GrantFence: record.AcceptedFence + 1}
		rotation = store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: record.ActiveCredentialGeneration, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_invalidated"}
		harness.Invalidate(t, connections, terminal)
		invalidated := mustAdapterConnection(t, connections, init.SessionID)
		assertConnectionNoWrite(t, connections, init.SessionID, invalidated, func() error {
			_, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
			return err
		})
		assertConnectionNoWrite(t, connections, init.SessionID, invalidated, func() error {
			_, err := connections.ValidateAdapterAdmission(context.Background(), init.SessionID, admission)
			return err
		})
		assertConnectionNoWrite(t, connections, init.SessionID, invalidated, func() error {
			_, err := connections.PrepareAdapterCredentialRotation(context.Background(), init.SessionID, rotation)
			return err
		})
		connections = harness.Open(t)
		record = initializeAndHello(t, connections, init)
		rotation = store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: record.ActiveCredentialGeneration, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_invalidated_activation"}
		if _, err := connections.PrepareAdapterCredentialRotation(context.Background(), init.SessionID, rotation); err != nil {
			t.Fatalf("prepare invalidation activation: %v", err)
		}
		harness.Invalidate(t, connections, terminal)
		invalidated = mustAdapterConnection(t, connections, init.SessionID)
		assertConnectionNoWrite(t, connections, init.SessionID, invalidated, func() error {
			_, err := connections.ActivateAdapterCredential(context.Background(), init.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: record.ConnectionEpoch, PendingGeneration: 2, RotationID: rotation.RotationID})
			return err
		})
	}
}
func initializeAndHello(t *testing.T, connections store.AdapterConnectionStore, init store.AdapterConnectionInitialize) store.AdapterConnection {
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("initialize connection: %v", err)
	}
	record, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: init.ActiveCredentialGeneration})
	if err != nil {
		t.Fatalf("hello connection: %v", err)
	}
	return record
}
func mustAdapterConnection(t *testing.T, connections store.AdapterConnectionStore, sessionID string) store.AdapterConnection {
	record, err := connections.AdapterConnection(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}
	return record
}
func assertConnectionNoWrite(t *testing.T, connections store.AdapterConnectionStore, sessionID string, want store.AdapterConnection, operation func() error) {
	if err := operation(); err == nil {
		t.Fatal("invalid connection operation unexpectedly succeeded")
	}
	if got := mustAdapterConnection(t, connections, sessionID); !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid connection operation mutated record = %+v, want %+v", got, want)
	}
}

func assertAdmissionFenceRace(t *testing.T, connections store.AdapterConnectionStore, sessionID string, admission store.AdapterConnectionAdmission, advance func() error) {
	t.Helper()
	start := make(chan struct{})
	results := make(chan error, 8)
	advanceResult := make(chan error, 1)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := connections.ValidateAdapterAdmission(context.Background(), sessionID, admission)
			results <- err
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		advanceResult <- advance()
	}()
	close(start)
	wg.Wait()
	close(results)
	for range results {
	}
	if err := <-advanceResult; err != nil {
		t.Fatalf("fence mutation: %v", err)
	}
}

type CommandAuthorityFailure string

const (
	CommandAuthoritySuperseded CommandAuthorityFailure = "superseded"
	CommandAuthorityRevoked    CommandAuthorityFailure = "revoked"
	CommandAuthorityExpired    CommandAuthorityFailure = "expired"
	CommandAuthorityTerminal   CommandAuthorityFailure = "terminal"
)

func Contract(t *testing.T, newStore Harness) {
	t.Helper()

	t.Run("append replay and latest", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_basic"
		first, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.tool_call", 2),
		})
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if first != 1 {
			t.Fatalf("first seq = %d, want 1", first)
		}

		latest, err := st.LatestSeq(ctx, sessionID)
		if err != nil {
			t.Fatalf("LatestSeq() error = %v", err)
		}
		if latest != 2 {
			t.Fatalf("latest = %d, want 2", latest)
		}

		got := replayAll(t, st, sessionID, 0)
		assertSeqs(t, got, []int64{1, 2})
		if got[0].SessionID != sessionID || got[0].Type != "session.message" {
			t.Fatalf("first replayed event = %+v", got[0])
		}
		if wantTime := time.UnixMilli(1764937200001); !got[0].Time.Equal(wantTime) {
			t.Fatalf("first replayed event time = %s, want %s", got[0].Time, wantTime)
		}
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(got[1].Payload, &payload); err != nil {
			t.Fatalf("payload is invalid JSON: %v", err)
		}
		if payload.N != 2 {
			t.Fatalf("payload.n = %d, want 2", payload.N)
		}
	})

	t.Run("replay after seq", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_replay"
		if _, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.message", 2),
			pending("session.message", 3),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		got := replayAll(t, st, sessionID, 1)
		assertSeqs(t, got, []int64{2, 3})
	})

	t.Run("append empty batch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		first, err := st.Append(ctx, "ses_contract_empty", nil)
		if err != nil {
			t.Fatalf("Append(empty) error = %v", err)
		}
		if first != 0 {
			t.Fatalf("first seq = %d, want 0", first)
		}
	})

	t.Run("sessions are independent", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		if first, err := st.Append(ctx, "ses_contract_a", []store.PendingEvent{pending("session.message", 1)}); err != nil || first != 1 {
			t.Fatalf("Append(session a) = %d, %v; want 1, nil", first, err)
		}
		if first, err := st.Append(ctx, "ses_contract_b", []store.PendingEvent{pending("session.message", 1)}); err != nil || first != 1 {
			t.Fatalf("Append(session b) = %d, %v; want 1, nil", first, err)
		}
	})

	t.Run("concurrent append has no gaps or duplicates", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_concurrent"
		const writers = 16
		const batchSize = 8

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, writers)
		for writer := range writers {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				<-start
				events := make([]store.PendingEvent, 0, batchSize)
				for i := range batchSize {
					events = append(events, pending("session.message", writer*batchSize+i))
				}
				_, err := st.Append(ctx, sessionID, events)
				if err != nil {
					errs <- err
				}
			}(writer)
		}

		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("Append() concurrent error = %v", err)
		}

		got := replayAll(t, st, sessionID, 0)
		wantTotal := writers * batchSize
		if len(got) != wantTotal {
			t.Fatalf("replayed %d events, want %d", len(got), wantTotal)
		}
		for i, ev := range got {
			wantSeq := int64(i + 1)
			if ev.Seq != wantSeq {
				t.Fatalf("event[%d].Seq = %d, want %d", i, ev.Seq, wantSeq)
			}
		}
	})

	t.Run("replay callback error stops scan", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_callback_error"
		if _, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.message", 2),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		wantErr := errors.New("stop replay")
		var calls int
		err := st.Replay(ctx, sessionID, 0, func(store.Event) error {
			calls++
			return wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("Replay() error = %v, want %v", err, wantErr)
		}
		if calls != 1 {
			t.Fatalf("callback calls = %d, want 1", calls)
		}
	})
}

func HistoryContract(t *testing.T, harness HistoryHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.PruneBefore == nil {
		t.Fatal("history contract harness must provide open, reopen, and prune callbacks")
	}

	t.Run("empty page and limit bounds", func(t *testing.T) {
		st := harness.Open(t)
		page := historyPage(t, st, "ses_history_empty", nil, 1)
		assertHistoryPage(t, page, nil, 0, nil, store.RetentionComplete)
		for _, limit := range []int{0, 101} {
			if _, err := st.History(context.Background(), "ses_history_empty", nil, limit); err == nil {
				t.Fatalf("History(limit=%d) unexpectedly succeeded", limit)
			}
		}
	})

	t.Run("exclusive cursor preserves ascending bounded order", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_cursor"
		appendHistoryEvents(t, st, sessionID, 5)

		page := historyPage(t, st, sessionID, nil, 2)
		assertHistoryPage(t, page, []int64{4, 5}, 5, int64Pointer(4), store.RetentionComplete)
		page = historyPage(t, st, sessionID, page.NextBeforeSeq, 2)
		assertHistoryPage(t, page, []int64{2, 3}, 5, int64Pointer(2), store.RetentionComplete)
		page = historyPage(t, st, sessionID, page.NextBeforeSeq, 2)
		assertHistoryPage(t, page, []int64{1}, 5, nil, store.RetentionComplete)
	})

	t.Run("retention gap remains explicit after prune", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_gap"
		appendHistoryEvents(t, st, sessionID, 5)
		harness.PruneBefore(t, st, sessionID, 4)
		page := historyPage(t, st, sessionID, nil, 100)
		assertHistoryPage(t, page, []int64{4, 5}, 5, nil, store.RetentionGap)
		beforeRetainedHistory := int64(4)
		page = historyPage(t, st, sessionID, &beforeRetainedHistory, 100)
		assertHistoryPage(t, page, nil, 5, nil, store.RetentionGap)
	})

	t.Run("concurrent append remains sequenced and pageable", func(t *testing.T) {
		st := harness.Open(t)
		const (
			sessionID = "ses_history_concurrent"
			writers   = 8
			batchSize = 4
		)
		start := make(chan struct{})
		errs := make(chan error, writers)
		var wg sync.WaitGroup
		for writer := range writers {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				<-start
				events := make([]store.PendingEvent, 0, batchSize)
				for index := range batchSize {
					events = append(events, pending("session.message", writer*batchSize+index))
				}
				_, err := st.Append(context.Background(), sessionID, events)
				errs <- err
			}(writer)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent Append() error = %v", err)
			}
		}

		page := historyPage(t, st, sessionID, nil, writers*batchSize)
		want := make([]int64, writers*batchSize)
		for index := range want {
			want[index] = int64(index + 1)
		}
		assertHistoryPage(t, page, want, int64(writers*batchSize), nil, store.RetentionComplete)
	})

	t.Run("reopen retains the same historical truth", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_reopen"
		appendHistoryEvents(t, st, sessionID, 3)
		st = harness.Reopen(t, st)
		page := historyPage(t, st, sessionID, nil, 100)
		assertHistoryPage(t, page, []int64{1, 2, 3}, 3, nil, store.RetentionComplete)
	})
}

func PendingCommandContract(t *testing.T, harness PendingCommandHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.Authority == nil || harness.Invalidate == nil {
		t.Fatal("pending command contract harness must provide open, reopen, authority, and invalidate callbacks")
	}

	t.Run("commit deduplicates one reference-only command", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{
			CommandID: "cmd_contract_1",
			Type:      "session.send",
			ExpiresAt: time.Now().Add(10 * time.Second),
		}
		result, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, userCommandEvent(1), request)
		if err != nil {
			t.Fatalf("CommitPendingCommand() error = %v", err)
		}
		assertPendingCommand(t, result.Command, "ses_command_1", request, 1, store.PendingCommandPending)
		if result.Duplicate {
			t.Fatal("initial pending command commit was duplicate")
		}
		duplicate, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, userCommandEvent(2), request)
		if err != nil {
			t.Fatalf("duplicate CommitPendingCommand() error = %v", err)
		}
		if !duplicate.Duplicate {
			t.Fatal("duplicate pending command commit was not marked duplicate")
		}
		assertPendingCommand(t, duplicate.Command, "ses_command_1", request, 1, store.PendingCommandPending)
		var events []store.Event
		if err := ledger.Replay(context.Background(), "ses_command_1", 0, func(event store.Event) error {
			events = append(events, event)
			return nil
		}); err != nil {
			t.Fatalf("Replay() error = %v", err)
		}
		if len(events) != 1 || events[0].Seq != result.Command.EventSeq || events[0].Type != "session.message" || !bytes.Contains(events[0].Payload, []byte(`"role":"user"`)) {
			t.Fatalf("atomic pending-command event reference = %+v", events)
		}
	})

	t.Run("only one claimant advances pending delivery", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{CommandID: "cmd_contract_claim", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
		if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_claim", authority, userCommandEvent(1), request); err != nil {
			t.Fatalf("CommitPendingCommand() error = %v", err)
		}
		pending, err := ledger.ListPendingCommands(context.Background(), "ses_command_claim", authority)
		if err != nil || len(pending) != 1 || pending[0].CommandID != request.CommandID || pending[0].Status != store.PendingCommandPending {
			t.Fatalf("ListPendingCommands() = %+v, %v; want one pending committed reference", pending, err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		claims := make(chan store.PendingCommandClaim, 8)
		errs := make(chan error, 8)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				claim, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID)
				claims <- claim
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(claims)
		close(errs)
		var claimed int
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent ClaimPendingCommand() error = %v", err)
			}
		}
		for claim := range claims {
			if claim.Claimed {
				claimed++
				assertPendingCommand(t, claim.Command, "ses_command_claim", request, 1, store.PendingCommandReceived)
			}
		}
		if claimed != 1 {
			t.Fatalf("successful concurrent claims = %d, want 1", claimed)
		}
		claimedList, err := ledger.ListPendingCommands(context.Background(), "ses_command_claim", authority)
		if err != nil || len(claimedList) != 1 || claimedList[0].Status != store.PendingCommandReceived {
			t.Fatalf("ListPendingCommands() after claim = %+v, %v; want received lease", claimedList, err)
		}
		claim, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID)
		if err != nil || claim.Claimed {
			t.Fatalf("post-claim ClaimPendingCommand() = %+v, %v; want not claimed", claim, err)
		}
		assertPendingCommand(t, claim.Command, "ses_command_claim", request, 1, store.PendingCommandReceived)
		claim, err = ledger.ClaimPendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID)
		if err != nil || claim.Claimed {
			t.Fatalf("second ClaimPendingCommand() = %+v, %v; want not claimed", claim, err)
		}
		resolved, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID, store.PendingCommandCompleted)
		if err != nil {
			t.Fatalf("ResolvePendingCommand() error = %v", err)
		}
		assertPendingCommand(t, resolved, "ses_command_claim", request, 1, store.PendingCommandCompleted)
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID, store.PendingCommandPending); err == nil {
			t.Fatal("ResolvePendingCommand(pending) unexpectedly succeeded")
		}
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID, store.PendingCommandOutcomeUnknown); err == nil {
			t.Fatal("ResolvePendingCommand() rewrote a terminal outcome")
		}
	})

	t.Run("authority loss rejects every mutation without new event", func(t *testing.T) {
		for _, kind := range []CommandAuthorityFailure{CommandAuthoritySuperseded, CommandAuthorityRevoked, CommandAuthorityExpired, CommandAuthorityTerminal} {
			t.Run(string(kind), func(t *testing.T) {
				ledger := harness.Open(t)
				authority := harness.Authority(t, ledger)
				harness.Invalidate(t, ledger, kind)
				request := store.PendingCommandRequest{CommandID: "cmd_contract_stale_" + string(kind), Type: "session.send", ExpiresAt: time.Now().Add(time.Second)}
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_stale", authority, userCommandEvent(1), request); err == nil {
					t.Fatal("stale CommitPendingCommand() unexpectedly succeeded")
				}
				latest, err := ledger.LatestSeq(context.Background(), "ses_command_stale")
				if err != nil || latest != 0 {
					t.Fatalf("stale commit latest seq = %d, %v; want 0, nil", latest, err)
				}

				ledger = harness.Open(t)
				authority = harness.Authority(t, ledger)
				request.CommandID = "cmd_contract_stale_claim_" + string(kind)
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_stale", authority, userCommandEvent(1), request); err != nil {
					t.Fatalf("prepare stale claim: %v", err)
				}
				harness.Invalidate(t, ledger, kind)
				if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_stale", authority, request.CommandID); err == nil {
					t.Fatal("stale ClaimPendingCommand() unexpectedly succeeded")
				}

				ledger = harness.Open(t)
				authority = harness.Authority(t, ledger)
				request.CommandID = "cmd_contract_stale_resolve_" + string(kind)
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_stale", authority, userCommandEvent(1), request); err != nil {
					t.Fatalf("prepare stale resolve: %v", err)
				}
				if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_stale", authority, request.CommandID); err != nil {
					t.Fatalf("prepare resolve claim: %v", err)
				}
				harness.Invalidate(t, ledger, kind)
				if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_stale", authority, request.CommandID, store.PendingCommandCompleted); err == nil {
					t.Fatal("stale ResolvePendingCommand() unexpectedly succeeded")
				}
			})
		}
	})

	t.Run("expired pending command cannot be claimed", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{CommandID: "cmd_contract_claim_expired", Type: "session.send", ExpiresAt: time.Now().Add(50 * time.Millisecond)}
		if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_expired", authority, userCommandEvent(1), request); err != nil {
			t.Fatalf("prepare expiry claim: %v", err)
		}
		time.Sleep(75 * time.Millisecond)
		if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_expired", authority, request.CommandID); err == nil {
			t.Fatal("expired ClaimPendingCommand() unexpectedly succeeded")
		}
	})

	t.Run("uncertain delivery can resolve after authority loss without replay", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{CommandID: "cmd_contract_unknown", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
		if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_unknown", authority, userCommandEvent(1), request); err != nil {
			t.Fatalf("prepare unknown delivery: %v", err)
		}
		if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_unknown", authority, request.CommandID); err != nil {
			t.Fatalf("claim unknown delivery: %v", err)
		}
		harness.Invalidate(t, ledger, CommandAuthorityRevoked)
		resolved, err := ledger.ResolvePendingCommandUnknown(context.Background(), "ses_command_unknown", request.CommandID)
		if err != nil {
			t.Fatalf("ResolvePendingCommandUnknown() error = %v", err)
		}
		assertPendingCommand(t, resolved, "ses_command_unknown", request, 1, store.PendingCommandOutcomeUnknown)
	})

	t.Run("reopen preserves queued and terminal ledger truth", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{CommandID: "cmd_contract_reopen", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
		committed, err := ledger.CommitPendingCommand(context.Background(), "ses_command_reopen", authority, userCommandEvent(1), request)
		if err != nil || committed.Duplicate {
			t.Fatalf("CommitPendingCommand() = %+v, %v; want new command", committed, err)
		}
		ledger = harness.Reopen(t, ledger)
		authority = harness.Authority(t, ledger)
		assertPendingCommandEvent(t, ledger, "ses_command_reopen", committed.Command.EventSeq)
		duplicate, err := ledger.CommitPendingCommand(context.Background(), "ses_command_reopen", authority, userCommandEvent(2), request)
		if err != nil || !duplicate.Duplicate {
			t.Fatalf("reopened duplicate = %+v, %v; want original duplicate", duplicate, err)
		}
		assertPendingCommand(t, duplicate.Command, "ses_command_reopen", request, committed.Command.EventSeq, store.PendingCommandPending)
		assertPendingCommandEvent(t, ledger, "ses_command_reopen", committed.Command.EventSeq)
		claim, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID)
		if err != nil || !claim.Claimed {
			t.Fatalf("reopened claim = %+v, %v; want claimed", claim, err)
		}
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID, store.PendingCommandCompleted); err != nil {
			t.Fatalf("reopened resolve: %v", err)
		}
		ledger = harness.Reopen(t, ledger)
		authority = harness.Authority(t, ledger)
		claim, err = ledger.ClaimPendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID)
		if err != nil || claim.Claimed || claim.Command.Status != store.PendingCommandCompleted {
			t.Fatalf("terminal reopened claim = %+v, %v; want completed non-claim", claim, err)
		}
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID, store.PendingCommandOutcomeUnknown); err == nil {
			t.Fatal("reopened terminal outcome was rewritten")
		}
	})

	t.Run("invalid references and expiry are rejected", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		invalid := []struct {
			name    string
			event   store.PendingEvent
			request store.PendingCommandRequest
		}{
			{"expired", userCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_expired", Type: "session.send", ExpiresAt: time.Now().Add(-time.Second)}},
			{"unbounded expiry", userCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_unbounded", Type: "session.send", ExpiresAt: time.Now().Add(31 * time.Second)}},
			{"wrong type", userCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_type", Type: "session.interrupt", ExpiresAt: time.Now().Add(time.Second)}},
			{"wrong event", pending("session.state", 1), store.PendingCommandRequest{CommandID: "cmd_contract_event", Type: "session.send", ExpiresAt: time.Now().Add(time.Second)}},
			{"non-user message", nonUserCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_agent", Type: "session.send", ExpiresAt: time.Now().Add(time.Second)}},
		}
		for _, test := range invalid {
			t.Run(test.name, func(t *testing.T) {
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_invalid", authority, test.event, test.request); err == nil {
					t.Fatal("CommitPendingCommand() unexpectedly succeeded")
				}
			})
		}
	})
}

func ProposalContract(t *testing.T, harness ProposalHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.Authority == nil || harness.Invalidate == nil {
		t.Fatal("proposal contract harness must provide open, reopen, authority, and invalidate callbacks")
	}

	t.Run("commit deduplicates one durable event and receipt", func(t *testing.T) {
		proposals := harness.Open(t)
		authority := harness.Authority(t, proposals)
		request := store.ProposedEventRequest{ProposalID: "proposal_contract_1", Event: pending("session.state", 1)}
		receipt, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_1", authority, request)
		if err != nil {
			t.Fatalf("CommitProposedEvent() error = %v", err)
		}
		assertProposalReceipt(t, receipt, "ses_proposal_1", request.ProposalID, 1)
		duplicate, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_1", authority, request)
		if err != nil {
			t.Fatalf("duplicate CommitProposedEvent() error = %v", err)
		}
		assertProposalReceipt(t, duplicate, "ses_proposal_1", request.ProposalID, 1)
		events := replayAll(t, proposals, "ses_proposal_1", 0)
		if len(events) != 1 || events[0].Seq != receipt.Seq || events[0].Type != request.Event.Type || !bytes.Equal(events[0].Payload, request.Event.Payload) {
			t.Fatalf("proposal durable event = %+v", events)
		}
	})

	t.Run("conflicting retry changes no durable truth", func(t *testing.T) {
		proposals := harness.Open(t)
		authority := harness.Authority(t, proposals)
		request := store.ProposedEventRequest{ProposalID: "proposal_contract_conflict", Event: pending("session.state", 1)}
		if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_conflict", authority, request); err != nil {
			t.Fatalf("CommitProposedEvent() error = %v", err)
		}
		for _, conflict := range []store.ProposedEventRequest{
			{ProposalID: request.ProposalID, Event: pending("session.message", 1)},
			{ProposalID: request.ProposalID, Event: pending("session.state", 2)},
			{ProposalID: request.ProposalID, Event: store.PendingEvent{Type: request.Event.Type, Time: request.Event.Time.Add(time.Millisecond), Payload: append([]byte(nil), request.Event.Payload...)}},
		} {
			if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_conflict", authority, conflict); err == nil {
				t.Fatal("conflicting proposal retry unexpectedly succeeded")
			}
		}
		if latest, err := proposals.LatestSeq(context.Background(), "ses_proposal_conflict"); err != nil || latest != 1 {
			t.Fatalf("latest seq after conflict = %d, %v; want 1, nil", latest, err)
		}
	})

	t.Run("input mutation cannot rewrite proposal truth", func(t *testing.T) {
		proposals := harness.Open(t)
		authority := harness.Authority(t, proposals)
		request := store.ProposedEventRequest{ProposalID: "proposal_contract_snapshot", Event: pending("session.state", 1)}
		originalPayload := append([]byte(nil), request.Event.Payload...)
		if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_snapshot", authority, request); err != nil {
			t.Fatalf("CommitProposedEvent() error = %v", err)
		}
		request.Event.Payload[0] = 'x'
		if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_snapshot", authority, request); err == nil {
			t.Fatal("mutated proposal retry unexpectedly succeeded")
		}
		events := replayAll(t, proposals, "ses_proposal_snapshot", 0)
		if len(events) != 1 || !bytes.Equal(events[0].Payload, originalPayload) {
			t.Fatalf("durable event after input mutation = %+v", events)
		}
	})

	t.Run("authority loss rejects before append", func(t *testing.T) {
		for _, kind := range []CommandAuthorityFailure{CommandAuthoritySuperseded, CommandAuthorityRevoked, CommandAuthorityExpired, CommandAuthorityTerminal} {
			t.Run(string(kind), func(t *testing.T) {
				proposals := harness.Open(t)
				authority := harness.Authority(t, proposals)
				harness.Invalidate(t, proposals, kind)
				if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_stale", authority, store.ProposedEventRequest{ProposalID: "proposal_" + string(kind), Event: pending("session.state", 1)}); err == nil {
					t.Fatal("stale proposal unexpectedly succeeded")
				}
				if latest, err := proposals.LatestSeq(context.Background(), "ses_proposal_stale"); err != nil || latest != 0 {
					t.Fatalf("latest seq after stale proposal = %d, %v; want 0, nil", latest, err)
				}
			})
		}
	})

	t.Run("concurrent retry and reopen preserve one receipt", func(t *testing.T) {
		proposals := harness.Open(t)
		authority := harness.Authority(t, proposals)
		request := store.ProposedEventRequest{ProposalID: "proposal_contract_reopen", Event: pending("session.state", 1)}
		start := make(chan struct{})
		receipts := make(chan store.ProposedEventReceipt, 8)
		errs := make(chan error, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				receipt, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_reopen", authority, request)
				receipts <- receipt
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(receipts)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent proposal error = %v", err)
			}
		}
		for receipt := range receipts {
			assertProposalReceipt(t, receipt, "ses_proposal_reopen", request.ProposalID, 1)
		}
		proposals = harness.Reopen(t, proposals)
		authority = harness.Authority(t, proposals)
		receipt, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_reopen", authority, request)
		if err != nil {
			t.Fatalf("reopened duplicate proposal: %v", err)
		}
		assertProposalReceipt(t, receipt, "ses_proposal_reopen", request.ProposalID, 1)
	})
}

func assertProposalReceipt(t *testing.T, receipt store.ProposedEventReceipt, sessionID, proposalID string, seq int64) {
	t.Helper()
	if receipt.SessionID != sessionID || receipt.ProposalID != proposalID || receipt.Seq != seq || receipt.Status != store.ProposedEventAccepted {
		t.Fatalf("proposal receipt = %+v, want session=%s proposal=%s seq=%d accepted", receipt, sessionID, proposalID, seq)
	}
}

func AttachmentContract(t *testing.T, harness AttachmentHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil {
		t.Fatal("attachment contract harness must provide open and reopen callbacks")
	}

	t.Run("creates stable identity and looks up by attachment or target", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_lookup", "ses_bootstrap_lookup", "ses_target_lookup")
		commit, err := ledger.CreateAttachment(context.Background(), request)
		if err != nil || commit.Noop {
			t.Fatalf("CreateAttachment() = %+v, %v; want new attachment", commit, err)
		}
		assertAttachment(t, commit.Attachment, request.Identity, store.AttachmentJoinPending, store.AttachmentDeliveryPending, 0, nil, &request.ExpiresAt, nil)
		assertAttachmentSummary(t, commit.Summary, commit.Attachment, nil)

		byID, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
		if err != nil {
			t.Fatalf("Attachment() error = %v", err)
		}
		byTarget, err := ledger.AttachmentForTarget(context.Background(), request.Identity.TargetSessionID)
		if err != nil {
			t.Fatalf("AttachmentForTarget() error = %v", err)
		}
		assertSameAttachment(t, byID, commit.Attachment)
		assertSameAttachment(t, byTarget, commit.Attachment)
		for _, mutate := range []func(*store.AttachmentIdentity){
			func(identity *store.AttachmentIdentity) { identity.BootstrapSessionID = "ses_bootstrap_rewrite" },
			func(identity *store.AttachmentIdentity) { identity.TargetSessionID = "ses_target_rewrite" },
			func(identity *store.AttachmentIdentity) { identity.TargetCredentialLineageRef = "lineage_rewrite" },
		} {
			retry := request
			mutate(&retry.Identity)
			if _, err := ledger.CreateAttachment(context.Background(), retry); err == nil {
				t.Fatal("identity-rewriting attachment retry unexpectedly succeeded")
			}
		}
		afterConflict, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
		if err != nil {
			t.Fatalf("Attachment() after immutable-identity conflicts: %v", err)
		}
		assertSameAttachment(t, afterConflict, commit.Attachment)
	})

	t.Run("versioned mutations return their committed blocker summary", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_mutation", "ses_bootstrap_mutation", "ses_target_mutation")
		_, err := ledger.CreateAttachment(context.Background(), request)
		if err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reason := "capacity"
		blockerSessionID := "ses_blocker_mutation"
		queued := store.AttachmentUpdate{
			Status:            store.AttachmentQueued,
			DeliveryState:     store.AttachmentDeliveryPending,
			QueueReason:       &reason,
			ExpiresAt:         &request.ExpiresAt,
			BlockingSessionID: &blockerSessionID,
			Blocker: &store.AttachmentBlocker{
				Kind:              store.AttachmentBlockerQueued,
				Reason:            &reason,
				ExpiresAt:         &request.ExpiresAt,
				BlockingSessionID: &blockerSessionID,
			},
		}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, queued)
		if err != nil {
			t.Fatalf("queued UpdateAttachment() error = %v", err)
		}
		assertAttachment(t, mutation.Attachment, request.Identity, store.AttachmentQueued, store.AttachmentDeliveryPending, 1, &reason, &request.ExpiresAt, &blockerSessionID)
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, queued.Blocker)
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, queued); err == nil {
			t.Fatal("stale UpdateAttachment() unexpectedly succeeded")
		}

		started := store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived}
		mutation, err = ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 1, started)
		if err != nil {
			t.Fatalf("start-received UpdateAttachment() error = %v", err)
		}
		assertAttachment(t, mutation.Attachment, request.Identity, store.AttachmentStartReceived, store.AttachmentDeliveryReceived, 2, nil, nil, nil)
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, nil)
	})
	t.Run("reconnect, canceled, and unknown outcomes remain bounded ledger truth", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_reconnect", "ses_bootstrap_reconnect", "ses_target_reconnect")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reauthorize := store.AttachmentUpdate{
			Status:        store.AttachmentReauthorizationRequired,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerReauthorizationRequired},
		}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, reauthorize)
		if err != nil {
			t.Fatalf("reauthorization UpdateAttachment() error = %v", err)
		}
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, reauthorize.Blocker)
		cancel := store.AttachmentUpdate{
			Status:        store.AttachmentCanceled,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerNewRunRequired},
		}
		mutation, err = ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 1, cancel)
		if err != nil {
			t.Fatalf("canceled UpdateAttachment() error = %v", err)
		}
		if mutation.Attachment.CanceledAt == nil {
			t.Fatal("canceled attachment omitted Store-clock canceled_at")
		}
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, cancel.Blocker)
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 2, reauthorize); err == nil {
			t.Fatal("canceled attachment was resurrected")
		}
	})
	t.Run("healthy target attach retry is a no-op", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_healthy", "ses_bootstrap_healthy", "ses_target_healthy")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		started := store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, started)
		if err != nil {
			t.Fatalf("start-received UpdateAttachment() error = %v", err)
		}
		duplicate, err := ledger.CreateAttachment(context.Background(), request)
		if err != nil || !duplicate.Noop {
			t.Fatalf("healthy CreateAttachment() = %+v, %v; want no-op", duplicate, err)
		}
		assertSameAttachment(t, duplicate.Attachment, mutation.Attachment)
	})
	t.Run("outcome unknown is bounded and terminal status cannot rebind", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_outcome", "ses_bootstrap_outcome", "ses_target_outcome")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		operation := "start"
		unknown := store.AttachmentUpdate{
			Status:        store.AttachmentStartReceived,
			DeliveryState: store.AttachmentDeliveryOutcomeUnknown,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerOutcomeUnknown, Operation: &operation},
		}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, unknown)
		if err != nil {
			t.Fatalf("outcome unknown UpdateAttachment() error = %v", err)
		}
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, unknown.Blocker)
		other := attachmentCreate("attach_contract_rebind", "ses_bootstrap_replacement", request.Identity.TargetSessionID)
		if _, err := ledger.CreateAttachment(context.Background(), other); err == nil {
			t.Fatal("replacement bootstrap rebound target attachment")
		}
		stored, err := ledger.AttachmentForTarget(context.Background(), request.Identity.TargetSessionID)
		if err != nil {
			t.Fatalf("AttachmentForTarget() after rebind rejection: %v", err)
		}
		if stored.Identity.BootstrapSessionID != request.Identity.BootstrapSessionID {
			t.Fatalf("bootstrap rebinding = %q, want %q", stored.Identity.BootstrapSessionID, request.Identity.BootstrapSessionID)
		}
	})
	t.Run("expiry is bounded and cannot be extended", func(t *testing.T) {
		ledger := harness.Open(t)
		expired := attachmentCreate("attach_contract_expired", "ses_bootstrap_expired", "ses_target_expired")
		expired.ExpiresAt = time.Now().Add(-time.Second)
		if _, err := ledger.CreateAttachment(context.Background(), expired); err == nil {
			t.Fatal("expired attachment create unexpectedly succeeded")
		}
		long := attachmentCreate("attach_contract_long", "ses_bootstrap_long", "ses_target_long")
		long.ExpiresAt = time.Now().Add(31 * time.Second)
		if _, err := ledger.CreateAttachment(context.Background(), long); err == nil {
			t.Fatal("unbounded attachment create unexpectedly succeeded")
		}
		request := attachmentCreate("attach_contract_expiry", "ses_bootstrap_expiry", "ses_target_expiry")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reason := "capacity"
		blocker := "ses_blocker_expiry"
		extended := request.ExpiresAt.Add(time.Second)
		update := store.AttachmentUpdate{
			Status:            store.AttachmentQueued,
			DeliveryState:     store.AttachmentDeliveryPending,
			QueueReason:       &reason,
			ExpiresAt:         &extended,
			BlockingSessionID: &blocker,
			Blocker:           &store.AttachmentBlocker{Kind: store.AttachmentBlockerQueued, Reason: &reason, ExpiresAt: &extended, BlockingSessionID: &blocker},
		}
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update); err == nil {
			t.Fatal("attachment expiry extension unexpectedly succeeded")
		}
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, store.AttachmentUpdate{Status: store.AttachmentStatus("starting"), DeliveryState: store.AttachmentDeliveryPending}); err == nil {
			t.Fatal("Hub-synthesized starting attachment state unexpectedly succeeded")
		}
	})
	t.Run("concurrent exact-version updates allow one winner", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_concurrent", "ses_bootstrap_concurrent", "ses_target_concurrent")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		update := store.AttachmentUpdate{
			Status:        store.AttachmentReauthorizationRequired,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerReauthorizationRequired},
		}
		start := make(chan struct{})
		results := make(chan error, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update)
				results <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		var successes int
		for err := range results {
			if err == nil {
				successes++
			}
		}
		if successes != 1 {
			t.Fatalf("concurrent attachment updates = %d successes, want 1", successes)
		}
	})
	t.Run("reopen preserves versioned attachment truth", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_reopen", "ses_bootstrap_reopen", "ses_target_reopen")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reauthorize := store.AttachmentUpdate{
			Status:        store.AttachmentReauthorizationRequired,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerReauthorizationRequired},
		}
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, reauthorize); err != nil {
			t.Fatalf("UpdateAttachment() before reopen: %v", err)
		}
		ledger = harness.Reopen(t, ledger)
		stored, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
		if err != nil {
			t.Fatalf("reopened Attachment() error = %v", err)
		}
		assertAttachment(t, stored, request.Identity, store.AttachmentReauthorizationRequired, store.AttachmentDeliveryPending, 1, nil, nil, nil)
	})
}
func attachmentCreate(attachID, bootstrapSessionID, targetSessionID string) store.AttachmentCreate {
	return store.AttachmentCreate{
		Identity: store.AttachmentIdentity{
			AttachID:                   attachID,
			BootstrapSessionID:         bootstrapSessionID,
			TargetSessionID:            targetSessionID,
			TargetCredentialLineageRef: "lineage_" + attachID,
		},
		ExpiresAt: time.Now().Add(20 * time.Second),
	}
}

func assertAttachment(t *testing.T, attachment store.Attachment, identity store.AttachmentIdentity, status store.AttachmentStatus, deliveryState store.AttachmentDeliveryState, version int64, reason *string, expiresAt *time.Time, blockingSessionID *string) {
	t.Helper()
	if attachment.Identity != identity || attachment.Status != status || attachment.DeliveryState != deliveryState || attachment.DeliveryVersion != version || !sameString(attachment.QueueReason, reason) || !sameTime(attachment.ExpiresAt, expiresAt) || !sameString(attachment.BlockingSessionID, blockingSessionID) {
		t.Fatalf("attachment = %+v, want identity=%+v status=%s delivery=%s version=%d reason=%v expiry=%v blocker=%v", attachment, identity, status, deliveryState, version, reason, expiresAt, blockingSessionID)
	}
}

func assertAttachmentSummary(t *testing.T, summary store.AttachmentSummary, attachment store.Attachment, blocker *store.AttachmentBlocker) {
	t.Helper()
	if summary.AttachID != attachment.Identity.AttachID || summary.TargetSessionID != attachment.Identity.TargetSessionID || summary.DeliveryVersion != attachment.DeliveryVersion || !sameTime(summary.ExpiresAt, attachment.ExpiresAt) || !sameBlocker(summary.Blocker, blocker) {
		t.Fatalf("attachment summary = %+v, want attachment=%+v blocker=%+v", summary, attachment, blocker)
	}
}

func assertSameAttachment(t *testing.T, got, want store.Attachment) {
	t.Helper()
	if got.Identity != want.Identity || got.Status != want.Status || got.DeliveryState != want.DeliveryState || got.DeliveryVersion != want.DeliveryVersion || !sameString(got.QueueReason, want.QueueReason) || !sameTime(got.ExpiresAt, want.ExpiresAt) || !sameTime(got.CanceledAt, want.CanceledAt) || !sameString(got.BlockingSessionID, want.BlockingSessionID) {
		t.Fatalf("attachment = %+v, want %+v", got, want)
	}
}

func sameBlocker(left, right *store.AttachmentBlocker) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.Kind == right.Kind && sameString(left.Reason, right.Reason) && sameTime(left.ExpiresAt, right.ExpiresAt) && sameString(left.BlockingSessionID, right.BlockingSessionID) && sameString(left.Operation, right.Operation))
}

func sameString(left, right *string) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func sameTime(left, right *time.Time) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.Equal(*right))
}

func userCommandEvent(n int) store.PendingEvent {
	event := pending("session.message", n)
	event.Payload = json.RawMessage(`{"role":"user"}`)
	return event
}

func nonUserCommandEvent(n int) store.PendingEvent {
	event := pending("session.message", n)
	event.Payload = json.RawMessage(`{"role":"agent"}`)
	return event
}

func assertPendingCommand(t *testing.T, command store.PendingCommand, sessionID string, request store.PendingCommandRequest, eventSeq int64, status store.PendingCommandStatus) {
	t.Helper()
	if command.SessionID != sessionID || command.CommandID != request.CommandID || command.Type != request.Type || command.EventSeq != eventSeq || command.Status != status || !command.ExpiresAt.Equal(request.ExpiresAt) {
		t.Fatalf("pending command = %+v, want session=%s command=%s type=%s event=%d status=%s expiry=%s", command, sessionID, request.CommandID, request.Type, eventSeq, status, request.ExpiresAt)
	}
}

func assertPendingCommandEvent(t *testing.T, ledger store.EventStore, sessionID string, eventSeq int64) {
	t.Helper()
	var events []store.Event
	if err := ledger.Replay(context.Background(), sessionID, 0, func(event store.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(events) != 1 || events[0].Seq != eventSeq || events[0].Type != "session.message" || !bytes.Contains(events[0].Payload, []byte(`"role":"user"`)) {
		t.Fatalf("reopened pending-command event reference = %+v", events)
	}
}

func appendHistoryEvents(t *testing.T, st store.EventStore, sessionID string, count int) {
	t.Helper()
	events := make([]store.PendingEvent, 0, count)
	for index := range count {
		events = append(events, pending("session.message", index))
	}
	if _, err := st.Append(context.Background(), sessionID, events); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func historyPage(t *testing.T, st store.HistoryStore, sessionID string, beforeSeq *int64, limit int) store.HistoryPage {
	t.Helper()
	page, err := st.History(context.Background(), sessionID, beforeSeq, limit)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	return page
}

func assertHistoryPage(t *testing.T, page store.HistoryPage, wantSeqs []int64, latestSeq int64, nextBeforeSeq *int64, retentionState string) {
	t.Helper()
	if page.LatestSeq != latestSeq {
		t.Fatalf("page latest seq = %d, want %d", page.LatestSeq, latestSeq)
	}
	if page.RetentionState != retentionState {
		t.Fatalf("page retention state = %q, want %q", page.RetentionState, retentionState)
	}
	if (page.NextBeforeSeq == nil) != (nextBeforeSeq == nil) || (page.NextBeforeSeq != nil && *page.NextBeforeSeq != *nextBeforeSeq) {
		t.Fatalf("page next before seq = %v, want %v", page.NextBeforeSeq, nextBeforeSeq)
	}
	if len(page.Events) != len(wantSeqs) {
		t.Fatalf("page event count = %d, want %d", len(page.Events), len(wantSeqs))
	}
	for index, event := range page.Events {
		if event.Seq != wantSeqs[index] {
			t.Fatalf("page event[%d] seq = %d, want %d", index, event.Seq, wantSeqs[index])
		}
		if index > 0 && page.Events[index-1].Seq >= event.Seq {
			t.Fatalf("page events are not strictly ascending: %d then %d", page.Events[index-1].Seq, event.Seq)
		}
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func pending(eventType string, n int) store.PendingEvent {
	payload := json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))
	return store.PendingEvent{
		Type:    eventType,
		Time:    time.UnixMilli(1764937200000 + int64(n)),
		Payload: payload,
	}
}

func replayAll(t *testing.T, st store.EventStore, sessionID string, afterSeq int64) []store.Event {
	t.Helper()

	var got []store.Event
	if err := st.Replay(context.Background(), sessionID, afterSeq, func(ev store.Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	return got
}

func assertSeqs(t *testing.T, got []store.Event, want []int64) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}
	for i, ev := range got {
		if ev.Seq != want[i] {
			t.Fatalf("event[%d].Seq = %d, want %d", i, ev.Seq, want[i])
		}
	}
}

type WorkspaceLeaseAuthorityFailure string

const (
	WorkspaceLeaseAuthoritySuperseded WorkspaceLeaseAuthorityFailure = "superseded"
	WorkspaceLeaseAuthorityRevoked    WorkspaceLeaseAuthorityFailure = "revoked"
	WorkspaceLeaseAuthorityExpired    WorkspaceLeaseAuthorityFailure = "expired"
	WorkspaceLeaseAuthorityTerminal   WorkspaceLeaseAuthorityFailure = "terminal"
	WorkspaceLeaseAttachmentExpired   WorkspaceLeaseAuthorityFailure = "attachment_expired"
	WorkspaceLeaseAttachmentCanceled  WorkspaceLeaseAuthorityFailure = "attachment_canceled"
)

type WorkspaceLeaseHarness struct {
	Open       func(t *testing.T) store.WorkspaceLeaseStore
	Reopen     func(t *testing.T, current store.WorkspaceLeaseStore) store.WorkspaceLeaseStore
	Invalidate func(t *testing.T, leases store.WorkspaceLeaseStore, key store.WorkspaceLeaseKey, owner store.WorkspaceLeaseOwner, kind WorkspaceLeaseAuthorityFailure)
}

// WorkspaceLeaseContract proves that a backend preserves only trusted,
// non-secret writer ownership. It deliberately models no Provider spawn: a
// rejected start receipt is the proof that spawning remains impossible.
func WorkspaceLeaseContract(t *testing.T, harness WorkspaceLeaseHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.Invalidate == nil {
		t.Fatal("workspace-lease harness must provide open, reopen, and invalidate")
	}
	for _, shape := range []reflect.Type{
		reflect.TypeOf(store.WorkspaceLeaseKey{}),
		reflect.TypeOf(store.WorkspaceLeaseOwner{}),
		reflect.TypeOf(store.WorkspaceLeaseChildScope{}),
		reflect.TypeOf(store.WorkspaceLeaseReserve{}),
		reflect.TypeOf(store.WorkspaceLease{}),
	} {
		if shape.Kind() != reflect.Struct {
			continue
		}
		for _, forbidden := range []string{"Client", "Adapter", "Provider", "Path", "Content", "Bearer", "Token", "Grant", "Credential"} {
			if _, found := shape.FieldByName(forbidden); found {
				t.Fatalf("workspace lease type %s leaks %s", shape.Name(), forbidden)
			}
		}
	}

	ctx := context.Background()
	request := workspaceLeaseReserve("worker_first", 1, 1, "lease_first")
	leases := harness.Open(t)
	reserved, err := leases.ReserveWorkspaceLease(ctx, request)
	if err != nil {
		t.Fatalf("reserve workspace lease: %v", err)
	}
	assertWorkspaceLease(t, reserved, request, store.WorkspaceLeaseReserved)
	for _, mutate := range []func(*store.WorkspaceLeaseReserve){
		func(item *store.WorkspaceLeaseReserve) { item.Key = store.WorkspaceLeaseKey{} },
		func(item *store.WorkspaceLeaseReserve) { item.Owner.WorkerID = "" },
		func(item *store.WorkspaceLeaseReserve) { item.Owner.LeaseID = "" },
		func(item *store.WorkspaceLeaseReserve) { item.ExpiresAt = time.Now() },
		func(item *store.WorkspaceLeaseReserve) {
			item.ChildScope = &store.WorkspaceLeaseChildScope{ParentKey: request.Key, ExpiresAt: time.Now().Add(time.Minute)}
		},
		func(item *store.WorkspaceLeaseReserve) {
			item.ChildScope = &store.WorkspaceLeaseChildScope{ParentKey: request.Key, CapabilityDigest: [32]byte{3}, ExpiresAt: time.Now()}
		},
	} {
		invalid := request
		mutate(&invalid)
		if _, err := leases.ReserveWorkspaceLease(ctx, invalid); err == nil {
			t.Fatal("invalid workspace lease unexpectedly reserved")
		}
		if current := workspaceLease(t, leases, request.Key); !reflect.DeepEqual(current, reserved) {
			t.Fatalf("invalid reserve mutated lease = %+v, want %+v", current, reserved)
		}
	}
	exactRetry, err := leases.ReserveWorkspaceLease(ctx, request)
	if err != nil || !reflect.DeepEqual(exactRetry, reserved) {
		t.Fatalf("exact owner retry = %+v, %v; want %+v, nil", exactRetry, err, reserved)
	}
	contender := workspaceLeaseReserve("worker_contender", 2, 2, "lease_contender")
	contender.Key = request.Key
	if _, err := leases.ReserveWorkspaceLease(ctx, contender); err == nil {
		t.Fatal("second live owner unexpectedly replaced reserved lease")
	}
	if current := workspaceLease(t, leases, request.Key); !reflect.DeepEqual(current, reserved) {
		t.Fatalf("contender reserve mutated lease = %+v, want %+v", current, reserved)
	}
	child := workspaceLeaseReserve("worker_child", 1, 1, "lease_child")
	child.Key = store.WorkspaceLeaseKey{2}
	child.ChildScope = &store.WorkspaceLeaseChildScope{
		ParentKey: request.Key, CapabilityDigest: [32]byte{3}, ExpiresAt: time.Now().Add(time.Minute),
	}
	childReserved, err := leases.ReserveWorkspaceLease(ctx, child)
	if err != nil {
		t.Fatalf("reserve scoped child workspace lease: %v", err)
	}
	assertWorkspaceLease(t, childReserved, child, store.WorkspaceLeaseReserved)
	wrongOwner := request.Owner
	wrongOwner.ConnectionEpoch++
	if _, err := leases.RecordWorkspaceStartReceived(ctx, request.Key, reserved.Version, wrongOwner); err == nil {
		t.Fatal("wrong owner unexpectedly recorded start_received")
	}
	if current := workspaceLease(t, leases, request.Key); !reflect.DeepEqual(current, reserved) {
		t.Fatalf("wrong owner mutated lease = %+v, want %+v", current, reserved)
	}
	started, err := leases.RecordWorkspaceStartReceived(ctx, request.Key, reserved.Version, request.Owner)
	if err != nil {
		t.Fatalf("record start_received: %v", err)
	}
	assertWorkspaceLease(t, started, request, store.WorkspaceLeaseStartReceived)
	if _, err := leases.RecordWorkspaceStartReceived(ctx, request.Key, reserved.Version, request.Owner); err == nil {
		t.Fatal("stale start_received version unexpectedly succeeded")
	}
	if _, err := leases.ReserveWorkspaceLease(ctx, contender); err == nil {
		t.Fatal("second live owner unexpectedly replaced started lease")
	}
	if current := workspaceLease(t, leases, request.Key); !reflect.DeepEqual(current, started) {
		t.Fatalf("contender reserve mutated started lease = %+v, want %+v", current, started)
	}
	assertWorkspaceLeaseReserveRace(t, harness, ctx)

	for _, kind := range []WorkspaceLeaseAuthorityFailure{
		WorkspaceLeaseAuthoritySuperseded,
		WorkspaceLeaseAuthorityRevoked,
		WorkspaceLeaseAuthorityExpired,
		WorkspaceLeaseAuthorityTerminal,
		WorkspaceLeaseAttachmentExpired,
		WorkspaceLeaseAttachmentCanceled,
	} {
		t.Run(string(kind), func(t *testing.T) {
			request := workspaceLeaseReserve("worker_stale", 1, 1, "lease_stale")
			leases := harness.Open(t)
			reserved, err := leases.ReserveWorkspaceLease(ctx, request)
			if err != nil {
				t.Fatalf("reserve stale lease: %v", err)
			}
			assertWorkspaceLease(t, reserved, request, store.WorkspaceLeaseReserved)
			harness.Invalidate(t, leases, request.Key, request.Owner, kind)
			stale := workspaceLease(t, leases, request.Key)
			if _, err := leases.RecordWorkspaceStartReceived(ctx, request.Key, stale.Version, request.Owner); err == nil {
				t.Fatal("invalid authority unexpectedly recorded start_received")
			}
			quarantined, err := leases.QuarantineWorkspaceLease(ctx, request.Key, stale.Version)
			if err != nil {
				t.Fatalf("quarantine stale lease: %v", err)
			}
			assertWorkspaceLease(t, quarantined, request, store.WorkspaceLeaseQuarantined)
			leases = harness.Reopen(t, leases)
			if reconstructed := workspaceLease(t, leases, request.Key); !reflect.DeepEqual(reconstructed, quarantined) {
				t.Fatalf("reconstructed quarantine = %+v, want %+v", reconstructed, quarantined)
			}
			retry := workspaceLeaseReserve("worker_current", 2, 2, "lease_current")
			retry.Key = request.Key
			if _, err := leases.ReserveWorkspaceLease(ctx, retry); err == nil {
				t.Fatal("quarantined lease unexpectedly admitted replacement worker")
			}
		})
	}

	leases = harness.Open(t)
	reserved, err = leases.ReserveWorkspaceLease(ctx, request)
	if err != nil {
		t.Fatalf("reserve releasable lease: %v", err)
	}
	if _, err := leases.ReleaseWorkspaceLeaseAfterQuiescence(ctx, request.Key, reserved.Version, wrongOwner); err == nil {
		t.Fatal("wrong owner unexpectedly released lease")
	}
	released, err := leases.ReleaseWorkspaceLeaseAfterQuiescence(ctx, request.Key, reserved.Version, request.Owner)
	if err != nil {
		t.Fatalf("release after quiescence: %v", err)
	}
	assertWorkspaceLease(t, released, request, store.WorkspaceLeaseReleased)
	retry := workspaceLeaseReserve("worker_current", 2, 2, "lease_current")
	retry.Key = request.Key
	if replacement, err := leases.ReserveWorkspaceLease(ctx, retry); err != nil {
		t.Fatalf("known release rejected current worker retry: %v", err)
	} else {
		assertWorkspaceLease(t, replacement, retry, store.WorkspaceLeaseReserved)
	}
}

func workspaceLeaseReserve(workerID string, epoch, generation int64, leaseID string) store.WorkspaceLeaseReserve {
	return store.WorkspaceLeaseReserve{
		Key: store.WorkspaceLeaseKey{1},
		Owner: store.WorkspaceLeaseOwner{
			WorkerID: workerID, SessionID: "ses_workspace", ConnectionEpoch: epoch,
			CredentialGeneration: generation, LeaseID: leaseID,
		},
		ExpiresAt: time.Now().Add(time.Minute),
	}
}

func workspaceLease(t *testing.T, leases store.WorkspaceLeaseStore, key store.WorkspaceLeaseKey) store.WorkspaceLease {
	t.Helper()
	lease, err := leases.WorkspaceLease(context.Background(), key)
	if err != nil {
		t.Fatalf("read workspace lease: %v", err)
	}
	return lease
}

func assertWorkspaceLease(t *testing.T, got store.WorkspaceLease, want store.WorkspaceLeaseReserve, status store.WorkspaceLeaseStatus) {
	t.Helper()
	if got.Key != want.Key || !sameWorkspaceLeaseChildScope(got.ChildScope, want.ChildScope) || got.Owner != want.Owner || got.Status != status || got.Version < 1 || !got.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("workspace lease = %+v, want key=%v owner=%+v status=%s", got, want.Key, want.Owner, status)
	}
}

func sameWorkspaceLeaseChildScope(left, right *store.WorkspaceLeaseChildScope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.ParentKey == right.ParentKey && left.CapabilityDigest == right.CapabilityDigest && left.ExpiresAt.Equal(right.ExpiresAt)
}

func assertWorkspaceLeaseReserveRace(t *testing.T, harness WorkspaceLeaseHarness, ctx context.Context) {
	t.Helper()
	leases := harness.Open(t)
	left := workspaceLeaseReserve("worker_race_left", 1, 1, "lease_race_left")
	left.Key = store.WorkspaceLeaseKey{9}
	right := workspaceLeaseReserve("worker_race_right", 2, 2, "lease_race_right")
	right.Key = left.Key
	type result struct {
		request store.WorkspaceLeaseReserve
		lease   store.WorkspaceLease
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var group sync.WaitGroup
	for _, request := range []store.WorkspaceLeaseReserve{left, right} {
		group.Add(1)
		go func(request store.WorkspaceLeaseReserve) {
			defer group.Done()
			<-start
			lease, err := leases.ReserveWorkspaceLease(ctx, request)
			results <- result{request: request, lease: lease, err: err}
		}(request)
	}
	close(start)
	group.Wait()
	close(results)
	var winner *result
	for result := range results {
		if result.err == nil {
			if winner != nil {
				t.Fatal("concurrent reserve admitted more than one owner")
			}
			copy := result
			winner = &copy
		}
	}
	if winner == nil {
		t.Fatal("concurrent reserve admitted no owner")
	}
	assertWorkspaceLease(t, winner.lease, winner.request, store.WorkspaceLeaseReserved)
	if current := workspaceLease(t, leases, left.Key); !reflect.DeepEqual(current, winner.lease) {
		t.Fatalf("concurrent reserve stored %+v, want winner %+v", current, winner.lease)
	}
}
