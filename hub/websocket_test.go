package hub_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
	"nhooyr.io/websocket"
)

func TestWebSocketServerAcceptsHelloAndPing(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1", LastSeq: 2}},
	})
	ack := readFrame(t, conn).(*protocol.HelloAck)
	if len(ack.Sessions) != 1 || ack.Sessions[0].SessionID != "ses_1" ||
		ack.Sessions[0].LatestSeq != 7 || ack.Sessions[0].ReplayFrom != 3 {
		t.Fatalf("hello ack = %+v", ack)
	}

	writeFrame(t, conn, &protocol.Ping{Nonce: "n1"})
	pong := readFrame(t, conn).(*protocol.Pong)
	if pong.Nonce != "n1" {
		t.Fatalf("pong nonce = %q", pong.Nonce)
	}
}

func TestWebSocketServerRejectsAdapterWithoutDispatchStore(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	if frame, err := readFrameWithin(adapter, time.Second); err != nil {
		t.Fatalf("read adapter rejection: %v", err)
	} else if _, ok := frame.(*protocol.HelloAck); ok {
		t.Fatalf("adapter without dispatch store received hello.ack: %+v", frame)
	}
}

func TestWebSocketHandlerFailsClosedForActivitySinkWithoutSummaryPageStore(t *testing.T) {
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{
		ActivitySink: hub.ActivitySinkFunc(func(context.Context, hub.ActivitySummary) error { return nil }),
	})
	if err := handler.RunActivityDispatcher(context.Background()); err == nil {
		t.Fatal("activity sink without summary page store was accepted")
	}
}

func TestWebSocketServerAcceptsAdapterWithDispatchStore(t *testing.T) {
	t.Parallel()

	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	if _, ok := readFrame(t, adapter).(*protocol.HelloAck); !ok {
		t.Fatal("adapter did not receive hello.ack")
	}
}

func TestWebSocketV2AdapterReceivesOnlyCurrentConnectionAuthorityReceipt(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "connection-authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	if ack := readFrame(t, client).(*protocol.HelloAck); ack.ConnectionAuthority != nil {
		t.Fatalf("client received adapter authority receipt: %+v", ack.ConnectionAuthority)
	}

	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	ack := readFrame(t, adapter).(*protocol.HelloAck)
	if ack.ConnectionAuthority == nil {
		t.Fatal("v2 adapter hello omitted connection authority receipt")
	}
	connection, err := events.AdapterConnection(ctx, "ses_1")
	if err != nil {
		t.Fatal(err)
	}
	receipt := ack.ConnectionAuthority
	if receipt.SessionID != "ses_1" || receipt.ConnectionEpoch != connection.ConnectionEpoch || receipt.CredentialGeneration != connection.ActiveCredentialGeneration || receipt.AcceptedFence != connection.AcceptedFence || receipt.WriterLeaseID == "" || receipt.ExpiresAt != connection.ActiveCredentialExpiresAt.UnixMilli() {
		t.Fatalf("connection authority receipt = %+v; connection = %+v", receipt, connection)
	}

	v1 := dialWebSocket(t, server.URL)
	defer v1.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloVersionFor(t, v1, "adapter-token", "ses_1", protocol.ProtocolVersion)
	if ack := readFrame(t, v1).(*protocol.HelloAck); ack.ConnectionAuthority != nil {
		t.Fatalf("v1 adapter received connection authority receipt: %+v", ack.ConnectionAuthority)
	}
}

func TestWebSocketV2ProviderStartUsesStoreLinearizedAdmission(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "provider-start.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	receipt := readFrame(t, adapter).(*protocol.HelloAck).ConnectionAuthority
	if receipt == nil {
		t.Fatal("v2 adapter receipt missing")
	}
	var key store.WorkspaceLeaseKey
	key[0] = 42
	if _, err := events.ReserveWorkspaceLease(ctx, store.WorkspaceLeaseReserve{Key: key, Owner: store.WorkspaceLeaseOwner{WorkerID: "worker_start", SessionID: receipt.SessionID, ConnectionEpoch: receipt.ConnectionEpoch, CredentialGeneration: receipt.CredentialGeneration, LeaseID: receipt.WriterLeaseID}, ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("ReserveWorkspaceLease() = %v", err)
	}
	writeFrame(t, adapter, &protocol.ProviderStart{Attempt: 1})
	if prepare, ok := readFrame(t, adapter).(*protocol.ProviderStartPrepare); !ok || prepare.Attempt != 1 {
		t.Fatal("provider start did not enter the Store-held prepare phase")
	}
	writeFrame(t, adapter, &protocol.ProviderStartStarted{Attempt: 1})
	if ack := readFrame(t, adapter).(*protocol.ProviderStartAck); ack.Attempt != 1 || ack.Status != protocol.ProviderStartAdmitted || ack.RecoveryHandle == "" {
		t.Fatalf("provider start ack = %+v", ack)
	}
	lease, err := events.WorkspaceLease(ctx, key)
	if err != nil || lease.Status != store.WorkspaceLeaseStartReceived {
		t.Fatalf("workspace start receipt = %+v, %v", lease, err)
	}
	writeFrame(t, adapter, &protocol.ProviderStart{Attempt: 2})
	if prepare, ok := readFrame(t, adapter).(*protocol.ProviderStartPrepare); !ok || prepare.Attempt != 2 {
		t.Fatal("provider restart did not enter the Store-held prepare phase")
	}
	writeFrame(t, adapter, &protocol.ProviderStartStarted{Attempt: 2})
	if ack := readFrame(t, adapter).(*protocol.ProviderStartAck); ack.Attempt != 2 || ack.Status != protocol.ProviderStartAdmitted || ack.RecoveryHandle == "" {
		t.Fatalf("provider restart ack = %+v", ack)
	}
	writeFrame(t, adapter, &protocol.ProviderStart{Attempt: 2})
	if ack := readFrame(t, adapter).(*protocol.ProviderStartAck); ack.Attempt != 2 || ack.Status != protocol.ProviderStartRejected {
		t.Fatalf("duplicate provider restart ack = %+v", ack)
	}

	v1 := dialWebSocket(t, server.URL)
	defer v1.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloVersionFor(t, v1, "adapter-token", "ses_1", protocol.ProtocolVersion)
	_ = readFrame(t, v1).(*protocol.HelloAck)
	writeFrame(t, v1, &protocol.ProviderStart{Attempt: 1})
	if _, ok := readFrame(t, v1).(*protocol.Error); !ok {
		t.Fatal("v1 Adapter start was not rejected")
	}
}

func TestWebSocketV2ProviderStartAcceptsLegacyEmptyFirstChild(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "provider-start-legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	receipt := readFrame(t, adapter).(*protocol.HelloAck).ConnectionAuthority
	if receipt == nil {
		t.Fatal("v2 adapter receipt missing")
	}
	var key store.WorkspaceLeaseKey
	key[0] = 43
	if _, err := events.ReserveWorkspaceLease(ctx, store.WorkspaceLeaseReserve{Key: key, Owner: store.WorkspaceLeaseOwner{WorkerID: "worker_start_legacy", SessionID: receipt.SessionID, ConnectionEpoch: receipt.ConnectionEpoch, CredentialGeneration: receipt.CredentialGeneration, LeaseID: receipt.WriterLeaseID}, ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("ReserveWorkspaceLease() = %v", err)
	}

	writeFrame(t, adapter, &protocol.ProviderStart{})
	if prepare, ok := readFrame(t, adapter).(*protocol.ProviderStartPrepare); !ok || prepare.Attempt != 0 {
		t.Fatalf("legacy provider start prepare = %#v", prepare)
	}
	writeFrame(t, adapter, &protocol.ProviderStartStarted{})
	if ack, ok := readFrame(t, adapter).(*protocol.ProviderStartAck); !ok || ack.Attempt != 0 || ack.Status != protocol.ProviderStartAdmitted || ack.RecoveryHandle != "" {
		t.Fatalf("legacy provider start ack = %#v", ack)
	}
	lease, err := events.WorkspaceLease(ctx, key)
	if err != nil || lease.Status != store.WorkspaceLeaseStartReceived {
		t.Fatalf("legacy workspace start receipt = %+v, %v", lease, err)
	}

	writeFrame(t, adapter, &protocol.ProviderStart{})
	if ack, ok := readFrame(t, adapter).(*protocol.ProviderStartAck); !ok || ack.Attempt != 0 || ack.Status != protocol.ProviderStartRejected || ack.RecoveryHandle != "" {
		t.Fatalf("duplicate legacy provider start ack = %#v", ack)
	}
}

func TestWebSocketSettingsRoutesCapabilityReserveDeliveryAndTerminalResult(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "settings.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	frame := readFrame(t, client)
	clientAck, ok := frame.(*protocol.HelloAck)
	if !ok {
		t.Fatalf("settings client hello = %#v", frame)
	}
	if clientAck.Capabilities == nil || clientAck.Capabilities.Settings == nil ||
		clientAck.Capabilities.Settings.SchemaVersion != 1 || clientAck.Capabilities.Settings.MaxPendingChanges != 1 ||
		clientAck.Capabilities.Settings.ProviderResponseTimeoutSeconds != 30 {
		t.Fatalf("settings hello capability = %+v", clientAck.Capabilities)
	}
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	publishSettingsCapability(t, adapter, "capability_initial", 1001, settingsCapabilityPayload("sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000", "balanced"))
	if event := readFrame(t, client).(*protocol.Event); event.Type != "session.settings.capabilities" || event.Seq == nil || *event.Seq != 1 {
		t.Fatalf("initial capability fanout = %+v", event)
	}
	stale := &protocol.Command{CommandID: "cmd_settings_stale", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","model_id":"reasoning"}`)}
	writeFrame(t, client, stale)
	if ack := readCommandAckFor(t, client, stale.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "stale_capability" {
		t.Fatalf("stale settings acknowledgement = %+v", ack)
	}

	command := &protocol.Command{CommandID: "cmd_settings_route", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","model_id":"reasoning"}`)}
	writeFrame(t, client, command)
	delivered := readFrame(t, adapter).(*protocol.Command)
	if delivered.CommandID != command.CommandID || delivered.Type != protocol.CommandSettingsChange || string(delivered.Payload) != string(command.Payload) {
		t.Fatalf("settings delivery = %+v", delivered)
	}
	pending := &protocol.Command{CommandID: "cmd_settings_pending", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","model_id":"balanced"}`)}
	writeFrame(t, client, pending)
	if ack := readCommandAckFor(t, client, pending.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "settings_change_pending" {
		t.Fatalf("pending settings acknowledgement = %+v", ack)
	}
	writeFrame(t, adapter, &protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted})
	execute := readFrame(t, adapter).(*protocol.SettingsDeliveryExecute)
	if execute.SessionID != "ses_1" || execute.CommandID != command.CommandID || execute.ReservationVersion != 1 || execute.OperationTimeoutMS != 30000 {
		t.Fatalf("settings execute = %+v", execute)
	}
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckAccepted || ack.Reason != "" {
		t.Fatalf("client delivery acknowledgement = %+v", ack)
	}
	writeFrame(t, client, command)
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckDuplicate || ack.Reason != "" {
		t.Fatalf("duplicate settings acknowledgement = %+v", ack)
	}

	publishSettingsCapability(t, adapter, "capability_effective", 1002, settingsCapabilityPayload("sha256:5e6c6921513d5a3e3ed9f5fc0a67bb94e55a66f822a76b1ced587d5a908e2761", "reasoning"))
	if event := readFrame(t, client).(*protocol.Event); event.Type != "session.settings.capabilities" || event.Seq == nil || *event.Seq != 2 {
		t.Fatalf("effective capability fanout = %+v", event)
	}
	writeFrame(t, adapter, &protocol.Event{Type: "session.settings.effective", SessionID: "ses_1", Time: 1003, ProposalID: "effective_result", Payload: json.RawMessage(`{"cmd_id":"cmd_settings_route","request_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","effective_fingerprint":"sha256:5e6c6921513d5a3e3ed9f5fc0a67bb94e55a66f822a76b1ced587d5a908e2761","outcome":"applied","effective_model_id":"reasoning","effective_permission_mode_id":"ask","reason_code":null}`)})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != "effective_result" || receipt.Seq != 3 {
		t.Fatalf("settings terminal receipt = %+v", receipt)
	}
	terminal := readFrame(t, client).(*protocol.Event)
	if terminal.Type != "session.settings.effective" || terminal.Seq == nil || *terminal.Seq != 3 || strings.Contains(string(terminal.Payload), "writer_lease") {
		t.Fatalf("settings terminal fanout = %+v", terminal)
	}
	stored, err := events.SettingsCommand(ctx, "ses_1", command.CommandID)
	if err != nil || stored.Status != store.SettingsCommandApplied || stored.TerminalEventSeq == nil || *stored.TerminalEventSeq != 3 {
		t.Fatalf("settings command terminal state = %+v, %v", stored, err)
	}
}

func TestWebSocketRunControlRoutesCapabilityAndStoreTerminalOutcome(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "run-control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(ctx, "ses_1", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: json.RawMessage(`{"state":"busy"}`)}}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1", LastSeq: 1}}})
	if ack := readFrame(t, client).(*protocol.HelloAck); ack.Capabilities == nil || ack.Capabilities.RunControl == nil ||
		ack.Capabilities.RunControl.SchemaVersion != 1 || ack.Capabilities.RunControl.MaxPending != 1 || ack.Capabilities.RunControl.CompletionTimeoutSeconds != 30 {
		t.Fatalf("run-control hello capability = %+v", ack.Capabilities)
	}
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.Event{Type: "session.run.capabilities", SessionID: "ses_1", Time: 1001, ProposalID: "run_capability_1", Payload: runControlCapabilityPayload(true, true)})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != "run_capability_1" || receipt.Seq != 2 {
		t.Fatalf("run-control capability receipt = %+v", receipt)
	}
	if capability := readFrame(t, client).(*protocol.Event); capability.Type != "session.run.capabilities" || capability.Seq == nil || *capability.Seq != 2 {
		t.Fatalf("run-control capability fanout = %+v", capability)
	}

	command := &protocol.Command{CommandID: "cmd_interrupt_1", Type: protocol.CommandSessionInterrupt, SessionID: "ses_1", Payload: json.RawMessage(`{}`)}
	writeFrame(t, client, command)
	if routed := readFrame(t, adapter).(*protocol.Command); routed.CommandID != command.CommandID || routed.Type != command.Type {
		t.Fatalf("run-control route = %+v", routed)
	}
	writeFrame(t, adapter, &protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted})
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckAccepted || ack.Reason != "" {
		t.Fatalf("run-control routing acknowledgement = %+v", ack)
	}

	outcome := json.RawMessage(`{"cmd_id":"cmd_interrupt_1","operation":"interrupt","outcome":"completed","completion_state":"ready","reason_code":null}`)
	writeFrame(t, adapter, &protocol.Event{Type: "session.run.outcome", SessionID: "ses_1", Time: 1002, ProposalID: "run_outcome_1", Payload: outcome})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != "run_outcome_1" || receipt.Seq != 4 {
		t.Fatalf("run-control outcome receipt = %+v", receipt)
	}
	if state := readFrame(t, client).(*protocol.Event); state.Type != "session.state" || state.Seq == nil || *state.Seq != 3 || string(state.Payload) != `{"state":"ready"}` {
		t.Fatalf("run-control completion state = %+v", state)
	}
	if terminal := readFrame(t, client).(*protocol.Event); terminal.Type != "session.run.outcome" || terminal.Seq == nil || *terminal.Seq != 4 || string(terminal.Payload) != string(outcome) {
		t.Fatalf("run-control Store terminal outcome = %+v", terminal)
	}
	stored, err := events.RunControl(ctx, "ses_1", command.CommandID)
	if err != nil || stored.Outcome != store.RunControlCompleted || stored.TerminalEventSeq == nil || *stored.TerminalEventSeq != 4 {
		t.Fatalf("run-control ledger = %+v, %v", stored, err)
	}
}

func TestWebSocketRunControlFailsClosedBeforeCapabilityOrUnsupportedControl(t *testing.T) {
	for _, test := range []struct {
		name       string
		capability *json.RawMessage
		reason     string
	}{
		{name: "unavailable", reason: "run_control_unavailable"},
		{name: "unsupported", capability: pointerToRawMessage(runControlCapabilityPayload(false, true)), reason: "interrupt_unsupported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "run-control-reject.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer ledger.Close()
			events := &settingsWebSocketStore{Store: ledger}
			if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
			if _, err := events.Append(ctx, "ses_1", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: json.RawMessage(`{"state":"busy"}`)}}); err != nil {
				t.Fatal(err)
			}
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
			client := dialWebSocket(t, server.URL)
			defer client.Close(websocket.StatusNormalClosure, "")
			adapter := dialWebSocket(t, server.URL)
			defer adapter.Close(websocket.StatusNormalClosure, "")
			writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1", LastSeq: 1}}})
			_ = readFrame(t, client).(*protocol.HelloAck)
			writeAdapterHelloV2(t, adapter, "adapter-token")
			_ = readFrame(t, adapter).(*protocol.HelloAck)
			if test.capability != nil {
				writeFrame(t, adapter, &protocol.Event{Type: "session.run.capabilities", SessionID: "ses_1", Time: 1001, ProposalID: "run_capability_reject", Payload: *test.capability})
				_ = readFrame(t, adapter).(*protocol.EventReceipt)
				_ = readFrame(t, client).(*protocol.Event)
			}
			command := &protocol.Command{CommandID: "cmd_interrupt_reject", Type: protocol.CommandSessionInterrupt, SessionID: "ses_1", Payload: json.RawMessage(`{}`)}
			writeFrame(t, client, command)
			if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckRejected || ack.Reason != test.reason {
				t.Fatalf("run-control rejection = %+v", ack)
			}
			if _, err := events.RunControl(ctx, "ses_1", command.CommandID); err == nil {
				t.Fatal("rejected run control created a durable reservation")
			}
		})
	}
}

func pointerToRawMessage(value json.RawMessage) *json.RawMessage { return &value }

func runControlCapabilityPayload(interruptSupported, stopSupported bool) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"schema_version":1,"interrupt_supported":%t,"stop_supported":%t}`, interruptSupported, stopSupported))
}

func TestWebSocketSettingsFinalizesReportedSuccessWithProviderMismatch(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "settings-mismatch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	payload := settingsCapabilityPayload("sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000", "balanced")
	publishSettingsCapability(t, adapter, "capability_initial", 1001, payload)
	_ = readFrame(t, client).(*protocol.Event)

	command := &protocol.Command{CommandID: "cmd_settings_mismatch", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","model_id":"reasoning"}`)}
	writeFrame(t, client, command)
	_ = readFrame(t, adapter).(*protocol.Command)
	writeFrame(t, adapter, &protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted})
	_ = readFrame(t, adapter).(*protocol.SettingsDeliveryExecute)
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckAccepted {
		t.Fatalf("delivery acknowledgement = %+v", ack)
	}
	publishSettingsCapability(t, adapter, "capability_readback", 1002, payload)
	_ = readFrame(t, client).(*protocol.Event)
	writeFrame(t, adapter, &protocol.Event{Type: "session.settings.effective", SessionID: "ses_1", Time: 1003, ProposalID: "mismatch_result", Payload: json.RawMessage(`{"cmd_id":"cmd_settings_mismatch","request_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","effective_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","outcome":"applied","effective_model_id":"balanced","effective_permission_mode_id":"ask","reason_code":null}`)})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != "mismatch_result" || receipt.Seq != 3 {
		t.Fatalf("mismatch terminal receipt = %+v", receipt)
	}
	terminal := readFrame(t, client).(*protocol.Event)
	if terminal.Type != "session.settings.effective" || !strings.Contains(string(terminal.Payload), `"outcome":"mismatched_effective"`) || !strings.Contains(string(terminal.Payload), `"reason_code":"provider_mismatched_effective"`) {
		t.Fatalf("mismatch terminal event = %+v", terminal)
	}
	stored, err := events.SettingsCommand(ctx, "ses_1", command.CommandID)
	if err != nil || stored.Status != store.SettingsCommandMismatched {
		t.Fatalf("mismatch terminal state = %+v, %v", stored, err)
	}
}

func TestWebSocketSettingsReplacementRecoversPendingReservation(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "settings-recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	first := dialWebSocket(t, server.URL)
	defer first.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHelloV2(t, first, "adapter-token")
	_ = readFrame(t, first).(*protocol.HelloAck)
	publishSettingsCapability(t, first, "capability_before_recovery", 1001, settingsCapabilityPayload("sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000", "balanced"))
	_ = readFrame(t, client).(*protocol.Event)

	command := &protocol.Command{CommandID: "cmd_settings_recovery", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","model_id":"reasoning"}`)}
	writeFrame(t, client, command)
	_ = readFrame(t, first).(*protocol.Command)
	writeFrame(t, first, &protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted})
	_ = readFrame(t, first).(*protocol.SettingsDeliveryExecute)
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckAccepted {
		t.Fatalf("delivery acknowledgement = %+v", ack)
	}

	replacement := dialWebSocket(t, server.URL)
	defer replacement.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, replacement, "adapter-token")
	_ = readFrame(t, replacement).(*protocol.HelloAck)
	publishSettingsCapability(t, replacement, "capability_after_recovery", 1002, settingsCapabilityPayload("sha256:5e6c6921513d5a3e3ed9f5fc0a67bb94e55a66f822a76b1ced587d5a908e2761", "reasoning"))
	if capability := readFrame(t, client).(*protocol.Event); capability.Type != "session.settings.capabilities" || capability.Seq == nil || *capability.Seq != 2 {
		t.Fatalf("replacement capability fanout = %+v", capability)
	}
	terminal := readFrame(t, client).(*protocol.Event)
	if terminal.Type != "session.settings.effective" || terminal.Seq == nil || *terminal.Seq != 3 ||
		!strings.Contains(string(terminal.Payload), `"outcome":"outcome_unknown"`) || !strings.Contains(string(terminal.Payload), `"reason_code":"recovery_unconfirmed"`) || strings.Contains(string(terminal.Payload), "writer_lease") {
		t.Fatalf("replacement recovery terminal = %+v", terminal)
	}
	stored, err := events.SettingsCommand(ctx, "ses_1", command.CommandID)
	if err != nil || stored.Status != store.SettingsCommandOutcomeUnknown || stored.TerminalEventSeq == nil || *stored.TerminalEventSeq != 3 {
		t.Fatalf("replacement recovery ledger = %+v, %v", stored, err)
	}
}

func TestWebSocketSettingsRestartRecoversPendingReservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "settings-restart-recovery.db")
	ledger, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	first := dialWebSocket(t, server.URL)

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHelloV2(t, first, "adapter-token")
	_ = readFrame(t, first).(*protocol.HelloAck)
	publishSettingsCapability(t, first, "capability_before_restart", 1001, settingsCapabilityPayload("sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000", "balanced"))
	_ = readFrame(t, client).(*protocol.Event)

	command := &protocol.Command{CommandID: "cmd_settings_restart", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","model_id":"reasoning"}`)}
	writeFrame(t, client, command)
	_ = readFrame(t, first).(*protocol.Command)
	writeFrame(t, first, &protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted})
	_ = readFrame(t, first).(*protocol.SettingsDeliveryExecute)
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckAccepted {
		t.Fatalf("delivery acknowledgement = %+v", ack)
	}

	server.Close()
	_ = first.Close(websocket.StatusNormalClosure, "")
	_ = client.Close(websocket.StatusNormalClosure, "")
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	events = &settingsWebSocketStore{Store: ledger}

	restarted := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	resumedClient := dialWebSocket(t, restarted.URL)
	defer resumedClient.Close(websocket.StatusNormalClosure, "")
	replacement := dialWebSocket(t, restarted.URL)
	defer replacement.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, resumedClient, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1", LastSeq: 1}}})
	_ = readFrame(t, resumedClient).(*protocol.HelloAck)
	writeAdapterHelloV2(t, replacement, "adapter-token")
	_ = readFrame(t, replacement).(*protocol.HelloAck)
	publishSettingsCapability(t, replacement, "capability_after_restart", 1002, settingsCapabilityPayload("sha256:5e6c6921513d5a3e3ed9f5fc0a67bb94e55a66f822a76b1ced587d5a908e2761", "reasoning"))
	if capability := readFrame(t, resumedClient).(*protocol.Event); capability.Type != "session.settings.capabilities" || capability.Seq == nil || *capability.Seq != 2 {
		t.Fatalf("restart capability fanout = %+v", capability)
	}
	terminal := readFrame(t, resumedClient).(*protocol.Event)
	if terminal.Type != "session.settings.effective" || terminal.Seq == nil || *terminal.Seq != 3 ||
		!strings.Contains(string(terminal.Payload), `"outcome":"outcome_unknown"`) || !strings.Contains(string(terminal.Payload), `"reason_code":"recovery_unconfirmed"`) || strings.Contains(string(terminal.Payload), "writer_lease") {
		t.Fatalf("restart recovery terminal = %+v", terminal)
	}
	stored, err := events.SettingsCommand(ctx, "ses_1", command.CommandID)
	if err != nil || stored.Status != store.SettingsCommandOutcomeUnknown || stored.TerminalEventSeq == nil || *stored.TerminalEventSeq != 3 {
		t.Fatalf("restart recovery ledger = %+v, %v", stored, err)
	}
}

func TestWebSocketSettingsRestartRecoversExpiredDeliveryReservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "settings-delivery-restart-recovery.db")
	ledger, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	first := dialWebSocket(t, server.URL)

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHelloV2(t, first, "adapter-token")
	_ = readFrame(t, first).(*protocol.HelloAck)
	publishSettingsCapability(t, first, "capability_before_delivery_restart", 1001, settingsCapabilityPayload("sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000", "balanced"))
	_ = readFrame(t, client).(*protocol.Event)

	command := &protocol.Command{CommandID: "cmd_settings_delivery_restart", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","model_id":"reasoning"}`)}
	writeFrame(t, client, command)
	_ = readFrame(t, first).(*protocol.Command)

	server.Close()
	_ = first.Close(websocket.StatusNormalClosure, "")
	_ = client.Close(websocket.StatusNormalClosure, "")
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `UPDATE session_settings_commands
		SET created_at_ms=CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)-6000,
		    delivery_deadline_ms=CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)-1000,
		    updated_at_ms=CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)-500
		WHERE session_id=? AND cmd_id=?`, "ses_1", command.CommandID); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	events = &settingsWebSocketStore{Store: ledger}

	restarted := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	resumedClient := dialWebSocket(t, restarted.URL)
	defer resumedClient.Close(websocket.StatusNormalClosure, "")
	replacement := dialWebSocket(t, restarted.URL)
	defer replacement.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, resumedClient, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1", LastSeq: 1}}})
	_ = readFrame(t, resumedClient).(*protocol.HelloAck)
	writeAdapterHelloV2(t, replacement, "adapter-token")
	_ = readFrame(t, replacement).(*protocol.HelloAck)
	publishSettingsCapability(t, replacement, "capability_after_delivery_restart", 1002, settingsCapabilityPayload("sha256:5e6c6921513d5a3e3ed9f5fc0a67bb94e55a66f822a76b1ced587d5a908e2761", "reasoning"))
	if capability := readFrame(t, resumedClient).(*protocol.Event); capability.Type != "session.settings.capabilities" || capability.Seq == nil || *capability.Seq != 2 {
		t.Fatalf("restart capability fanout = %+v", capability)
	}
	terminal := readFrame(t, resumedClient).(*protocol.Event)
	if terminal.Type != "session.settings.effective" || terminal.Seq == nil || *terminal.Seq != 3 ||
		!strings.Contains(string(terminal.Payload), `"outcome":"rejected"`) || !strings.Contains(string(terminal.Payload), `"reason_code":"adapter_delivery_failed"`) || strings.Contains(string(terminal.Payload), "writer_lease") {
		t.Fatalf("delivery recovery terminal = %+v", terminal)
	}
	stored, err := events.SettingsCommand(ctx, "ses_1", command.CommandID)
	if err != nil || stored.Status != store.SettingsCommandRejected || stored.TerminalEventSeq == nil || *stored.TerminalEventSeq != 3 {
		t.Fatalf("delivery recovery ledger = %+v, %v", stored, err)
	}
}

func publishSettingsCapability(t *testing.T, adapter *websocket.Conn, proposalID string, at int64, payload json.RawMessage) {
	t.Helper()
	writeFrame(t, adapter, &protocol.Event{Type: "session.settings.capabilities", SessionID: "ses_1", Time: at, ProposalID: proposalID, Payload: payload})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != proposalID {
		t.Fatalf("settings capability receipt = %+v", receipt)
	}
}

func settingsCapabilityPayload(fingerprint, effectiveModel string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"schema_version":1,"fingerprint":%q,"models":[{"id":"balanced","label":"Balanced"},{"id":"reasoning","label":"Reasoning"}],"permission_modes":[{"id":"ask","label":"Ask first"},{"id":"workspace","label":"Workspace"}],"effective_model_id":%q,"effective_permission_mode_id":"ask","model_change":"allowed","permission_change":"allowed","model_read_only_reason":null,"permission_read_only_reason":null}`, fingerprint, effectiveModel))
}

func TestWebSocketServerDeliversCommittedPendingCommandAfterAdapterReconnect(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 1}, map[string][]store.Event{
		"ses_1": {{SessionID: "ses_1", Seq: 1, Type: "session.message", Payload: json.RawMessage(`{"role":"user","content":"resume"}`)}},
	})
	events.seedPending(store.PendingCommand{SessionID: "ses_1", CommandID: "cmd_reconnect", Type: "session.send", EventSeq: 1, Status: store.PendingCommandPending, ExpiresAt: time.Now().Add(time.Minute)})
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	if _, ok := readFrame(t, adapter).(*protocol.HelloAck); !ok {
		t.Fatal("adapter did not receive hello.ack")
	}
	frame, readErr := readFrameWithin(adapter, time.Second)
	if readErr != nil {
		events.mu.Lock()
		status := events.pending["ses_1\x00cmd_reconnect"].Status
		events.mu.Unlock()
		t.Fatalf("durable reconnect read error: %v (status=%q)", readErr, status)
	}
	command, ok := frame.(*protocol.Command)
	if !ok || command.CommandID != "cmd_reconnect" || command.Type != protocol.CommandSessionSend || command.SessionID != "ses_1" || string(command.Payload) != `{"role":"user","content":"resume"}` {
		events.mu.Lock()
		status := events.pending["ses_1\x00cmd_reconnect"].Status
		events.mu.Unlock()
		t.Logf("durable pending status before assertion: %q", status)
		t.Fatalf("durable reconnect command = %#v, want reconstructed session.send", frame)
	}
	events.mu.Lock()
	status := events.pending["ses_1\x00cmd_reconnect"].Status
	events.mu.Unlock()
	if status != store.PendingCommandCompleted {
		t.Fatalf("durable reconnect status = %q, want completed", status)
	}
}

func TestWebSocketServerDoesNotReplayPreviouslyReceivedCommand(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 1}, map[string][]store.Event{
		"ses_1": {{SessionID: "ses_1", Seq: 1, Type: "session.message", Payload: json.RawMessage(`{"role":"user","content":"already-sent"}`)}},
	})
	events.seedPending(store.PendingCommand{SessionID: "ses_1", CommandID: "cmd_received", Type: "session.send", EventSeq: 1, Status: store.PendingCommandReceived, ExpiresAt: time.Now().Add(time.Minute)})
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	if _, ok := readFrame(t, adapter).(*protocol.HelloAck); !ok {
		t.Fatal("adapter did not receive hello.ack")
	}
	if _, err := readFrameWithin(adapter, 100*time.Millisecond); err == nil {
		t.Fatal("previously received durable command was replayed")
	}
	events.mu.Lock()
	status := events.pending["ses_1\x00cmd_received"].Status
	events.mu.Unlock()
	if status != store.PendingCommandOutcomeUnknown {
		t.Fatalf("previously received durable status = %q, want outcome_unknown", status)
	}
}

func TestWebSocketSessionAttachCommitsWarmAttachWithoutRouting(t *testing.T) {
	events := newRecordingWarmAttachStore()
	issuer := &recordingSessionCredentialIssuer{}
	verifier := &t18BAttachGrantVerifier{
		raw: "raw-grant-must-not-escape",
		grant: auth.AttachGrant{
			Audience: "deploy-attach", JTI: "jti_1", AttachID: "attach_1",
			BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code",
			IssuedAt: time.Now().Add(-time.Second), ExpiresAt: time.Now().Add(time.Minute),
			DeliveryDeadline: time.Now().Add(30 * time.Second),
		},
	}
	handshake := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: websocketTestAuth{
			principals: map[string]auth.Principal{
				"client-token":  {Subject: "client", Scopes: []auth.Scope{auth.SessionControl("ses_target")}},
				"adapter-token": {Subject: "adapter", Scopes: []auth.Scope{auth.SessionAdapter("ses_bootstrap")}},
			},
			credentials: map[string]adapterCredentialEvidence{"adapter": {Generation: 1, ExpiresAt: time.Now().Add(time.Hour)}},
		},
		AttachGrantVerifier: verifier, AttachGrantAudience: "deploy-attach", EventStore: events,
	})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
		cfg.SessionCredentialIssuer = issuer
		cfg.SessionCredentialLifecycle = issuer
	})
	legacyBootstrap := dialWebSocket(t, server.URL)
	defer legacyBootstrap.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloFor(t, legacyBootstrap, "adapter-token", "ses_bootstrap")
	_ = readFrame(t, legacyBootstrap).(*protocol.HelloAck)

	fence, err := events.AllocateAdapterGrantFence(context.Background())
	if err != nil {
		t.Fatalf("AllocateAdapterGrantFence() error = %v", err)
	}
	verifier.grant.GrantFence = fence

	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_target"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)
	v1Command := &protocol.Command{CommandID: "attach-v1-bootstrap", Type: protocol.CommandSessionAttach, SessionID: "ses_target", Payload: json.RawMessage(`{"grant":"raw-grant-must-not-escape"}`)}
	writeFrame(t, client, v1Command)
	if ack := readCommandAckFor(t, client, v1Command.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "attach_rejected" {
		t.Fatalf("v1 bootstrap acknowledgement = %+v", ack)
	}
	if got := len(events.warmAttachRequests()); got != 0 {
		t.Fatalf("v1 bootstrap wrote %d Store records", got)
	}
	if frame, err := readFrameWithin(legacyBootstrap, 100*time.Millisecond); err == nil {
		t.Fatalf("v1 bootstrap received v2 frame %+v", frame)
	}
	_ = legacyBootstrap.CloseNow()

	bootstrap := dialWebSocket(t, server.URL)
	defer bootstrap.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloVersionFor(t, bootstrap, "adapter-token", "ses_bootstrap", protocol.ProtocolVersionV2)
	_ = readFrame(t, bootstrap).(*protocol.HelloAck)
	fence, err = events.AllocateAdapterGrantFence(context.Background())
	if err != nil {
		t.Fatalf("AllocateAdapterGrantFence() after v2 bootstrap error = %v", err)
	}
	verifier.grant.GrantFence = fence
	issuer.setFailure(errors.New("issuer unavailable"))
	prepareFailed := &protocol.Command{CommandID: "attach-prepare-failed", Type: protocol.CommandSessionAttach, SessionID: "ses_target", Payload: json.RawMessage(`{"grant":"raw-grant-must-not-escape"}`)}
	writeFrame(t, client, prepareFailed)
	if ack := readCommandAckFor(t, client, prepareFailed.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "attach_rejected" {
		t.Fatalf("prepare-failed acknowledgement = %+v", ack)
	}
	if got := len(events.warmAttachRequests()); got != 0 {
		t.Fatalf("prepare failure wrote %d Store records", got)
	}
	issuer.setFailure(nil)
	discardsBeforeCommitFailure := issuer.discardCount()
	events.setCommitFailure(errors.New("commit failed"))
	commitFailed := &protocol.Command{CommandID: "attach-commit-failed", Type: protocol.CommandSessionAttach, SessionID: "ses_target", Payload: json.RawMessage(`{"grant":"raw-grant-must-not-escape"}`)}
	writeFrame(t, client, commitFailed)
	if ack := readCommandAckFor(t, client, commitFailed.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "attach_rejected" {
		t.Fatalf("commit-failed acknowledgement = %+v", ack)
	}
	if got := len(events.warmAttachRequests()); got != 0 {
		t.Fatalf("commit failure wrote %d Store records", got)
	}
	_ = readFrame(t, bootstrap).(*protocol.TargetJoinChallenge)
	if got := issuer.activationCount(); got != 0 {
		t.Fatalf("commit failure activated %d credential(s)", got)
	}
	if got := issuer.discardCount(); got != discardsBeforeCommitFailure+1 {
		t.Fatalf("commit failure discard count = %d, want %d", got, discardsBeforeCommitFailure+1)
	}
	events.setCommitFailure(nil)
	command := &protocol.Command{
		CommandID: "attach-command", Type: protocol.CommandSessionAttach, SessionID: "ses_target",
		Payload: json.RawMessage(`{"grant":"raw-grant-must-not-escape"}`),
	}
	writeFrame(t, client, command)
	challenge := readFrame(t, bootstrap).(*protocol.TargetJoinChallenge)
	target := dialWebSocket(t, server.URL)
	defer target.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, target, &protocol.TargetJoin{ProtocolVersion: protocol.ProtocolVersionV2, JoinNonce: challenge.JoinNonce})
	credential := readFrame(t, target).(*protocol.TargetJoinCredential)
	if credential.Credential == "" || credential.TargetSessionID != "ses_target" || credential.TargetCredentialLineageRef == "" || credential.Generation != 1 ||
		strings.Contains(fmt.Sprintf("%+v", credential), "raw-grant-must-not-escape") {
		t.Fatalf("target credential = %+v", credential)
	}
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckAccepted || ack.Reason != "" {
		t.Fatalf("attach acknowledgement = %+v", ack)
	}
	requests := events.warmAttachRequests()
	if len(requests) != 1 || requests[0].Attempt.Identity.TargetSessionID != "ses_target" ||
		requests[0].Attempt.IssuedCredentialGeneration == nil || *requests[0].Attempt.IssuedCredentialGeneration != 1 ||
		requests[0].TargetActivation.Generation != 1 || !requests[0].TargetActivation.ExpiresAt.Equal(requests[0].Attachment.ExpiresAt) ||
		requests[0].BootstrapAdmission.CredentialGeneration != 1 || requests[0].BootstrapAdmission.ConnectionEpoch <= 0 ||
		requests[0].BootstrapAdmission.AcceptedFence <= 0 || requests[0].BootstrapAdmission.GrantFence != fence ||
		requests[0].Attachment.Identity.TargetCredentialLineageRef == "" ||
		requests[0].FirstDelivery.ReferenceID != "attach_1" ||
		strings.Contains(fmt.Sprintf("%+v", requests[0]), "raw-grant-must-not-escape") {
		t.Fatalf("warm attach request = %+v", requests)
	}
	wrongTarget := *command
	wrongTarget.CommandID = "attach-wrong-target"
	wrongTarget.SessionID = "ses_other"
	writeFrame(t, client, &wrongTarget)
	if ack := readCommandAckFor(t, client, wrongTarget.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "unauthorized" {
		t.Fatalf("target substitution acknowledgement = %+v", ack)
	}
	malformed := *command
	malformed.CommandID = "attach-malformed"
	malformed.Payload = json.RawMessage(`{"grant":"raw-grant-must-not-escape","extra":true}`)
	writeFrame(t, client, &malformed)
	if ack := readCommandAckFor(t, client, malformed.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "invalid_command" {
		t.Fatalf("malformed attach acknowledgement = %+v", ack)
	}
	if got := len(events.warmAttachRequests()); got != 1 {
		t.Fatalf("rejected attaches wrote %d Store requests, want 1", got)
	}
	writeFrame(t, client, command)
	_ = readFrame(t, bootstrap).(*protocol.TargetJoinChallenge)
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckDuplicate || ack.Reason != "" {
		t.Fatalf("attach retry acknowledgement = %+v", ack)
	}
	if got := len(events.warmAttachRequests()); got != 2 {
		t.Fatalf("warm attach retries = %d, want 2 Store calls", got)
	}
	if frame, err := readFrameWithin(target, 100*time.Millisecond); err == nil {
		t.Fatalf("pending target socket survived credential delivery: %+v", frame)
	}
}

func TestWebSocketSessionAttachDoesNotHandoffAfterFinalTupleLoss(t *testing.T) {
	events := newRecordingWarmAttachStore()
	issuer := &recordingSessionCredentialIssuer{}
	verifier := &t18BAttachGrantVerifier{raw: "raw-grant", grant: auth.AttachGrant{
		Audience: "deploy-attach", JTI: "jti_final", AttachID: "attach_final", BootstrapSessionID: "ses_bootstrap",
		TargetSessionID: "ses_target", Provider: "claude-code", IssuedAt: time.Now().Add(-time.Second),
		ExpiresAt: time.Now().Add(time.Minute), DeliveryDeadline: time.Now().Add(30 * time.Second),
	}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: websocketTestAuth{principals: map[string]auth.Principal{
			"client-token":  {Subject: "client", Scopes: []auth.Scope{auth.SessionControl("ses_target")}},
			"adapter-token": {Subject: "adapter", Scopes: []auth.Scope{auth.SessionAdapter("ses_bootstrap")}},
		}, credentials: map[string]adapterCredentialEvidence{"adapter": {Generation: 1, ExpiresAt: time.Now().Add(time.Hour)}}},
		AttachGrantVerifier: verifier, AttachGrantAudience: "deploy-attach", EventStore: events,
	})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
		cfg.SessionCredentialIssuer = issuer
		cfg.SessionCredentialLifecycle = issuer
	})
	bootstrap := dialWebSocket(t, server.URL)
	defer bootstrap.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloVersionFor(t, bootstrap, "adapter-token", "ses_bootstrap", protocol.ProtocolVersionV2)
	_ = readFrame(t, bootstrap).(*protocol.HelloAck)
	fence, err := events.AllocateAdapterGrantFence(context.Background())
	if err != nil {
		t.Fatalf("allocate grant fence: %v", err)
	}
	verifier.grant.GrantFence = fence
	events.setAfterCommit(func(store *recordingWarmAttachStore) {
		store.mu.Lock()
		now := time.Now()
		connection := store.connections["ses_target"]
		connection.RevokedAt = &now
		connection.TerminalAt = &now
		store.connections["ses_target"] = connection
		store.mu.Unlock()
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_target"}}})
	_ = readFrame(t, client).(*protocol.HelloAck)
	command := &protocol.Command{CommandID: "attach-final-tuple-loss", Type: protocol.CommandSessionAttach, SessionID: "ses_target", Payload: json.RawMessage(`{"grant":"raw-grant"}`)}
	writeFrame(t, client, command)
	challenge := readFrame(t, bootstrap).(*protocol.TargetJoinChallenge)
	target := dialWebSocket(t, server.URL)
	defer target.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, target, &protocol.TargetJoin{ProtocolVersion: protocol.ProtocolVersionV2, JoinNonce: challenge.JoinNonce})
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "attach_rejected" {
		t.Fatalf("final tuple loss acknowledgement = %+v", ack)
	}
	if got := len(events.warmAttachRequests()); got != 1 {
		t.Fatalf("final tuple loss Store records = %d", got)
	}
	if frame, err := readFrameWithin(target, 100*time.Millisecond); err == nil {
		t.Fatalf("final tuple loss delivered credential: %+v", frame)
	}
}

func TestWebSocketSessionAttachRejectsDuplicateAfterUncertainCredentialHandoff(t *testing.T) {
	// Any unexpected pending-socket input makes delivery uncertain. A retry must
	// not turn that uncertainty into a duplicate-success acknowledgement.
	events := newRecordingWarmAttachStore()
	issuer := &recordingSessionCredentialIssuer{}
	verifier := &t18BAttachGrantVerifier{raw: "raw-grant-uncertain", grant: auth.AttachGrant{
		Audience: "deploy-attach", JTI: "jti_uncertain", AttachID: "attach_uncertain", BootstrapSessionID: "ses_bootstrap",
		TargetSessionID: "ses_target", Provider: "claude-code", IssuedAt: time.Now().Add(-time.Second),
		ExpiresAt: time.Now().Add(time.Minute), DeliveryDeadline: time.Now().Add(30 * time.Second),
	}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: websocketTestAuth{principals: map[string]auth.Principal{
		"client-token":  {Subject: "client", Scopes: []auth.Scope{auth.SessionControl("ses_target")}},
		"adapter-token": {Subject: "adapter", Scopes: []auth.Scope{auth.SessionAdapter("ses_bootstrap")}},
	}, credentials: map[string]adapterCredentialEvidence{"adapter": {Generation: 1, ExpiresAt: time.Now().Add(time.Hour)}}}, AttachGrantVerifier: verifier, AttachGrantAudience: "deploy-attach", EventStore: events})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.SessionCredentialIssuer, cfg.SessionCredentialLifecycle = events, issuer, issuer
	})
	bootstrap := dialWebSocket(t, server.URL)
	defer bootstrap.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloVersionFor(t, bootstrap, "adapter-token", "ses_bootstrap", protocol.ProtocolVersionV2)
	_ = readFrame(t, bootstrap).(*protocol.HelloAck)
	fence, err := events.AllocateAdapterGrantFence(context.Background())
	if err != nil {
		t.Fatalf("allocate grant fence: %v", err)
	}
	verifier.grant.GrantFence = fence
	commitEntered := make(chan struct{})
	commitRelease := make(chan struct{})
	events.setBeforeCommit(func() { close(commitEntered); <-commitRelease })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_target"}}})
	_ = readFrame(t, client).(*protocol.HelloAck)
	command := &protocol.Command{CommandID: "attach-uncertain", Type: protocol.CommandSessionAttach, SessionID: "ses_target", Payload: json.RawMessage(`{"grant":"raw-grant-uncertain"}`)}
	writeFrame(t, client, command)
	challenge := readFrame(t, bootstrap).(*protocol.TargetJoinChallenge)
	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		t.Fatal("warm attach did not reach commit gate")
	}
	target := dialWebSocket(t, server.URL)
	defer target.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, target, &protocol.TargetJoin{ProtocolVersion: protocol.ProtocolVersionV2, JoinNonce: challenge.JoinNonce})
	targetResult := make(chan error, 1)
	go func() {
		_, err := readFrameWithin(target, time.Second)
		targetResult <- err
	}()
	writeFrame(t, target, &protocol.Ping{Nonce: "injected"})
	if err := <-targetResult; err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending target socket did not close after injected input: %v", err)
	}
	close(commitRelease)
	events.setBeforeCommit(nil)
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "attach_rejected" {
		t.Fatalf("uncertain handoff acknowledgement = %+v", ack)
	}
	if got := issuer.activationCount(); got != 1 {
		t.Fatalf("uncertain delivery activated %d credentials, want one", got)
	}
}

func TestWebSocketServerNegotiatesV2WithoutHistoryCapability(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
	})
	ack := readFrame(t, conn).(*protocol.HelloAck)
	if ack.ProtocolVersion != protocol.ProtocolVersionV2 || ack.Capabilities != nil {
		t.Fatalf("v2 hello ack = %+v, want v2 with no capabilities", ack)
	}
	writeFrame(t, conn, &protocol.HistoryPageRequest{RequestID: "hist_unready", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, conn).(*protocol.Error); got.Code != "history_unsupported" {
		t.Fatalf("unready history error = %+v", got)
	}
}

func TestWebSocketServerAdvertisesAndServesAuthorizedHistory(t *testing.T) {
	t.Parallel()

	before := int64(58)
	next := int64(52)
	events := &fakeHistoryStore{
		fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 57}, nil),
		page: store.HistoryPage{
			Events:    []store.Event{{SessionID: "ses_1", Seq: 52, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)}},
			LatestSeq: 57, NextBeforeSeq: &next, RetentionState: store.RetentionGap,
		},
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "view-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
	})
	ack := readFrame(t, client).(*protocol.HelloAck)
	if ack.Capabilities == nil || ack.Capabilities.HistoryPage == nil || ack.Capabilities.HistoryPage.MaxLimit != 100 {
		t.Fatalf("history capability = %+v", ack.Capabilities)
	}
	writeFrame(t, client, &protocol.HistoryPageRequest{
		RequestID: "hist_1", SessionID: "ses_1", BeforeSeq: &before, Limit: 1,
	})
	response := readFrame(t, client).(*protocol.HistoryPageResponse)
	if response.RequestID != "hist_1" || response.SessionID != "ses_1" || response.LatestSeq != 57 ||
		response.NextBeforeSeq == nil || *response.NextBeforeSeq != next || len(response.Events) != 1 ||
		response.Events[0].Frame != protocol.FrameEvent || response.Events[0].Seq != 52 || response.RetentionState != store.RetentionGap {
		t.Fatalf("history response = %+v", response)
	}
	call := events.historyCall(t)
	if call.sessionID != "ses_1" || call.beforeSeq == nil || *call.beforeSeq != before || call.limit != 1 {
		t.Fatalf("history call = %+v", call)
	}

	control := dialWebSocket(t, server.URL)
	defer control.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, control, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
	})
	_ = readFrame(t, control).(*protocol.HelloAck)
	writeFrame(t, control, &protocol.HistoryPageRequest{RequestID: "hist_control", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, control).(*protocol.HistoryPageResponse); got.RequestID != "hist_control" {
		t.Fatalf("control history response = %+v", got)
	}
	if calls := events.historyCalls(); calls != 2 {
		t.Fatalf("history calls = %d, want 2", calls)
	}
}

func TestWebSocketServerHistoryAuthorizationAndAvailability(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		version     int
		token       string
		sessionID   string
		storeError  error
		wantCode    string
		helloDenied bool
	}{
		{name: "v1 unsupported", version: 1, token: "view-token", sessionID: "ses_1", wantCode: "history_unsupported"},
		{name: "api wildcard denied", version: 2, token: "api-token", sessionID: "ses_1", helloDenied: true},
		{name: "cross session denied", version: 2, token: "view-token", sessionID: "ses_2", wantCode: "history_unavailable"},
		{name: "store failure", version: 2, token: "view-token", sessionID: "ses_1", storeError: errors.New("private store failure"), wantCode: "history_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 0}, nil), err: test.storeError}
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
				cfg.EventStore = events
			})
			client := dialWebSocket(t, server.URL)
			defer client.Close(websocket.StatusNormalClosure, "")
			writeFrame(t, client, &protocol.Hello{
				ProtocolVersion: test.version, Role: protocol.RoleClient, Token: test.token,
				Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
			})
			first := readFrame(t, client)
			if test.helloDenied {
				if got := first.(*protocol.Error); got.Code != "unauthorized" || events.historyCalls() != 0 {
					t.Fatalf("hello error = %+v, history calls = %d", got, events.historyCalls())
				}
				return
			}
			_ = first.(*protocol.HelloAck)
			writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_denied", SessionID: test.sessionID, Limit: 1})
			got := readFrame(t, client).(*protocol.Error)
			if got.Code != test.wantCode || got.Message == "private store failure" {
				t.Fatalf("history error = %+v", got)
			}
			if test.name != "store failure" && events.historyCalls() != 0 {
				t.Fatalf("denied history reached store")
			}
			writeFrame(t, client, &protocol.Ping{Nonce: "after-history-error"})
			if pong := readFrame(t, client).(*protocol.Pong); pong.Nonce != "after-history-error" {
				t.Fatalf("pong after history error = %+v", pong)
			}
		})
	}
}

func TestWebSocketServerRejectsUntrustedHistoryPages(t *testing.T) {
	t.Parallel()
	event := func(sessionID string, seq int64) store.Event {
		return store.Event{SessionID: sessionID, Seq: seq, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)}
	}
	typedEvent := func(eventType string) store.Event { ev := event("ses_1", 1); ev.Type = eventType; return ev }
	oversizedPayload := json.RawMessage(`"` + strings.Repeat("a", 64*1024) + `"`)
	nextTwo := int64(2)
	for _, test := range []struct {
		name   string
		page   store.HistoryPage
		before *int64
		limit  int
	}{
		{name: "cross session", page: store.HistoryPage{Events: []store.Event{event("ses_2", 1)}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "oversized", page: store.HistoryPage{Events: []store.Event{event("ses_1", 1), event("ses_1", 2)}, LatestSeq: 2, RetentionState: store.RetentionComplete}},
		{name: "nonpositive sequence", page: store.HistoryPage{Events: []store.Event{event("ses_1", 0)}, RetentionState: store.RetentionComplete}},
		{name: "descending sequence", page: store.HistoryPage{Events: []store.Event{event("ses_1", 2), event("ses_1", 1)}, LatestSeq: 2, RetentionState: store.RetentionComplete}},
		{name: "cursor violation", page: store.HistoryPage{Events: []store.Event{event("ses_1", 2)}, LatestSeq: 2, RetentionState: store.RetentionComplete}, before: &nextTwo},
		{name: "latest below event", page: store.HistoryPage{Events: []store.Event{event("ses_1", 2)}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "inconsistent next", page: store.HistoryPage{Events: []store.Event{event("ses_1", 1)}, LatestSeq: 2, NextBeforeSeq: &nextTwo, RetentionState: store.RetentionComplete}},
		{name: "invalid retention", page: store.HistoryPage{LatestSeq: 1, RetentionState: "unknown"}},
		{name: "invalid payload", page: store.HistoryPage{Events: []store.Event{{SessionID: "ses_1", Seq: 1, Type: "x", Payload: json.RawMessage(`{`)}}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "oversized payload", page: store.HistoryPage{Events: []store.Event{{SessionID: "ses_1", Seq: 1, Type: "x", Payload: oversizedPayload}}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "omitted complete cursor", page: store.HistoryPage{Events: []store.Event{event("ses_1", 4), event("ses_1", 5)}, LatestSeq: 5, RetentionState: store.RetentionComplete}, limit: 2},
		{name: "underfilled complete page", page: store.HistoryPage{Events: []store.Event{event("ses_1", 5)}, LatestSeq: 5, RetentionState: store.RetentionComplete}, limit: 2},
		{name: "durable activity", page: store.HistoryPage{Events: []store.Event{typedEvent("agent.activity")}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "durable log tail", page: store.HistoryPage{Events: []store.Event{typedEvent("log.tail")}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "durable legacy warning", page: store.HistoryPage{Events: []store.Event{typedEvent("session.idle_warning")}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
		{name: "durable v2 warning", page: store.HistoryPage{Events: []store.Event{typedEvent("x.vm.idle_warning")}, LatestSeq: 1, RetentionState: store.RetentionComplete}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 2}, nil), page: test.page}
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
				cfg.EventStore = events
				cfg.EphemeralEventVariants = map[string]map[int]string{
					"session.idle_warning": {protocol.ProtocolVersion: "session.idle_warning"},
					"x.vm.idle_warning":    {protocol.ProtocolVersionV2: "x.vm.idle_warning"},
				}
			})
			client := dialWebSocket(t, server.URL)
			defer client.Close(websocket.StatusNormalClosure, "")
			writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "view-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
			_ = readFrame(t, client).(*protocol.HelloAck)
			limit := test.limit
			if limit == 0 {
				limit = 1
			}
			writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_bad", SessionID: "ses_1", BeforeSeq: test.before, Limit: limit})
			if got := readFrame(t, client).(*protocol.Error); got.Code != "history_unavailable" {
				t.Fatalf("history error = %+v", got)
			}
		})
	}
}

func TestWebSocketServerReauthenticatesHistoryBeforeAndAfterStore(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		validCalls int
		storeCalls int
	}{
		{name: "expired before request", validCalls: 1, storeCalls: 0},
		{name: "revoked during read", validCalls: 2, storeCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			events := &fakeHistoryStore{
				fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 1}, nil),
				page:           store.HistoryPage{Events: []store.Event{{SessionID: "ses_1", Seq: 1, Type: "x", Payload: json.RawMessage(`{}`)}}, LatestSeq: 1, RetentionState: store.RetentionComplete},
			}
			authenticator := &boundedWebsocketAuth{validCalls: test.validCalls, principal: auth.Principal{Subject: "viewer", Scopes: []auth.Scope{auth.SessionView("ses_1")}}}
			handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: authenticator, EventStore: events})
			server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
			client := dialWebSocket(t, server.URL)
			defer client.Close(websocket.StatusNormalClosure, "")
			writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "expiring-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
			_ = readFrame(t, client).(*protocol.HelloAck)
			writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_expired", SessionID: "ses_1", Limit: 1})
			if got := readFrame(t, client).(*protocol.Error); got.Code != "history_unavailable" || events.historyCalls() != test.storeCalls {
				t.Fatalf("history error = %+v, store calls = %d", got, events.historyCalls())
			}
		})
	}
}

func TestWebSocketServerRejectsReboundHistoryPrincipal(t *testing.T) {
	t.Parallel()
	events := &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 1}, nil), page: store.HistoryPage{Events: []store.Event{{SessionID: "ses_1", Seq: 1, Type: "x", Payload: json.RawMessage(`{}`)}}, LatestSeq: 1, RetentionState: store.RetentionComplete}}
	authenticator := &reboundWebsocketAuth{principals: []auth.Principal{
		{Subject: "viewer-a", Scopes: []auth.Scope{auth.SessionView("ses_1")}},
		{Subject: "viewer-b", Scopes: []auth.Scope{auth.SessionView("ses_1")}},
	}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: authenticator, EventStore: events})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "rebound-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_rebound", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, client).(*protocol.Error); got.Code != "history_unavailable" || events.historyCalls() != 0 {
		t.Fatalf("history error = %+v, store calls = %d", got, events.historyCalls())
	}
}

func TestWebSocketServerRejectsAdapterHistoryRequest(t *testing.T) {
	t.Parallel()

	events := &fakeHistoryStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 0}, nil)}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	ack := readFrame(t, adapter).(*protocol.HelloAck)
	if ack.Capabilities != nil {
		t.Fatalf("adapter capabilities = %+v", ack.Capabilities)
	}
	writeFrame(t, adapter, &protocol.HistoryPageRequest{RequestID: "hist_adapter", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, adapter).(*protocol.Error); got.Code != "history_unsupported" {
		t.Fatalf("adapter history error = %+v", got)
	}
	if events.historyCalls() != 0 {
		t.Fatal("adapter history request reached store")
	}
}

func TestWebSocketServerAttachOnlyDeniesReadReplayLiveAndCommands(t *testing.T) {
	t.Parallel()
	base := newFakeEventStore(map[string]int64{"ses_1": 0}, map[string][]store.Event{
		"ses_1": {{SessionID: "ses_1", Seq: 1, Type: "session.message", Payload: json.RawMessage(`{}`)}},
	})
	base.setAdmissionTruth("ses_1", store.SessionAdmissionTruth{SessionID: "ses_1"})
	events := &fakeHistoryStore{fakeEventStore: base, page: store.HistoryPage{RetentionState: store.RetentionComplete}}
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: testHandshakeWithStore(events), EventStore: events})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	ack := readFrame(t, client).(*protocol.HelloAck)
	if ack.Sessions[0].State != "attach_only" || base.replayCallCount() != 0 {
		t.Fatalf("attach-only ack = %+v, replay calls = %d", ack.Sessions[0], base.replayCallCount())
	}
	if ack.Capabilities != nil {
		t.Fatalf("attach-only capabilities = %+v, want nil", ack.Capabilities)
	}
	writeFrame(t, client, &protocol.HistoryPageRequest{RequestID: "hist_attach_only", SessionID: "ses_1", Limit: 1})
	if got := readFrame(t, client).(*protocol.Error); got.Code != "history_unavailable" || events.historyCalls() != 0 {
		t.Fatalf("history error = %+v, store calls = %d", got, events.historyCalls())
	}
	writeFrame(t, client, &protocol.Command{CommandID: "cmd_attach_only", Type: protocol.CommandSessionSend, SessionID: "ses_1", Payload: json.RawMessage(`{"content":[{"kind":"text","text":"blocked"}]}`)})
	if got := readFrame(t, client).(*protocol.CommandAck); got.Status != protocol.AckRejected || len(base.appended()) != 0 {
		t.Fatalf("command ack = %+v, appends = %d", got, len(base.appended()))
	}
	if err := handler.EmitEphemeralEvent(context.Background(), protocol.Event{Type: "log.tail", SessionID: "ses_1", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("EmitEphemeralEvent() error = %v", err)
	}
	if frame, err := readFrameWithin(client, 80*time.Millisecond); err == nil {
		t.Fatalf("attach-only client received live frame %+v", frame)
	}
}

func TestWebSocketServerReplaysEventsAfterHelloAck(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 4}, map[string][]store.Event{
		"ses_1": {
			{SessionID: "ses_1", Seq: 1, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)},
			{SessionID: "ses_1", Seq: 3, Type: "session.message", Time: time.UnixMilli(1003), Payload: json.RawMessage(`{"n":3}`)},
			{SessionID: "ses_1", Seq: 4, Type: "session.state", Time: time.UnixMilli(1004), Payload: json.RawMessage(`{"state":"ready"}`)},
		},
	})
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1", LastSeq: 2}},
	})
	if ack := readFrame(t, conn).(*protocol.HelloAck); ack.Sessions[0].ReplayFrom != 3 {
		t.Fatalf("hello ack = %+v", ack)
	}
	first := readFrame(t, conn).(*protocol.Event)
	second := readFrame(t, conn).(*protocol.Event)
	if first.SessionID != "ses_1" || first.Seq == nil || *first.Seq != 3 ||
		first.Type != "session.message" || first.Time != 1003 || string(first.Payload) != `{"n":3}` {
		t.Fatalf("first replay event = %+v payload=%s", first, string(first.Payload))
	}
	if second.SessionID != "ses_1" || second.Seq == nil || *second.Seq != 4 ||
		second.Type != "session.state" || second.Time != 1004 {
		t.Fatalf("second replay event = %+v", second)
	}
}

func TestWebSocketServerDoesNotReplayV2IdleWarning(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 2}, map[string][]store.Event{
		"ses_1": {
			{SessionID: "ses_1", Seq: 1, Type: "x.vm.idle_warning", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{}`)},
			{SessionID: "ses_1", Seq: 2, Type: "session.message", Time: time.UnixMilli(1002), Payload: json.RawMessage(`{}`)},
		},
	})
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
		cfg.EphemeralEventVariants = map[string]map[int]string{
			"x.vm.idle_warning": {protocol.ProtocolVersionV2: "x.vm.idle_warning"},
		}
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token",
		Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}},
	})
	_ = readFrame(t, client).(*protocol.HelloAck)
	if event := readFrame(t, client).(*protocol.Event); event.Type != "session.message" || event.Seq == nil || *event.Seq != 2 {
		t.Fatalf("replayed event = %+v, want session.message seq=2", event)
	}
}

func TestWebSocketServerPersistsAdapterDurableEventBeforeFanout(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2001,
		Payload:   json.RawMessage(`{"role":"agent"}`),
	})

	ev := readFrame(t, client).(*protocol.Event)
	if ev.SessionID != "ses_1" || ev.Type != "session.message" || ev.Seq == nil || *ev.Seq != 1 ||
		ev.Time != 2001 || string(ev.Payload) != `{"role":"agent"}` {
		t.Fatalf("fanout event = %+v payload=%s", ev, string(ev.Payload))
	}
	calls := events.appended()
	if len(calls) != 1 || calls[0].sessionID != "ses_1" || len(calls[0].events) != 1 ||
		calls[0].events[0].Type != "session.message" {
		t.Fatalf("append calls = %+v", calls)
	}
}

func TestWebSocketServerBatchesAdapterDurableEvents(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2001,
		Payload:   json.RawMessage(`{"n":1}`),
	})
	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"n":2}`),
	})

	first := readFrame(t, client).(*protocol.Event)
	second := readFrame(t, client).(*protocol.Event)
	if first.Seq == nil || *first.Seq != 1 || string(first.Payload) != `{"n":1}` {
		t.Fatalf("first fanout event = %+v payload=%s", first, string(first.Payload))
	}
	if second.Seq == nil || *second.Seq != 2 || string(second.Payload) != `{"n":2}` {
		t.Fatalf("second fanout event = %+v payload=%s", second, string(second.Payload))
	}

	calls := events.appended()
	if len(calls) != 1 || calls[0].sessionID != "ses_1" || len(calls[0].events) != 2 {
		t.Fatalf("append calls = %+v, want one batch of two events", calls)
	}
}

func TestWebSocketServerDoesNotFanoutDurableEventWhenPersistenceFails(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	events.setAppendError(errors.New("disk full"))
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2001,
		Payload:   json.RawMessage(`{"role":"agent"}`),
	})

	errFrame := readFrame(t, adapter).(*protocol.Error)
	if errFrame.Code != "persist_failed" || errFrame.Fatal {
		t.Fatalf("adapter error = %+v", errFrame)
	}
	if frame, err := readFrameWithin(client, 80*time.Millisecond); err == nil {
		t.Fatalf("client unexpectedly received frame %+v", frame)
	}
}

func TestWebSocketServerBroadcastsAdapterEphemeralEventWithoutPersistence(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "log.tail",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"line":"hello"}`),
	})

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Seq != nil || ev.Type != "log.tail" || ev.Time != 2002 || string(ev.Payload) != `{"line":"hello"}` {
		t.Fatalf("ephemeral fanout event = %+v payload=%s", ev, string(ev.Payload))
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("ephemeral event was persisted: %+v", calls)
	}
}

func TestWebSocketServerEmitsIdleWarningFromHostWithoutPersistence(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{
		Handshake:              testHandshakeWithStore(events),
		EventStore:             events,
		EphemeralEventVariants: map[string]map[int]string{"session.idle_warning": {protocol.ProtocolVersion: "session.idle_warning"}},
	})
	broadcaster, ok := handler.(hub.EphemeralBroadcaster)
	if !ok {
		t.Fatalf("handler does not implement EphemeralBroadcaster")
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)

	payload := json.RawMessage(`{"vm_id":"vm_1","seconds_until_suspend":600,"suspend_at":"2026-06-21T12:30:00Z","message":"This VM will pause soon because it has been idle."}`)
	if err := broadcaster.EmitEphemeralEvent(context.Background(), protocol.Event{
		Type:      "session.idle_warning",
		SessionID: "ses_1",
		Time:      2003,
		Payload:   payload,
	}); err != nil {
		t.Fatalf("emit idle warning: %v", err)
	}

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Seq != nil || ev.Type != "session.idle_warning" || ev.Time != 2003 ||
		string(ev.Payload) != string(payload) {
		t.Fatalf("idle warning event = %+v payload=%s", ev, string(ev.Payload))
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("idle warning event was persisted: %+v", calls)
	}
}

func TestWebSocketServerSelectsPublisherEphemeralVariantByNegotiatedVersion(t *testing.T) {
	t.Parallel()

	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{
		Handshake: testHandshake(),
		EphemeralEventVariants: map[string]map[int]string{
			"publisher.notice": {
				protocol.ProtocolVersionV2: "publisher.notice.current",
			},
		},
	})
	broadcaster := handler.(hub.EphemeralBroadcaster)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	v1Client := dialWebSocket(t, server.URL)
	defer v1Client.Close(websocket.StatusNormalClosure, "")
	v2Client := dialWebSocket(t, server.URL)
	defer v2Client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, v1Client, "client-token", 0)
	_ = readFrame(t, v1Client).(*protocol.HelloAck)
	writeFrame(t, v2Client, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersionV2,
		Role:            protocol.RoleClient,
		Token:           "client-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
	})
	_ = readFrame(t, v2Client).(*protocol.HelloAck)

	if err := broadcaster.EmitEphemeralEvent(context.Background(), protocol.Event{
		Type: "publisher.notice", SessionID: "ses_1", Time: 2003,
		Payload: json.RawMessage(`{"message":"opaque warning"}`),
	}); err != nil {
		t.Fatalf("emit publisher event: %v", err)
	}
	if frame := readFrame(t, v1Client).(*protocol.Event); frame.Type != "publisher.notice" || frame.Seq != nil {
		t.Fatalf("v1 publisher event = %+v", frame)
	}
	if frame := readFrame(t, v2Client).(*protocol.Event); frame.Type != "publisher.notice.current" || frame.Seq != nil {
		t.Fatalf("v2 publisher event = %+v", frame)
	}
}

func TestWebSocketServerRejectsPublisherEphemeralTypesFromAdapter(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
		cfg.EphemeralEventVariants = map[string]map[int]string{
			"publisher.notice": {protocol.ProtocolVersionV2: "publisher.notice.current"},
		}
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	for _, eventType := range []string{"publisher.notice", "publisher.notice.current"} {
		writeFrame(t, adapter, &protocol.Event{
			Type: eventType, SessionID: "ses_1", Time: 2003, Payload: json.RawMessage(`{}`),
		})
		if frame := readFrame(t, adapter).(*protocol.Error); frame.Code != "invalid_event" {
			t.Fatalf("adapter error for %q = %+v, want invalid_event", eventType, frame)
		}
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("publisher ephemeral type was persisted: %+v", calls)
	}
}

func TestWebSocketServerBroadcastsEphemeralEventWithoutPersistence(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, adapter, &protocol.Event{
		Type:      "log.tail",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"line":"hello"}`),
	})

	ev := readFrame(t, client).(*protocol.Event)
	if ev.Type != "log.tail" || ev.Seq != nil {
		t.Fatalf("ephemeral fanout event = %+v", ev)
	}
}

func TestWebSocketServerPersistsAndRoutesClientSessionSend(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	observer := dialWebSocket(t, server.URL)
	defer observer.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeClientHello(t, observer, "client-token", 0)
	_ = readFrame(t, observer).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_send_1",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"Continue"}]}`),
	})

	observerEvent := readFrame(t, observer).(*protocol.Event)
	if observerEvent.Type != "session.message" || observerEvent.Seq == nil || *observerEvent.Seq != 1 {
		t.Fatalf("observer event = %+v", observerEvent)
	}
	var message struct {
		MessageID string `json:"message_id"`
		Role      string `json:"role"`
		Content   []struct {
			Kind string `json:"kind"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(observerEvent.Payload, &message); err != nil {
		t.Fatalf("decode session.message payload: %v", err)
	}
	if message.MessageID != "cmd_send_1" || message.Role != "user" ||
		len(message.Content) != 1 || message.Content[0].Text != "Continue" {
		t.Fatalf("session.message payload = %+v", message)
	}

	routed := readFrame(t, adapter).(*protocol.Command)
	if routed.CommandID != "cmd_send_1" || routed.Type != protocol.CommandSessionSend ||
		string(routed.Payload) != `{"content":[{"kind":"text","text":"Continue"}]}` {
		t.Fatalf("routed command = %+v payload=%s", routed, string(routed.Payload))
	}

	ack := readCommandAckFor(t, client, "cmd_send_1")
	if ack.Status != protocol.AckAccepted || ack.Reason != "" {
		t.Fatalf("client ack = %+v", ack)
	}
	calls := events.appended()
	if len(calls) != 1 || calls[0].sessionID != "ses_1" ||
		calls[0].events[0].Type != "session.message" {
		t.Fatalf("append calls = %+v", calls)
	}
}

func TestWebSocketServerRoutesFileReferenceThroughDurableLedger(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "file-references.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	events := &settingsWebSocketStore{Store: ledger}
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_1", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersionV2, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	if ack := readFrame(t, client).(*protocol.HelloAck); ack.Capabilities == nil || ack.Capabilities.FileReferences == nil {
		t.Fatalf("file-reference hello capability = %+v", ack.Capabilities)
	}
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	payload := fileReferenceSendPayload(t)
	writeFrame(t, client, &protocol.Command{CommandID: "cmd_file_missing", Type: protocol.CommandSessionSend, SessionID: "ses_1", Payload: payload})
	if ack := readCommandAckFor(t, client, "cmd_file_missing"); ack.Status != protocol.AckRejected || ack.Reason != "file_references_unsupported" {
		t.Fatalf("missing capability ack = %+v", ack)
	}
	if latest, err := events.LatestSeq(ctx, "ses_1"); err != nil || latest != 0 {
		t.Fatalf("missing capability durable state = %d, %v", latest, err)
	}

	capability := fileReferenceCapability()
	publishFileReferenceCapability(t, adapter, "file_reference_capability", 1001, capability)
	_ = readFrame(t, client).(*protocol.Event)
	stalePayload := json.RawMessage(strings.Replace(string(payload), capability.Fingerprint, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1))
	writeFrame(t, client, &protocol.Command{CommandID: "cmd_file_stale", Type: protocol.CommandSessionSend, SessionID: "ses_1", Payload: stalePayload})
	if ack := readCommandAckFor(t, client, "cmd_file_stale"); ack.Status != protocol.AckRejected || ack.Reason != "file_references_unsupported" {
		t.Fatalf("stale capability ack = %+v", ack)
	}
	if latest, err := events.LatestSeq(ctx, "ses_1"); err != nil || latest != 1 {
		t.Fatalf("stale capability durable state = %d, %v", latest, err)
	}
	writeFrame(t, client, &protocol.Command{CommandID: "cmd_file_1", Type: protocol.CommandSessionSend, SessionID: "ses_1", Payload: payload})
	routed := readFrame(t, adapter).(*protocol.Command)
	if routed.CommandID != "cmd_file_1" || !strings.Contains(string(routed.Payload), `"message_id":"cmd_file_1"`) {
		t.Fatalf("routed file-reference command = %+v payload=%s", routed, routed.Payload)
	}
	if ack := readCommandAckFor(t, client, "cmd_file_1"); ack.Status != protocol.AckAccepted {
		t.Fatalf("file-reference ack = %+v", ack)
	}
	command, err := events.FileReferenceCommand(ctx, "ses_1", "cmd_file_1")
	if err != nil || command.Status != store.FileReferencePending || command.MessageID != "cmd_file_1" || command.ReferenceCount != 1 || command.Writer == nil {
		t.Fatalf("file-reference ledger = %+v, %v", command, err)
	}
	encoded, err := json.Marshal(command)
	if err != nil || strings.Contains(string(encoded), "src/app.ts") || strings.Contains(string(encoded), "0123456789abcdef") || strings.Contains(string(encoded), "text/plain") || strings.Contains(string(encoded), "Bytes") {
		t.Fatalf("file-reference ledger retained sensitive metadata: %s (%v)", encoded, err)
	}
	writeFrame(t, adapter, &protocol.Event{Type: "session.file_references.outcome", SessionID: "ses_1", Time: 1002, ProposalID: "file_reference_outcome", Payload: json.RawMessage(`{"message_id":"cmd_file_1","cmd_id":"cmd_file_1","outcome":"delivered","reference_index":null,"reason":null}`)})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != "file_reference_outcome" || receipt.Seq != 3 {
		t.Fatalf("file-reference outcome receipt = %+v", receipt)
	}
	if event := readFrame(t, client).(*protocol.Event); event.Type != "session.file_references.outcome" || !strings.Contains(string(event.Payload), `"outcome":"delivered"`) {
		t.Fatalf("file-reference outcome event = %+v", event)
	}
}

func TestWebSocketServerRejectsV1FileReferenceWithoutDurableMutation(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.Command{CommandID: "cmd_file_v1", Type: protocol.CommandSessionSend, SessionID: "ses_1", Payload: fileReferenceSendPayload(t)})
	if ack := readCommandAckFor(t, client, "cmd_file_v1"); ack.Status != protocol.AckRejected || ack.Reason != "file_references_unsupported" {
		t.Fatalf("v1 file-reference ack = %+v", ack)
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("v1 file-reference reached durable Store: %+v", calls)
	}
}

func fileReferenceCapability() protocol.FileReferenceCapabilityPayload {
	capability := protocol.FileReferenceCapabilityPayload{
		SchemaVersion: 1, MaxReferences: 8, MaxTotalBytes: 10485760,
		File:  protocol.FileReferenceDispositionCapability{Mode: "allowed", MaxBytes: fileReferenceInt64(10485760)},
		Image: protocol.FileReferenceImageCapability{Mode: "unsupported", MediaTypes: []string{}, Reason: fileReferenceString("provider_unsupported")},
	}
	capability.Fingerprint = protocol.FileReferenceCapabilityFingerprint(capability)
	return capability
}

func fileReferenceSendPayload(t *testing.T) json.RawMessage {
	t.Helper()
	capability := fileReferenceCapability()
	return json.RawMessage(fmt.Sprintf(`{"content":[{"kind":"text","text":"Review this"},{"kind":"file_reference","disposition":"file","path":"src/app.ts","version":"version_1","content_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","bytes":123,"media_type":"text/plain"}],"capability_fingerprint":%q}`, capability.Fingerprint))
}

func fileReferenceInt64(value int64) *int64 { return &value }

func fileReferenceString(value string) *string { return &value }

func publishFileReferenceCapability(t *testing.T, adapter *websocket.Conn, proposalID string, at int64, capability protocol.FileReferenceCapabilityPayload) {
	t.Helper()
	payload, err := json.Marshal(capability)
	if err != nil {
		t.Fatal(err)
	}
	writeFrame(t, adapter, &protocol.Event{Type: "session.file_references.capabilities", SessionID: "ses_1", Time: at, ProposalID: proposalID, Payload: payload})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != proposalID {
		t.Fatalf("file-reference capability receipt = %+v", receipt)
	}
}

func TestWebSocketServerRejectsUnauthorizedClientCommand(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "view-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_unauthorized",
		Type:      protocol.CommandSessionInterrupt,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{}`),
	})

	ack := readFrame(t, client).(*protocol.CommandAck)
	if ack.CommandID != "cmd_unauthorized" || ack.Status != protocol.AckRejected || ack.Reason != "unauthorized" {
		t.Fatalf("client ack = %+v", ack)
	}
}

func TestWebSocketServerRejectsSettingsChangeWithoutLiteralControlScope(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "view-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_settings_view_only",
		Type:      protocol.CommandType("session.settings.change"),
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"capability_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","model_id":"reasoning"}`),
	})

	ack := readFrame(t, client).(*protocol.CommandAck)
	if ack.CommandID != "cmd_settings_view_only" || ack.Status != protocol.AckRejected || ack.Reason != "unauthorized" {
		t.Fatalf("settings client ack = %+v", ack)
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("settings command persisted before literal control authorization: %+v", calls)
	}
}

func TestWebSocketServerRejectsV1SettingsWithLiteralReason(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersion, Role: protocol.RoleClient, Token: "client-token", Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	if ack := readFrame(t, client).(*protocol.HelloAck); ack.Capabilities != nil && ack.Capabilities.Settings != nil {
		t.Fatalf("v1 hello acknowledged Settings capability: %+v", ack.Capabilities)
	}
	command := &protocol.Command{CommandID: "cmd_settings_v1", Type: protocol.CommandSettingsChange, SessionID: "ses_1", Payload: json.RawMessage(`{"capability_fingerprint":"sha256:a77186c8bf756736dc64be46864c21e4b10fd8ad8d719abf2e00dfa51c341000","model_id":"reasoning"}`)}
	writeFrame(t, client, command)
	if ack := readCommandAckFor(t, client, command.CommandID); ack.Status != protocol.AckRejected || ack.Reason != "settings_unsupported" {
		t.Fatalf("v1 Settings acknowledgement = %+v", ack)
	}
	if calls := events.appended(); len(calls) != 0 {
		t.Fatalf("v1 Settings command reached the Store: %+v", calls)
	}
}

func TestWebSocketServerPersistsPermissionDecisionAndDeduplicatesRequest(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	observer := dialWebSocket(t, server.URL)
	defer observer.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeClientHello(t, observer, "client-token", 0)
	_ = readFrame(t, observer).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	decisionPayload := json.RawMessage(`{"request_id":"pr_1","decision":"approve","decided_by":"usr_1","note":""}`)
	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_decide_1",
		Type:      protocol.CommandPermissionRespond,
		SessionID: "ses_1",
		Payload:   decisionPayload,
	})

	decision := readFrame(t, observer).(*protocol.Event)
	if decision.Type != "permission.decision" || decision.Seq == nil || *decision.Seq != 1 ||
		string(decision.Payload) != string(decisionPayload) {
		t.Fatalf("permission decision event = %+v payload=%s", decision, string(decision.Payload))
	}
	routed := readFrame(t, adapter).(*protocol.Command)
	if routed.CommandID != "cmd_decide_1" || routed.Type != protocol.CommandPermissionRespond ||
		string(routed.Payload) != string(decisionPayload) {
		t.Fatalf("routed permission command = %+v payload=%s", routed, string(routed.Payload))
	}
	if ack := readCommandAckFor(t, client, "cmd_decide_1"); ack.Status != protocol.AckAccepted {
		t.Fatalf("permission ack = %+v", ack)
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_decide_2",
		Type:      protocol.CommandPermissionRespond,
		SessionID: "ses_1",
		Payload:   decisionPayload,
	})
	duplicate := readFrame(t, client).(*protocol.CommandAck)
	if duplicate.CommandID != "cmd_decide_2" || duplicate.Status != protocol.AckDuplicate {
		t.Fatalf("duplicate decision ack = %+v", duplicate)
	}
	if frame, err := readFrameWithin(adapter, 80*time.Millisecond); err == nil {
		t.Fatalf("adapter unexpectedly received duplicate decision %+v", frame)
	}
	calls := events.appended()
	if len(calls) != 1 || calls[0].events[0].Type != "permission.decision" {
		t.Fatalf("append calls = %+v", calls)
	}
}

func TestWebSocketServerDeduplicatesAcceptedClientCommands(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	command := &protocol.Command{
		CommandID: "cmd_duplicate",
		Type:      protocol.CommandSessionInterrupt,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{}`),
	}
	writeFrame(t, client, command)
	first := readFrame(t, adapter).(*protocol.Command)
	if first.CommandID != "cmd_duplicate" {
		t.Fatalf("first routed command = %+v", first)
	}
	if ack := readFrame(t, client).(*protocol.CommandAck); ack.Status != protocol.AckAccepted {
		t.Fatalf("first ack = %+v", ack)
	}

	writeFrame(t, client, command)
	duplicate := readFrame(t, client).(*protocol.CommandAck)
	if duplicate.CommandID != "cmd_duplicate" || duplicate.Status != protocol.AckDuplicate {
		t.Fatalf("duplicate ack = %+v", duplicate)
	}
	if frame, err := readFrameWithin(adapter, 80*time.Millisecond); err == nil {
		t.Fatalf("adapter unexpectedly received duplicate frame %+v", frame)
	}
}

func TestWebSocketServerReportsAcceptedClientCommandActivity(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	observer := &recordingCommandActivityObserver{}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
		cfg.CommandActivityObserver = observer
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_activity",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"hello"}]}`),
	})
	ack := readCommandAckFor(t, client, "cmd_activity")
	if ack.Status != protocol.AckAccepted {
		t.Fatalf("ack = %+v", ack)
	}
	if len(observer.got) != 1 {
		t.Fatalf("observed activity count = %d, want 1", len(observer.got))
	}
	got := observer.got[0]
	if got.SessionID != "ses_1" || got.CommandID != "cmd_activity" || got.Type != protocol.CommandSessionSend ||
		got.DurableSeq == nil || *got.DurableSeq != 1 || got.At.IsZero() {
		t.Fatalf("observed activity = %+v", got)
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_rejected",
		Type:      protocol.CommandSessionInterrupt,
		SessionID: "ses_other",
		Payload:   json.RawMessage(`{}`),
	})
	rejected := readCommandAckFor(t, client, "cmd_rejected")
	if rejected.Status != protocol.AckRejected || rejected.Reason != "unauthorized" {
		t.Fatalf("rejected ack = %+v", rejected)
	}
	if len(observer.got) != 1 {
		t.Fatalf("observed rejected command activity = %+v", observer.got)
	}

	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_activity",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"hello"}]}`),
	})
	duplicate := readCommandAckFor(t, client, "cmd_activity")
	if duplicate.Status != protocol.AckDuplicate {
		t.Fatalf("duplicate ack = %+v", duplicate)
	}
	if len(observer.got) != 1 {
		t.Fatalf("observed duplicate command activity = %+v", observer.got)
	}
}

func TestWebSocketServerReportsAdapterActivity(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	observer := &recordingAdapterActivityObserver{}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.AdapterActivityObserver = events, observer
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	if got := waitAdapterActivityCount(t, observer, 1); len(got) != 1 || got[0].SessionID != "ses_1" || got[0].At.IsZero() {
		t.Fatalf("hello adapter activity = %+v", got)
	}

	writeFrame(t, adapter, &protocol.Ping{Nonce: "adapter-ping"})
	pong := readFrame(t, adapter).(*protocol.Pong)
	if pong.Nonce != "adapter-ping" {
		t.Fatalf("pong nonce = %q", pong.Nonce)
	}
	if got := waitAdapterActivityCount(t, observer, 2); len(got) != 2 || got[1].SessionID != "ses_1" || got[1].At.IsZero() {
		t.Fatalf("ping adapter activity = %+v", got)
	}

	writeFrame(t, adapter, &protocol.Event{
		Type:      "log.tail",
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"line":"ready"}`),
	})
	if got := waitAdapterActivityCount(t, observer, 3); got[2].SessionID != "ses_1" || got[2].At.IsZero() {
		t.Fatalf("event adapter activity = %+v", got)
	}
}

func TestWebSocketServerBuffersSessionSendUntilAdapterReconnects(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.Command{
		CommandID: "cmd_buffered",
		Type:      protocol.CommandSessionSend,
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[{"kind":"text","text":"Buffered"}]}`),
	})

	ack := readCommandAckFor(t, client, "cmd_buffered")
	if ack.Status != protocol.AckAccepted {
		t.Fatalf("client ack = %+v", ack)
	}

	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	routed := readFrame(t, adapter).(*protocol.Command)
	if routed.CommandID != "cmd_buffered" || routed.Type != protocol.CommandSessionSend {
		t.Fatalf("buffered command = %+v", routed)
	}
}

func TestWebSocketServerBuffersLiveEventsUntilReplayCompletes(t *testing.T) {
	t.Parallel()

	events := newFakeEventStore(map[string]int64{"ses_1": 1}, map[string][]store.Event{
		"ses_1": {
			{SessionID: "ses_1", Seq: 1, Type: "session.message", Time: time.UnixMilli(1001), Payload: json.RawMessage(`{"n":1}`)},
		},
	})
	replayStarted := make(chan struct{})
	releaseReplay := make(chan struct{})
	events.onReplayEvent = func() {
		close(replayStarted)
		<-releaseReplay
	}
	observer := &recordingAdapterActivityObserver{}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
		cfg.AdapterActivityObserver = observer
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	replayed := readFrame(t, client).(*protocol.Event)
	if replayed.Seq == nil || *replayed.Seq != 1 {
		t.Fatalf("replayed event = %+v", replayed)
	}
	<-replayStarted

	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.Event{
		Type:      "log.tail",
		SessionID: "ses_1",
		Time:      2001,
		Payload:   json.RawMessage(`{"line":"during replay"}`),
	})
	_ = waitAdapterActivityCount(t, observer, 2)
	writeFrame(t, adapter, &protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Time:      2002,
		Payload:   json.RawMessage(`{"n":2}`),
	})
	close(releaseReplay)

	ephemeral := readFrame(t, client).(*protocol.Event)
	if ephemeral.Seq != nil || ephemeral.Type != "log.tail" || string(ephemeral.Payload) != `{"line":"during replay"}` {
		t.Fatalf("ephemeral event after replay = %+v payload=%s", ephemeral, string(ephemeral.Payload))
	}
	live := readFrame(t, client).(*protocol.Event)
	if live.Seq == nil || *live.Seq != 2 || string(live.Payload) != `{"n":2}` {
		t.Fatalf("live event after replay = %+v payload=%s", live, string(live.Payload))
	}
}

func TestWebSocketServerRejectsFirstFrameThatIsNotHello(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Ping{Nonce: "early"})
	errFrame := readFrame(t, conn).(*protocol.Error)
	if errFrame.Code != "invalid_hello" || !errFrame.Fatal {
		t.Fatalf("error frame = %+v", errFrame)
	}
}

func TestWebSocketServerRejectsUnauthorizedHello(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake())
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           "bad-token",
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1"}},
	})
	errFrame := readFrame(t, conn).(*protocol.Error)
	if errFrame.Code != "unauthorized" || !errFrame.Fatal {
		t.Fatalf("error frame = %+v", errFrame)
	}
}

func TestWebSocketServerTimesOutWaitingForHello(t *testing.T) {
	t.Parallel()

	server := newWebSocketTestServer(t, testHandshake(), func(cfg *hub.WebSocketConfig) {
		cfg.HandshakeTimeout = 20 * time.Millisecond
	})
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")

	errFrame := readFrame(t, conn).(*protocol.Error)
	if errFrame.Code != "timeout" || !errFrame.Fatal {
		t.Fatalf("error frame = %+v", errFrame)
	}
}

func TestClientHelloAckIsFirstFrameDuringLivePublish(t *testing.T) {
	const sessionCount = 32
	latest := make(map[string]int64, sessionCount)
	subscriptions := make([]protocol.Subscription, 0, sessionCount)
	scopes := make([]auth.Scope, 0, sessionCount)
	for index := 0; index < sessionCount; index++ {
		sessionID := fmt.Sprintf("ses_ack_%02d", index)
		latest[sessionID] = 0
		subscriptions = append(subscriptions, protocol.Subscription{SessionID: sessionID})
		scopes = append(scopes, auth.SessionView(sessionID))
	}
	events := newFakeEventStore(latest, nil)
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: websocketTestAuth{
		principals: map[string]auth.Principal{"ack-client": {Subject: "ack-client", Scopes: scopes}},
	}, EventStore: events})
	handler := hub.NewWebSocketHandler(hub.WebSocketConfig{Handshake: handshake})
	server := httptest.NewServer(handler)
	defer server.Close()

	for attempt := 0; attempt < 20; attempt++ {
		client := dialWebSocket(t, server.URL)
		stop := make(chan struct{})
		var publishers sync.WaitGroup
		for worker := 0; worker < 4; worker++ {
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				for {
					select {
					case <-stop:
						return
					default:
						_ = handler.EmitEphemeralEvent(context.Background(), protocol.Event{Type: "agent.activity", SessionID: "ses_ack_00"})
					}
				}
			}()
		}
		writeFrame(t, client, &protocol.Hello{ProtocolVersion: protocol.ProtocolVersion, Role: protocol.RoleClient, Token: "ack-client", Subscriptions: subscriptions})
		first := readFrame(t, client)
		close(stop)
		publishers.Wait()
		_ = client.Close(websocket.StatusNormalClosure, "")
		if _, ok := first.(*protocol.HelloAck); !ok {
			t.Fatalf("attempt %d first frame = %T, want hello.ack", attempt, first)
		}
	}
}

func TestAdapterDispatchFencesAuthorityLoss(t *testing.T) {
	mutations := map[string]func(*store.AdapterConnection){
		"epoch":      func(connection *store.AdapterConnection) { connection.ConnectionEpoch++ },
		"generation": func(connection *store.AdapterConnection) { connection.ActiveCredentialGeneration++ },
		"revoked":    func(connection *store.AdapterConnection) { now := time.Now(); connection.RevokedAt = &now },
		"expired": func(connection *store.AdapterConnection) {
			connection.ActiveCredentialExpiresAt = time.Now().Add(-time.Second)
		},
		"terminal": func(connection *store.AdapterConnection) { now := time.Now(); connection.TerminalAt = &now },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			events := newDispatchFenceStore()
			observer := &recordingAdapterActivityObserver{}
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
				cfg.EventStore, cfg.AdapterActivityObserver = events, observer
			})
			adapter := dialWebSocket(t, server.URL)
			defer adapter.Close(websocket.StatusNormalClosure, "")
			writeAdapterHello(t, adapter, "adapter-token")
			_ = readFrame(t, adapter).(*protocol.HelloAck)
			_ = waitAdapterActivityCount(t, observer, 1)
			events.mutateConnection(mutate)
			writeFrame(t, adapter, &protocol.Ping{Nonce: "stale-ping"})
			if frame, err := readFrameWithin(adapter, 100*time.Millisecond); err == nil {
				t.Fatalf("stale adapter received frame %+v", frame)
			}
			if got := observer.activities(); len(got) != 1 {
				t.Fatalf("stale heartbeat emitted activity %+v", got)
			}
		})
	}
}

func TestAdapterDispatchReplacesOldSocket(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	oldAdapter := dialWebSocket(t, server.URL)
	defer oldAdapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, oldAdapter, "adapter-token")
	_ = readFrame(t, oldAdapter).(*protocol.HelloAck)
	newAdapter := dialWebSocket(t, server.URL)
	defer newAdapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, newAdapter, "adapter-token")
	_ = readFrame(t, newAdapter).(*protocol.HelloAck)
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeFrame(t, client, &protocol.Command{CommandID: "cmd_current", Type: protocol.CommandSessionInterrupt, SessionID: "ses_1", Payload: json.RawMessage(`{}`)})
	if command := readFrame(t, newAdapter).(*protocol.Command); command.CommandID != "cmd_current" {
		t.Fatalf("new adapter command = %+v", command)
	}
	if _, err := readFrameWithin(oldAdapter, 100*time.Millisecond); err == nil {
		t.Fatal("replaced adapter remained readable")
	}
}

func TestAdapterHelloAckIsFirstFrameDuringConcurrentCommand(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	for index := 0; index < 16; index++ {
		writeFrame(t, client, &protocol.Command{CommandID: fmt.Sprintf("cmd_ack_%02d", index), Type: protocol.CommandSessionInterrupt, SessionID: "ses_1", Payload: json.RawMessage(`{}`)})
	}
	if first := readFrame(t, adapter); first.FrameName() != protocol.FrameHelloAck {
		t.Fatalf("first Adapter frame = %T, want hello.ack", first)
	}
}

func TestAdapterDispatchDoesNotBlockCollidingSession(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 0, "ses_55": 0}, nil)
	authenticator := websocketTestAuth{
		principals: map[string]auth.Principal{
			"adapter-token":    {Subject: "adapter-1", Scopes: []auth.Scope{auth.SessionAdapter("ses_1")}},
			"adapter-token-55": {Subject: "adapter-55", Scopes: []auth.Scope{auth.SessionAdapter("ses_55")}},
		},
		credentials: map[string]adapterCredentialEvidence{
			"adapter-1":  {Generation: 1, ExpiresAt: time.Now().Add(time.Hour)},
			"adapter-55": {Generation: 1, ExpiresAt: time.Now().Add(time.Hour)},
		},
	}
	observer := &blockingAdapterActivityObserver{sessionID: "ses_1", started: make(chan struct{}), release: make(chan struct{})}
	defer close(observer.release)
	server := newWebSocketTestServer(t, hub.NewHandshake(hub.HandshakeConfig{Authenticator: authenticator, EventStore: events}), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.AdapterActivityObserver = events, observer
	})
	first := dialWebSocket(t, server.URL)
	defer first.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloFor(t, first, "adapter-token", "ses_1")
	_ = readFrame(t, first).(*protocol.HelloAck)
	select {
	case <-observer.started:
	case <-time.After(time.Second):
		t.Fatal("first Session did not enter the blocked observer")
	}
	second := dialWebSocket(t, server.URL)
	defer second.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloFor(t, second, "adapter-token-55", "ses_55")
	frame, err := readFrameWithin(second, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("colliding Session admission was blocked: %v", err)
	}
	if _, ok := frame.(*protocol.HelloAck); !ok {
		t.Fatalf("colliding Session first frame = %T", frame)
	}
}

func TestAdapterDispatchRechecksBeforeDurableEffect(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	started, release := events.blockNextEffect()
	writeFrame(t, adapter, &protocol.Event{Type: "session.state", SessionID: "ses_1", Payload: json.RawMessage(`{"state":"working"}`)})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("durable effect did not reach dispatch barrier")
	}
	events.mutateConnection(func(connection *store.AdapterConnection) { connection.ActiveCredentialGeneration++ })
	close(release)
	time.Sleep(100 * time.Millisecond)
	if appended := events.appended(); len(appended) != 0 {
		t.Fatalf("stale enqueued event reached Store: %+v", appended)
	}
}

func TestAdapterActivityRechecksAuthorityInsideTransaction(t *testing.T) {
	mutations := map[string]func(*store.AdapterConnection){
		"expired": func(connection *store.AdapterConnection) {
			connection.ActiveCredentialExpiresAt = time.Now().Add(-time.Second)
		},
		"epoch":      func(connection *store.AdapterConnection) { connection.ConnectionEpoch++ },
		"generation": func(connection *store.AdapterConnection) { connection.ActiveCredentialGeneration++ },
		"revoked":    func(connection *store.AdapterConnection) { now := time.Now(); connection.RevokedAt = &now },
		"terminal":   func(connection *store.AdapterConnection) { now := time.Now(); connection.TerminalAt = &now },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			events := newDispatchFenceStore()
			observer := &recordingAdapterActivityObserver{}
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
				cfg.EventStore, cfg.AdapterActivityObserver = events, observer
			})
			adapter := dialWebSocket(t, server.URL)
			defer adapter.Close(websocket.StatusNormalClosure, "")
			writeAdapterHello(t, adapter, "adapter-token")
			_ = readFrame(t, adapter).(*protocol.HelloAck)
			_ = waitAdapterActivityCount(t, observer, 1)

			started, release := events.blockNextEffect()
			writeFrame(t, adapter, &protocol.Ping{Nonce: "activity-fence"})
			if pong := readFrame(t, adapter).(*protocol.Pong); pong.Nonce != "activity-fence" {
				t.Fatalf("pong = %+v", pong)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("activity effect did not enter authority transaction")
			}
			events.mutateConnection(mutate)
			close(release)
			time.Sleep(100 * time.Millisecond)
			if got := observer.activities(); len(got) != 1 {
				t.Fatalf("stale adapter activity reached observer: %+v", got)
			}
			if frame, err := readFrameWithin(adapter, time.Second); err == nil {
				t.Fatalf("stale adapter remained open with frame %+v", frame)
			}
		})
	}
}

func TestAdapterDispatchRejectsOldCredentialGeneration(t *testing.T) {
	events := newDispatchFenceStore()
	events.mutateConnection(func(connection *store.AdapterConnection) {
		connection.ActiveCredentialGeneration = 2
		connection.CredentialGenerationHighWatermark = 2
	})
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	if frame, err := readFrameWithin(adapter, 200*time.Millisecond); err == nil {
		if _, ok := frame.(*protocol.HelloAck); ok {
			t.Fatalf("old generation received hello ack %+v", frame)
		}
	}
}

func TestAdapterDispatchRejectsGenerationAboveOneBeforeRotation(t *testing.T) {
	events := newDispatchFenceStore()
	events.mutateConnection(func(connection *store.AdapterConnection) {
		connection.ActiveCredentialGeneration = 2
		connection.CredentialGenerationHighWatermark = 2
	})
	before, err := events.AdapterConnection(context.Background(), "ses_1")
	if err != nil {
		t.Fatal(err)
	}
	handshake := testHandshakeWithCredential(events, adapterCredentialEvidence{Generation: 2, ExpiresAt: time.Now().Add(time.Hour)})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	if frame, err := readFrameWithin(adapter, 200*time.Millisecond); err == nil {
		if _, accepted := frame.(*protocol.HelloAck); accepted {
			t.Fatalf("generation-2 Adapter received hello.ack before T18H")
		}
	}
	after, err := events.AdapterConnection(context.Background(), "ses_1")
	if err != nil || after.ConnectionEpoch != before.ConnectionEpoch || after.AcceptedFence != before.AcceptedFence {
		t.Fatalf("missing evidence mutated connection: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestAdapterDispatchAdmitsVerifiedRotatedCredential(t *testing.T) {
	events := newDispatchFenceStore()
	events.mutateConnection(func(connection *store.AdapterConnection) {
		connection.ActiveCredentialGeneration = 2
		connection.CredentialGenerationHighWatermark = 2
	})
	expiresAt := time.Now().Add(time.Hour).UTC()
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: websocketTestAuth{
		principals:  map[string]auth.Principal{"adapter-token": {Subject: "adapter", Scopes: []auth.Scope{auth.SessionAdapter("ses_1")}}},
		credentials: map[string]adapterCredentialEvidence{"adapter": {Generation: 2, ExpiresAt: expiresAt}},
		evidences: map[string]auth.SessionCredentialEvidence{"adapter-token": {
			SessionID: "ses_1", Lineage: auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetRotation, AttachID: "att_1"},
			Generation: 2, RotationID: "rot_2", RevocationID: "rev_2", ExpiresAt: expiresAt,
		}},
	}, EventStore: events})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	if _, ok := readFrame(t, adapter).(*protocol.HelloAck); !ok {
		t.Fatal("verified generation-2 Adapter did not receive hello.ack")
	}
}

func TestWebSocketServerRotatesAdapterCredentialAfterExactPossession(t *testing.T) {
	events := newDispatchFenceStore()
	issuer := &recordingSessionCredentialIssuer{}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.SessionCredentialIssuer, cfg.SessionCredentialLifecycle = events, issuer, issuer
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.CredentialRotationRequest{RotationID: "rot_2"})
	credential, ok := readFrame(t, adapter).(*protocol.CredentialRotationCredential)
	if !ok {
		t.Fatal("rotation request did not return a credential")
	}
	if credential.SessionID != "ses_1" || credential.RotationID != "rot_2" || credential.Generation != 2 || credential.Credential == "" || credential.ExpiresAt <= time.Now().UnixMilli() {
		t.Fatalf("rotation credential = %+v", credential)
	}
	writeFrame(t, adapter, &protocol.CredentialRotationRequest{RotationID: "rot_2"})
	duplicate, ok := readFrame(t, adapter).(*protocol.CredentialRotationCredential)
	if !ok || *duplicate != *credential {
		t.Fatalf("duplicate rotation credential = %+v, want %+v", duplicate, credential)
	}
	writeFrame(t, adapter, &protocol.CredentialRotationPossession{SessionID: credential.SessionID, RotationID: credential.RotationID, Generation: credential.Generation, AcceptedEpoch: 1})
	if rejected, ok := readFrame(t, adapter).(*protocol.Error); !ok || rejected.Code != "credential_rotation_rejected" {
		t.Fatalf("wrong possession response = %+v", rejected)
	}
	connection, err := events.AdapterConnection(context.Background(), "ses_1")
	if err != nil || connection.ActiveCredentialGeneration != 1 || connection.PendingCredentialGeneration == nil || *connection.PendingCredentialGeneration != 2 {
		t.Fatalf("wrong possession changed connection = %+v, %v", connection, err)
	}
	writeFrame(t, adapter, &protocol.CredentialRotationPossession{SessionID: credential.SessionID, RotationID: credential.RotationID, Generation: credential.Generation, AcceptedEpoch: 2})
	activation, ok := readFrame(t, adapter).(*protocol.CredentialRotationActivation)
	if !ok {
		t.Fatal("rotation possession did not return activation")
	}
	if activation.RotationID != "rot_2" || activation.Generation != 2 || activation.ConnectionEpoch != 3 || activation.AcceptedFence < 4 {
		t.Fatalf("rotation activation = %+v", activation)
	}
	connection, err = events.AdapterConnection(context.Background(), "ses_1")
	if err != nil || connection.ActiveCredentialGeneration != 2 || connection.PendingCredentialGeneration != nil || connection.RotationID != nil || connection.PriorRecoveryGeneration == nil || *connection.PriorRecoveryGeneration != 1 {
		t.Fatalf("rotated connection = %+v, %v", connection, err)
	}
	if issuer.activationCount() != 1 {
		t.Fatalf("credential activation count = %d, want 1", issuer.activationCount())
	}
}

func TestWebSocketServerRotationPossessionFailsClosedAfterRevocation(t *testing.T) {
	events := newDispatchFenceStore()
	issuer := &recordingSessionCredentialIssuer{}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.SessionCredentialIssuer, cfg.SessionCredentialLifecycle = events, issuer, issuer
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.CredentialRotationRequest{RotationID: "rot_2"})
	credential := readFrame(t, adapter).(*protocol.CredentialRotationCredential)
	events.mutateConnection(func(connection *store.AdapterConnection) {
		now := time.Now().UTC()
		connection.RevokedAt = &now
	})
	writeFrame(t, adapter, &protocol.CredentialRotationPossession{SessionID: credential.SessionID, RotationID: credential.RotationID, Generation: credential.Generation, AcceptedEpoch: 2})
	if frame, err := readFrameWithin(adapter, time.Second); err == nil {
		t.Fatalf("revoked rotation received frame %+v", frame)
	}
	connection, err := events.AdapterConnection(context.Background(), "ses_1")
	if err != nil || connection.ActiveCredentialGeneration != 1 || connection.PendingCredentialGeneration == nil || issuer.activationCount() != 0 {
		t.Fatalf("revoked possession activated rotation: connection=%+v activation=%d err=%v", connection, issuer.activationCount(), err)
	}
}

func TestWebSocketServerRecoversExpiredPendingRotation(t *testing.T) {
	events := newDispatchFenceStore()
	issuer := &recordingSessionCredentialIssuer{}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.SessionCredentialIssuer, cfg.SessionCredentialLifecycle = events, issuer, issuer
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.CredentialRotationRequest{RotationID: "rot_lost_delivery"})
	first := readFrame(t, adapter).(*protocol.CredentialRotationCredential)
	events.mutateConnection(func(connection *store.AdapterConnection) {
		expired := time.Now().Add(-time.Second)
		connection.PendingCredentialExpiresAt = &expired
	})
	writeFrame(t, adapter, &protocol.CredentialRotationRequest{RotationID: "rot_recovered"})
	recovered, ok := readFrame(t, adapter).(*protocol.CredentialRotationCredential)
	if !ok || recovered.RotationID != "rot_recovered" || recovered.Generation != first.Generation+1 || recovered.Credential == "" {
		t.Fatalf("expired pending recovery = %+v", recovered)
	}
}

func TestWebSocketServerRejectsLifecycleActivationBeforeDurableCommit(t *testing.T) {
	events := newDispatchFenceStore()
	issuer := &recordingSessionCredentialIssuer{validateFailure: errors.New("activation unavailable")}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.SessionCredentialIssuer, cfg.SessionCredentialLifecycle = events, issuer, issuer
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.CredentialRotationRequest{RotationID: "rot_rollback"})
	credential := readFrame(t, adapter).(*protocol.CredentialRotationCredential)
	writeFrame(t, adapter, &protocol.CredentialRotationPossession{SessionID: credential.SessionID, RotationID: credential.RotationID, Generation: credential.Generation, AcceptedEpoch: 2})
	if frame, readErr := readFrameWithin(adapter, time.Second); readErr == nil {
		if rejected, ok := frame.(*protocol.Error); !ok || rejected.Code != "credential_rotation_rejected" {
			t.Fatalf("activation failure response = %+v", frame)
		}
	}
	connection, err := events.AdapterConnection(context.Background(), "ses_1")
	if err != nil || connection.ActiveCredentialGeneration != 1 || connection.PendingCredentialGeneration == nil || *connection.PendingCredentialGeneration != 2 || connection.RotationID == nil || *connection.RotationID != "rot_rollback" || connection.ConnectionEpoch != 2 {
		t.Fatalf("preflight failure mutated durable rotation = %+v, %v", connection, err)
	}
}

func TestWebSocketServerFailsClosedWhenActivationViolatesPreflight(t *testing.T) {
	events := newDispatchFenceStore()
	issuer := &recordingSessionCredentialIssuer{activationFailure: errors.New("activation unavailable")}
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore, cfg.SessionCredentialIssuer, cfg.SessionCredentialLifecycle = events, issuer, issuer
	})
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	writeFrame(t, adapter, &protocol.CredentialRotationRequest{RotationID: "rot_post_cas_failure"})
	credential := readFrame(t, adapter).(*protocol.CredentialRotationCredential)
	writeFrame(t, adapter, &protocol.CredentialRotationPossession{SessionID: credential.SessionID, RotationID: credential.RotationID, Generation: credential.Generation, AcceptedEpoch: 2})
	if _, err := readFrameWithin(adapter, time.Second); err == nil {
		t.Fatal("post-CAS lifecycle failure left the stale Adapter socket open")
	}
	connection, err := events.AdapterConnection(context.Background(), "ses_1")
	if err != nil || connection.ActiveCredentialGeneration != credential.Generation || connection.ConnectionEpoch != 3 || connection.RevokedAt != nil || connection.TerminalAt != nil {
		t.Fatalf("post-CAS failure must preserve only the new fenced tuple: %+v, %v", connection, err)
	}
}

func TestWebSocketServerRejectsRotationFramesOutsideV2Adapter(t *testing.T) {
	for _, test := range []struct {
		name  string
		hello func(*testing.T, *websocket.Conn)
	}{
		{name: "v1_adapter", hello: func(t *testing.T, conn *websocket.Conn) { writeAdapterHello(t, conn, "adapter-token") }},
		{name: "v2_client", hello: func(t *testing.T, conn *websocket.Conn) { writeClientHello(t, conn, "client-token", 0) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := newDispatchFenceStore()
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
			conn := dialWebSocket(t, server.URL)
			defer conn.Close(websocket.StatusNormalClosure, "")
			test.hello(t, conn)
			_ = readFrame(t, conn).(*protocol.HelloAck)
			for _, frame := range []protocol.Frame{
				&protocol.CredentialRotationRequest{RotationID: "rot_2"},
				&protocol.CredentialRotationPossession{SessionID: "ses_1", RotationID: "rot_2", Generation: 2, AcceptedEpoch: 2},
			} {
				writeFrame(t, conn, frame)
				if rejected, ok := readFrame(t, conn).(*protocol.Error); !ok || rejected.Code != "unsupported_frame" {
					t.Fatalf("%T response = %+v", frame, rejected)
				}
			}
			connection, err := events.AdapterConnection(context.Background(), "ses_1")
			if err != nil || connection.ActiveCredentialGeneration != 1 || connection.PendingCredentialGeneration != nil || connection.RotationID != nil {
				t.Fatalf("unsupported rotation changed connection = %+v, %v", connection, err)
			}
		})
	}
}

func TestCommittedAdapterEventRemainsLiveAfterPostCommitFenceChange(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	events.afterAppend = func() {
		events.mutateConnection(func(connection *store.AdapterConnection) { now := time.Now(); connection.RevokedAt = &now })
	}
	writeFrame(t, adapter, &protocol.Event{Type: "session.state", SessionID: "ses_1", Payload: json.RawMessage(`{"state":"working"}`)})
	frame, err := readFrameWithin(client, time.Second)
	if err != nil {
		t.Fatalf("committed event disappeared from live delivery: %v", err)
	}
	event, ok := frame.(*protocol.Event)
	if !ok || event.Seq == nil || *event.Seq != 1 {
		t.Fatalf("committed live event = %+v", frame)
	}
}

func TestAdapterDispatchClosesIdleSocketOnAuthorityLoss(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	events.mutateConnection(func(connection *store.AdapterConnection) {
		now := time.Now()
		connection.RevokedAt = &now
	})
	started := time.Now()
	if _, err := readFrameWithin(adapter, 2*time.Second); err == nil || time.Since(started) >= time.Second {
		t.Fatalf("idle adapter was not promptly closed after authority loss: %v", err)
	}
}

func TestAdapterDispatchDoesNotWriteErrorAfterAuthorityLoss(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	events.mutateConnection(func(connection *store.AdapterConnection) {
		now := time.Now()
		connection.RevokedAt = &now
	})
	writeFrame(t, adapter, &protocol.Event{Type: "x.vm.idle_warning", SessionID: "ses_1", Payload: json.RawMessage(`{}`)})
	if frame, err := readFrameWithin(adapter, 200*time.Millisecond); err == nil {
		t.Fatalf("stale adapter received frame %+v", frame)
	}
}

func TestAdapterDispatchDoesNotWriteProtocolErrorAfterAuthorityLoss(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	events.mutateConnection(func(connection *store.AdapterConnection) {
		now := time.Now()
		connection.RevokedAt = &now
	})
	writeFrame(t, adapter, &protocol.Command{Type: protocol.CommandSessionSend, SessionID: "ses_1"})
	if frame, err := readFrameWithin(adapter, 200*time.Millisecond); err == nil {
		t.Fatalf("stale adapter received frame %+v", frame)
	}
}

func TestAdapterDispatchDoesNotWriteHistoryErrorAfterAuthorityLoss(t *testing.T) {
	events := newDispatchFenceStore()
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHello(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)
	events.mutateConnection(func(connection *store.AdapterConnection) {
		now := time.Now()
		connection.RevokedAt = &now
	})
	writeFrame(t, adapter, &protocol.HistoryPageRequest{RequestID: "history_stale", SessionID: "ses_1", Limit: 1})
	if frame, err := readFrameWithin(adapter, 200*time.Millisecond); err == nil {
		t.Fatalf("stale adapter received frame %+v", frame)
	}
}

func TestAdapterDispatchRealSQLiteCommit(t *testing.T) {
	ctx := context.Background()
	events, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("open SQLite Store: %v", err)
	}
	defer events.Close()
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_sqlite_dispatch", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("initialize adapter connection: %v", err)
	}
	connection, err := events.AcceptAdapterHello(ctx, "ses_sqlite_dispatch", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept adapter hello: %v", err)
	}
	grant, err := events.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("allocate grant: %v", err)
	}
	seq, err := events.AppendAdapterEvents(ctx, connection.SessionID, store.AdapterConnectionAdmission{
		CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch,
		AcceptedFence: connection.AcceptedFence, GrantFence: grant,
	}, []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: json.RawMessage(`{"state":"working"}`)}})
	if err != nil || seq != 1 {
		t.Fatalf("AppendAdapterEvents() = %d, %v", seq, err)
	}
}

func TestAdapterDispatchRealSQLiteRejectsExpiryBeforeCommit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	events, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	expiresAt := time.Now().Add(time.Hour)
	if _, err := events.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_sqlite_expiry", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: expiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	connection, err := events.AcceptAdapterHello(ctx, "ses_sqlite_expiry", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `CREATE TRIGGER slow_adapter_event BEFORE INSERT ON session_events BEGIN SELECT length(randomblob(500000)); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `UPDATE session_adapter_connections
SET active_credential_expires_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) + 50
WHERE session_id = 'ses_sqlite_expiry'`); err != nil {
		t.Fatal(err)
	}
	grant, err := events.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := make([]store.PendingEvent, 16)
	for index := range pending {
		pending[index] = store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: json.RawMessage(`{"state":"working"}`)}
	}
	if _, err := events.AppendAdapterEvents(ctx, connection.SessionID, store.AdapterConnectionAdmission{
		CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch,
		AcceptedFence: connection.AcceptedFence, GrantFence: grant,
	}, pending); err == nil {
		t.Fatal("SQLite append committed after authority expired before commit")
	}
	if latest, err := events.LatestSeq(ctx, connection.SessionID); err != nil || latest != 0 {
		t.Fatalf("latest seq after expired SQLite append = %d, %v", latest, err)
	}
}

func newWebSocketTestServer(t *testing.T, handshake *hub.Handshake, options ...func(*hub.WebSocketConfig)) *httptest.Server {
	t.Helper()

	cfg := hub.WebSocketConfig{Handshake: handshake}
	for _, option := range options {
		option(&cfg)
	}
	srv := httptest.NewServer(hub.NewWebSocketHandler(cfg))
	t.Cleanup(srv.Close)
	return srv
}

func dialWebSocket(t *testing.T, httpURL string) *websocket.Conn {
	t.Helper()

	wsURL := "ws" + strings.TrimPrefix(httpURL, "http")
	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	return conn
}

func writeFrame(t *testing.T, conn *websocket.Conn, frame protocol.Frame) {
	t.Helper()

	data, err := protocol.Encode(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func writeClientHello(t *testing.T, conn *websocket.Conn, token string, lastSeq int64) {
	t.Helper()

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		Role:            protocol.RoleClient,
		Token:           token,
		Subscriptions:   []protocol.Subscription{{SessionID: "ses_1", LastSeq: lastSeq}},
	})
}

func writeAdapterHello(t *testing.T, conn *websocket.Conn, token string) {
	writeAdapterHelloFor(t, conn, token, "ses_1")
}

func writeAdapterHelloFor(t *testing.T, conn *websocket.Conn, token, sessionID string) {
	writeAdapterHelloVersionFor(t, conn, token, sessionID, protocol.ProtocolVersion)
}

func writeAdapterHelloV2(t *testing.T, conn *websocket.Conn, token string) {
	writeAdapterHelloVersionFor(t, conn, token, "ses_1", protocol.ProtocolVersionV2)
}

func writeAdapterHelloVersionFor(t *testing.T, conn *websocket.Conn, token, sessionID string, version int) {
	t.Helper()

	writeFrame(t, conn, &protocol.Hello{
		ProtocolVersion: version,
		Role:            protocol.RoleAdapter,
		Token:           token,
		SessionID:       sessionID,
		Provider:        "claude-code",
		Resume:          true,
	})
}

func TestWebSocketServerV2AdapterReceivesProposalReceiptBeforeFanout(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) {
		cfg.EventStore = events
	})
	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")

	writeClientHello(t, client, "client-token", 0)
	_ = readFrame(t, client).(*protocol.HelloAck)
	writeAdapterHelloV2(t, adapter, "adapter-token")
	if ack := readFrame(t, adapter).(*protocol.HelloAck); ack.ProtocolVersion != protocol.ProtocolVersionV2 {
		t.Fatalf("adapter v2 hello.ack version = %d", ack.ProtocolVersion)
	}
	writeFrame(t, adapter, &protocol.Event{
		Type: "session.message", SessionID: "ses_1", Time: 2001,
		ProposalID: "proposal_01H8X", Payload: json.RawMessage(`{"role":"agent"}`),
	})
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.ProposalID != "proposal_01H8X" || receipt.Seq != 1 || receipt.Status != protocol.EventReceiptAccepted {
		t.Fatalf("proposal receipt = %+v", receipt)
	}
	if event := readFrame(t, client).(*protocol.Event); event.Seq == nil || *event.Seq != 1 || event.ProposalID != "" {
		t.Fatalf("fanout event = %+v", event)
	}
}

func TestWebSocketServerRejectsInvalidAdapterProposalIDsWithoutStoreWrite(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    int
		proposalID string
		typeName   string
	}{
		{name: "v2 durable missing", version: protocol.ProtocolVersionV2, typeName: "session.message"},
		{name: "v2 durable oversized", version: protocol.ProtocolVersionV2, proposalID: strings.Repeat("p", 256), typeName: "session.message"},
		{name: "v2 ephemeral carries proposal", version: protocol.ProtocolVersionV2, proposalID: "proposal_1", typeName: "log.tail"},
		{name: "v1 carries proposal", version: protocol.ProtocolVersion, proposalID: "proposal_1", typeName: "session.message"},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
			adapter := dialWebSocket(t, server.URL)
			defer adapter.Close(websocket.StatusNormalClosure, "")
			writeAdapterHelloVersionFor(t, adapter, "adapter-token", "ses_1", test.version)
			_ = readFrame(t, adapter).(*protocol.HelloAck)
			writeFrame(t, adapter, &protocol.Event{Type: test.typeName, SessionID: "ses_1", ProposalID: test.proposalID, Payload: json.RawMessage(`{"role":"agent"}`)})
			response, ok := readFrame(t, adapter).(*protocol.Error)
			if !ok || response.Code != "invalid_event" {
				t.Fatalf("response = %+v, want invalid_event", response)
			}
			if calls := events.appended(); len(calls) != 0 {
				t.Fatalf("invalid proposal reached Store: %+v", calls)
			}
		})
	}
}

func TestWebSocketServerV2ProposalRetryReturnsOriginalReceiptOnly(t *testing.T) {
	events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
	server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeAdapterHelloV2(t, adapter, "adapter-token")
	_ = readFrame(t, adapter).(*protocol.HelloAck)

	proposal := &protocol.Event{Type: "session.message", SessionID: "ses_1", Time: 2001, ProposalID: "proposal_retry", Payload: json.RawMessage(`{"role":"agent"}`)}
	writeFrame(t, adapter, proposal)
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.Seq != 1 {
		t.Fatalf("initial receipt = %+v", receipt)
	}
	writeFrame(t, adapter, proposal)
	if receipt := readFrame(t, adapter).(*protocol.EventReceipt); receipt.Seq != 1 || receipt.ProposalID != proposal.ProposalID {
		t.Fatalf("retry receipt = %+v", receipt)
	}
	if calls := events.appended(); len(calls) != 1 {
		t.Fatalf("proposal retry Store calls = %+v", calls)
	}

	changed := *proposal
	changed.Payload = json.RawMessage(`{"role":"other"}`)
	writeFrame(t, adapter, &changed)
	if response := readFrame(t, adapter).(*protocol.Error); response.Code != "persist_failed" {
		t.Fatalf("changed retry response = %+v", response)
	}
	if calls := events.appended(); len(calls) != 1 {
		t.Fatalf("changed retry reached Store: %+v", calls)
	}
}

func TestWebSocketServerV2ProposalRejectsMissingOrNonpositiveTime(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
	}{
		{name: "missing", payload: `{"frame":"event","type":"session.message","session_id":"ses_1","proposal_id":"proposal_time_missing","payload":{"role":"agent"}}`},
		{name: "zero", payload: `{"frame":"event","type":"session.message","session_id":"ses_1","time":0,"proposal_id":"proposal_time_zero","payload":{"role":"agent"}}`},
		{name: "negative", payload: `{"frame":"event","type":"session.message","session_id":"ses_1","time":-1,"proposal_id":"proposal_time_negative","payload":{"role":"agent"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := newFakeEventStore(map[string]int64{"ses_1": 0}, nil)
			server := newWebSocketTestServer(t, testHandshakeWithStore(events), func(cfg *hub.WebSocketConfig) { cfg.EventStore = events })
			adapter := dialWebSocket(t, server.URL)
			defer adapter.Close(websocket.StatusNormalClosure, "")
			writeAdapterHelloV2(t, adapter, "adapter-token")
			_ = readFrame(t, adapter).(*protocol.HelloAck)

			if err := adapter.Write(context.Background(), websocket.MessageText, []byte(test.payload)); err != nil {
				t.Fatalf("write proposal: %v", err)
			}
			if response := readFrame(t, adapter).(*protocol.Error); response.Code != "invalid_event" {
				t.Fatalf("response = %+v, want invalid_event", response)
			}
			if calls := events.appended(); len(calls) != 0 {
				t.Fatalf("invalid time reached Store: %+v", calls)
			}
		})
	}
}

func readFrame(t *testing.T, conn *websocket.Conn) protocol.Frame {
	t.Helper()

	frame, err := readFrameWithin(conn, time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame
}

func readFrameWithin(conn *websocket.Conn, timeout time.Duration) (protocol.Frame, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, errors.New("websocket message type is not text")
	}
	frame, err := protocol.Decode(data)
	if err != nil {
		return nil, err
	}
	return frame, nil
}

func readCommandAckFor(t *testing.T, conn *websocket.Conn, commandID string) *protocol.CommandAck {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for command ack %s", commandID)
		default:
		}
		frame := readFrame(t, conn)
		if ack, ok := frame.(*protocol.CommandAck); ok && ack.CommandID == commandID {
			return ack
		}
	}
}

func testHandshake() *hub.Handshake {
	return testHandshakeWithStore(newFakeEventStore(map[string]int64{"ses_1": 7}, nil))
}

func testHandshakeWithStore(events store.EventStore) *hub.Handshake {
	return testHandshakeWithCredential(events, adapterCredentialEvidence{Generation: 1, ExpiresAt: time.Now().Add(time.Hour)})
}

func testHandshakeWithCredential(events store.EventStore, credential adapterCredentialEvidence) *hub.Handshake {
	return hub.NewHandshake(hub.HandshakeConfig{
		Authenticator: websocketTestAuth{
			principals: map[string]auth.Principal{
				"client-token": {
					Subject: "client",
					Scopes:  []auth.Scope{auth.SessionControl("ses_1")},
				},
				"view-token": {
					Subject: "viewer",
					Scopes:  []auth.Scope{auth.SessionView("ses_1")},
				},
				"adapter-token": {
					Subject: "adapter",
					Scopes:  []auth.Scope{auth.SessionAdapter("ses_1")},
				},
				"api-token": {
					Subject: "api",
					Scopes:  []auth.Scope{auth.API()},
				},
			},
			credentials: map[string]adapterCredentialEvidence{
				"adapter": credential,
			},
		},
		EventStore: events,
	})
}

type websocketTestAuth struct {
	principals  map[string]auth.Principal
	credentials map[string]adapterCredentialEvidence
	evidences   map[string]auth.SessionCredentialEvidence
}

type adapterCredentialEvidence struct {
	Generation      int64
	ExpiresAt       time.Time
	AllowInitialize bool
}

type boundedWebsocketAuth struct {
	mu         sync.Mutex
	validCalls int
	calls      int
	principal  auth.Principal
}

type reboundWebsocketAuth struct {
	mu         sync.Mutex
	principals []auth.Principal
	calls      int
}

func (a *reboundWebsocketAuth) Authenticate(_ context.Context, _ string) (auth.Principal, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	index := a.calls
	if index >= len(a.principals) {
		index = len(a.principals) - 1
	}
	a.calls++
	return a.principals[index], nil
}

func (a *reboundWebsocketAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}

func (a *reboundWebsocketAuth) SessionAdmissionClaim(_ context.Context, _ auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	return auth.SessionAdmissionClaim{SessionID: sessionID, Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func (a *boundedWebsocketAuth) Authenticate(_ context.Context, _ string) (auth.Principal, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls > a.validCalls {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return a.principal, nil
}

func (a *boundedWebsocketAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}

func (a *boundedWebsocketAuth) SessionAdmissionClaim(_ context.Context, _ auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	return auth.SessionAdmissionClaim{SessionID: sessionID, Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

type recordingCommandActivityObserver struct {
	got []hub.CommandActivity
}

func (o *recordingCommandActivityObserver) ObserveCommandActivity(_ context.Context, activity hub.CommandActivity) {
	o.got = append(o.got, activity)
}

type recordingAdapterActivityObserver struct {
	mu  sync.Mutex
	got []hub.AdapterActivity
}

type blockingAdapterActivityObserver struct {
	sessionID string
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (o *blockingAdapterActivityObserver) ObserveAdapterActivity(_ context.Context, activity hub.AdapterActivity) {
	if activity.SessionID == o.sessionID {
		o.once.Do(func() { close(o.started); <-o.release })
	}
}

func (o *recordingAdapterActivityObserver) ObserveAdapterActivity(_ context.Context, activity hub.AdapterActivity) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.got = append(o.got, activity)
}

func (o *recordingAdapterActivityObserver) activities() []hub.AdapterActivity {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]hub.AdapterActivity(nil), o.got...)
}

func waitAdapterActivityCount(t *testing.T, observer *recordingAdapterActivityObserver, want int) []hub.AdapterActivity {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got := observer.activities()
		if len(got) >= want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return observer.activities()
}

func (a websocketTestAuth) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	principal, ok := a.principals[token]
	if !ok {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return principal, nil
}

func (a websocketTestAuth) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}

func (a websocketTestAuth) AdapterCredential(_ context.Context, _ string, principal auth.Principal, _ string) (int64, int64, bool, error) {
	evidence, ok := a.credentials[principal.Subject]
	if !ok {
		return 0, 0, false, auth.ErrUnauthorized
	}
	return evidence.Generation, evidence.ExpiresAt.UnixNano(), evidence.AllowInitialize, nil
}

func (a websocketTestAuth) SessionCredentialEvidence(_ context.Context, token string) (auth.SessionCredentialEvidence, error) {
	evidence, ok := a.evidences[token]
	if !ok {
		return auth.SessionCredentialEvidence{}, auth.ErrUnauthorized
	}
	return evidence, nil
}

func (a websocketTestAuth) SessionAdmissionClaim(_ context.Context, _ auth.Principal, sessionID string) (auth.SessionAdmissionClaim, error) {
	return auth.SessionAdmissionClaim{SessionID: sessionID, Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

type fakeEventStore struct {
	store.AdapterConnectionStore
	mu            sync.Mutex
	latest        map[string]int64
	events        map[string][]store.Event
	connections   map[string]store.AdapterConnection
	nextFence     int64
	appendErr     error
	appendCalls   []appendCall
	onReplayEvent func()
	truth         map[string]store.SessionAdmissionTruth
	replayCalls   int
	pending       map[string]store.PendingCommand
	proposals     map[string]fakeProposal
}

type settingsWebSocketStore struct{ *sqlite.Store }

func (s *settingsWebSocketStore) SessionAdmissionTruth(_ context.Context, sessionID string) (store.SessionAdmissionTruth, error) {
	if sessionID != "ses_1" {
		return store.SessionAdmissionTruth{}, errors.New("unknown settings session")
	}
	return store.SessionAdmissionTruth{SessionID: sessionID, Exists: true, Complete: true, Live: true}, nil
}

type fakeProposal struct {
	receipt store.ProposedEventReceipt
	event   store.PendingEvent
}

type t18BAttachGrantVerifier struct {
	raw   string
	grant auth.AttachGrant
}

func (v *t18BAttachGrantVerifier) VerifyAttachGrant(ctx context.Context, rawGrant, expectedAudience string) (auth.AttachGrant, error) {
	if v == nil || rawGrant != v.raw || expectedAudience != v.grant.Audience {
		return auth.AttachGrant{}, auth.ErrUnauthorized
	}
	deriver, err := auth.NewHMACAttachCommitDeriver([]byte("test-only-attach-hmac-key"), 1)
	if err != nil {
		return auth.AttachGrant{}, err
	}
	grant := v.grant
	grant.Commit, err = deriver.DeriveAttachCommit(ctx, grant)
	if err != nil {
		return auth.AttachGrant{}, err
	}
	return grant, nil
}

type recordingWarmAttachStore struct {
	*fakeEventStore
	warmMu        sync.Mutex
	warmSeen      []store.WarmAttachRequest
	attachment    store.Attachment
	commitFailure error
	beforeCommit  func()
	afterCommit   func(*recordingWarmAttachStore)
}

func newRecordingWarmAttachStore() *recordingWarmAttachStore {
	return &recordingWarmAttachStore{fakeEventStore: newFakeEventStore(map[string]int64{"ses_bootstrap": 0}, nil)}
}

func (s *recordingWarmAttachStore) AttentionSnapshot(context.Context, []string) ([]store.SessionAttentionSummary, error) {
	return nil, nil
}

func (s *recordingWarmAttachStore) CommitWarmAttach(_ context.Context, request store.WarmAttachRequest) (store.WarmAttachCommit, error) {
	if s.beforeCommit != nil {
		s.beforeCommit()
	}
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if s.commitFailure != nil {
		return store.WarmAttachCommit{}, s.commitFailure
	}
	if request.Attempt.Fingerprint.Domain != "agentwharf.attach-request.v1" ||
		request.Attempt.IssuedCredentialGeneration == nil ||
		*request.Attempt.IssuedCredentialGeneration != request.TargetActivation.Generation {
		return store.WarmAttachCommit{}, errors.New("invalid T17H warm attach contract")
	}
	duplicate := len(s.warmSeen) > 0
	s.warmSeen = append(s.warmSeen, request)
	if !duplicate {
		s.mu.Lock()
		s.connections[request.Attachment.Identity.TargetSessionID] = store.AdapterConnection{
			SessionID: request.Attachment.Identity.TargetSessionID, ActiveCredentialGeneration: request.TargetActivation.Generation,
			CredentialGenerationHighWatermark: request.TargetActivation.Generation, ActiveCredentialExpiresAt: request.TargetActivation.ExpiresAt,
		}
		s.mu.Unlock()
	}
	if s.afterCommit != nil {
		s.afterCommit(s)
	}
	if !duplicate {
		s.attachment = store.Attachment{
			Identity: request.Attachment.Identity, Status: store.AttachmentJoinPending,
			DeliveryState: store.AttachmentDeliveryPending, DeliveryVersion: 1, ExpiresAt: &request.Attachment.ExpiresAt,
		}
	}
	return store.WarmAttachCommit{
		Attempt: store.AttachAttempt{
			Identity: request.Attempt.Identity, Fingerprint: request.Attempt.Fingerprint,
			ExpiresAt: request.Attempt.ExpiresAt, Outcome: request.Attempt.Outcome,
			IssuedCredentialGeneration: request.Attempt.IssuedCredentialGeneration,
		},
		Attachment:       s.attachment,
		TargetActivation: request.TargetActivation,
		Outbox: store.WarmAttachOutbox{
			TargetSessionID: request.Attempt.Identity.TargetSessionID, CommandID: request.FirstDelivery.CommandID,
			ReferenceID: request.FirstDelivery.ReferenceID, ReferenceDigest: request.FirstDelivery.ReferenceDigest,
			ExpiresAt: request.FirstDelivery.ExpiresAt,
		},
		Duplicate: duplicate,
	}, nil
}

func (s *recordingWarmAttachStore) CreateAttachment(context.Context, store.AttachmentCreate) (store.AttachmentCommit, error) {
	return store.AttachmentCommit{}, errors.New("not implemented in warm attach test double")
}

func (s *recordingWarmAttachStore) Attachment(_ context.Context, attachID string) (store.Attachment, error) {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if s.attachment.Identity.AttachID != attachID {
		return store.Attachment{}, errors.New("attachment not found")
	}
	return s.attachment, nil
}

func (s *recordingWarmAttachStore) AttachmentForTarget(_ context.Context, targetSessionID string) (store.Attachment, error) {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if s.attachment.Identity.TargetSessionID != targetSessionID {
		return store.Attachment{}, errors.New("attachment not found")
	}
	return s.attachment, nil
}

func (s *recordingWarmAttachStore) UpdateAttachment(_ context.Context, attachID string, expectedVersion int64, update store.AttachmentUpdate) (store.AttachmentMutation, error) {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if s.attachment.Identity.AttachID != attachID || s.attachment.DeliveryVersion != expectedVersion {
		return store.AttachmentMutation{}, errors.New("stale attachment")
	}
	s.attachment.Status, s.attachment.DeliveryState = update.Status, update.DeliveryState
	s.attachment.ExpiresAt = update.ExpiresAt
	s.attachment.DeliveryVersion++
	return store.AttachmentMutation{Attachment: s.attachment}, nil
}

type recordingSessionCredentialIssuer struct {
	mu                sync.Mutex
	calls             []auth.SessionCredentialRequest
	failure           error
	validateFailure   error
	activationFailure error
	activations       int
	discards          int
}

func (issuer *recordingSessionCredentialIssuer) ActivateSessionCredential(_ context.Context, _ auth.PreparedSessionCredential) error {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.activations++
	if issuer.activationFailure != nil {
		return issuer.activationFailure
	}
	return nil
}

func (issuer *recordingSessionCredentialIssuer) ValidateSessionCredentialActivation(_ context.Context, _ auth.PreparedSessionCredential) error {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	return issuer.validateFailure
}

func (issuer *recordingSessionCredentialIssuer) DiscardSessionCredential(_ context.Context, _ auth.PreparedSessionCredential) {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.discards++
}

func (issuer *recordingSessionCredentialIssuer) activationCount() int {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	return issuer.activations
}

func (issuer *recordingSessionCredentialIssuer) discardCount() int {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	return issuer.discards
}

func (issuer *recordingSessionCredentialIssuer) PrepareSessionCredential(_ context.Context, request auth.SessionCredentialRequest) (auth.PreparedSessionCredential, error) {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if issuer.failure != nil {
		return auth.PreparedSessionCredential{}, issuer.failure
	}
	issuer.calls = append(issuer.calls, request)
	return auth.PreparedSessionCredential{
		Bearer: "test-target-bearer", SessionID: request.SessionID, Lineage: request.Lineage, Generation: request.Generation,
		RotationID: request.RotationID, RevocationID: request.RevocationID, ExpiresAt: request.ExpiresAt, Scope: auth.SessionAdapter(request.SessionID),
	}, nil
}

func (s *recordingWarmAttachStore) setCommitFailure(err error) {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	s.commitFailure = err
}

func (s *recordingWarmAttachStore) setAfterCommit(after func(*recordingWarmAttachStore)) {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	s.afterCommit = after
}

func (s *recordingWarmAttachStore) setBeforeCommit(before func()) {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	s.beforeCommit = before
}

func (s *recordingWarmAttachStore) ValidateWarmAttachTargetActivation(_ context.Context, sessionID string, activation store.WarmAttachTargetActivation) error {
	connection, err := s.AdapterConnection(context.Background(), sessionID)
	if err != nil || connection.ConnectionEpoch != 0 || connection.AcceptedFence != 0 ||
		connection.ActiveCredentialGeneration != activation.Generation || connection.CredentialGenerationHighWatermark != activation.Generation ||
		!connection.ActiveCredentialExpiresAt.Equal(activation.ExpiresAt) || connection.RevokedAt != nil || connection.TerminalAt != nil ||
		!connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return errors.New("warm attach target activation lost")
	}
	return nil
}

func (s *recordingWarmAttachStore) WithAdapterConnectionTransaction(_ context.Context, fn func(store.AdapterConnectionStore) error) error {
	if fn == nil {
		return errors.New("adapter connection transaction callback is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(&warmAttachTransactionStore{recordingWarmAttachStore: s})
}

type warmAttachTransactionStore struct {
	*recordingWarmAttachStore
}

func (s *warmAttachTransactionStore) ValidateAdapterAdmission(_ context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	connection, ok := s.connections[sessionID]
	if !ok || admission.CredentialGeneration != connection.ActiveCredentialGeneration ||
		admission.ConnectionEpoch != connection.ConnectionEpoch || admission.AcceptedFence != connection.AcceptedFence ||
		admission.GrantFence <= connection.AcceptedFence || admission.GrantFence >= s.nextFence ||
		connection.RevokedAt != nil || connection.TerminalAt != nil || !connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.AdapterConnection{}, errors.New("adapter authority lost")
	}
	return connection, nil
}

func (s *warmAttachTransactionStore) ValidateWarmAttachTargetActivation(_ context.Context, sessionID string, activation store.WarmAttachTargetActivation) error {
	connection, ok := s.connections[sessionID]
	if !ok || connection.ConnectionEpoch != 0 || connection.AcceptedFence != 0 ||
		connection.ActiveCredentialGeneration != activation.Generation || connection.CredentialGenerationHighWatermark != activation.Generation ||
		!connection.ActiveCredentialExpiresAt.Equal(activation.ExpiresAt) || connection.RevokedAt != nil || connection.TerminalAt != nil ||
		!connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return errors.New("warm attach target activation lost")
	}
	return nil
}

func (issuer *recordingSessionCredentialIssuer) setFailure(err error) {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.failure = err
}

func (s *recordingWarmAttachStore) ExpireWarmAttach(context.Context, string, int64) (store.WarmAttachExpiry, error) {
	return store.WarmAttachExpiry{}, errors.New("not implemented in T18B test double")
}

func (s *recordingWarmAttachStore) warmAttachRequests() []store.WarmAttachRequest {
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	return append([]store.WarmAttachRequest(nil), s.warmSeen...)
}

type dispatchFenceStore struct {
	*fakeEventStore
	authorityMu sync.Mutex
	connection  store.AdapterConnection
	nextFence   int64
	effectOnce  sync.Once
	effectStart chan struct{}
	effectGo    chan struct{}
	afterAppend func()
}

func newDispatchFenceStore() *dispatchFenceStore {
	return &dispatchFenceStore{
		fakeEventStore: newFakeEventStore(map[string]int64{"ses_1": 0}, nil),
		connection: store.AdapterConnection{
			SessionID: "ses_1", ConnectionEpoch: 1, AcceptedFence: 1, ActiveCredentialGeneration: 1,
			CredentialGenerationHighWatermark: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
		},
		nextFence: 2,
	}
}

func (s *dispatchFenceStore) AdapterConnection(_ context.Context, sessionID string) (store.AdapterConnection, error) {
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	if sessionID != s.connection.SessionID {
		return store.AdapterConnection{}, errors.New("adapter connection not found")
	}
	return s.connection, nil
}

func (s *dispatchFenceStore) AllocateAdapterGrantFence(context.Context) (int64, error) {
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	fence := s.nextFence
	s.nextFence++
	return fence, nil
}

func (s *dispatchFenceStore) AcceptAdapterHello(_ context.Context, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	if sessionID != s.connection.SessionID || hello.CredentialGeneration != s.connection.ActiveCredentialGeneration ||
		s.connection.RevokedAt != nil || s.connection.TerminalAt != nil || !s.connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.AdapterConnection{}, errors.New("adapter hello rejected")
	}
	s.connection.ConnectionEpoch++
	s.connection.AcceptedFence = s.nextFence
	s.nextFence++
	return s.connection, nil
}

func (s *dispatchFenceStore) ValidateAdapterAdmission(_ context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	connection := s.connection
	if sessionID != connection.SessionID || admission.CredentialGeneration != connection.ActiveCredentialGeneration ||
		admission.ConnectionEpoch != connection.ConnectionEpoch || admission.AcceptedFence != connection.AcceptedFence ||
		admission.GrantFence <= connection.AcceptedFence || admission.GrantFence >= s.nextFence ||
		connection.RevokedAt != nil || connection.TerminalAt != nil || !connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.AdapterConnection{}, errors.New("adapter authority lost")
	}
	return connection, nil
}

func (s *dispatchFenceStore) IssueAdapterConnectionAuthorityReceipt(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, writer store.SettingsWriter) (store.ConnectionAuthorityReceipt, error) {
	connection, err := s.ValidateAdapterAdmission(ctx, sessionID, admission)
	if err != nil || writer.ConnectionEpoch != connection.ConnectionEpoch || writer.CredentialGeneration != connection.ActiveCredentialGeneration || writer.LeaseID == "" {
		return store.ConnectionAuthorityReceipt{}, errors.New("connection authority receipt is fenced")
	}
	return store.ConnectionAuthorityReceipt{SessionID: sessionID, ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: connection.ActiveCredentialGeneration, AcceptedFence: connection.AcceptedFence, WriterLeaseID: writer.LeaseID, ExpiresAt: connection.ActiveCredentialExpiresAt}, nil
}

func (s *dispatchFenceStore) ValidateAdapterEffectAdmission(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	return s.ValidateAdapterAdmission(ctx, sessionID, admission)
}

func (s *dispatchFenceStore) PrepareAdapterCredentialRotation(_ context.Context, sessionID string, rotation store.AdapterCredentialRotation) (store.AdapterConnection, error) {
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	connection := &s.connection
	if sessionID != connection.SessionID || rotation.ExpectedActiveCredentialGeneration != connection.ActiveCredentialGeneration ||
		rotation.ExpectedEpoch != connection.ConnectionEpoch || rotation.PendingGeneration <= connection.CredentialGenerationHighWatermark ||
		rotation.RotationID == "" || !rotation.ExpiresAt.After(time.Now()) ||
		(connection.PendingCredentialGeneration != nil && connection.PendingCredentialExpiresAt != nil && connection.PendingCredentialExpiresAt.After(time.Now())) ||
		(connection.PendingCredentialGeneration == nil) != (connection.PendingCredentialExpiresAt == nil) || (connection.PendingCredentialGeneration == nil) != (connection.RotationID == nil) ||
		connection.RevokedAt != nil || connection.TerminalAt != nil ||
		!connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.AdapterConnection{}, errors.New("adapter credential rotation rejected")
	}
	pendingGeneration, rotationID := rotation.PendingGeneration, rotation.RotationID
	pendingExpiry := rotation.ExpiresAt
	connection.PendingCredentialGeneration = &pendingGeneration
	connection.PendingCredentialExpiresAt = &pendingExpiry
	connection.RotationID = &rotationID
	connection.CredentialGenerationHighWatermark = rotation.PendingGeneration
	return *connection, nil
}

func (s *dispatchFenceStore) ActivateAdapterCredential(_ context.Context, sessionID string, activation store.AdapterCredentialActivation) (store.AdapterConnection, error) {
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	connection := &s.connection
	if sessionID != connection.SessionID || activation.ExpectedActiveCredentialGeneration != connection.ActiveCredentialGeneration ||
		activation.ExpectedEpoch != connection.ConnectionEpoch || connection.PendingCredentialGeneration == nil ||
		connection.PendingCredentialExpiresAt == nil || connection.RotationID == nil || *connection.PendingCredentialGeneration != activation.PendingGeneration ||
		*connection.RotationID != activation.RotationID || !connection.ActiveCredentialExpiresAt.After(time.Now()) ||
		!connection.PendingCredentialExpiresAt.After(time.Now()) || connection.RevokedAt != nil || connection.TerminalAt != nil {
		return store.AdapterConnection{}, errors.New("adapter credential activation rejected")
	}
	prior := connection.ActiveCredentialGeneration
	connection.PriorRecoveryGeneration = &prior
	connection.ActiveCredentialGeneration = *connection.PendingCredentialGeneration
	connection.ActiveCredentialExpiresAt = *connection.PendingCredentialExpiresAt
	connection.PendingCredentialGeneration = nil
	connection.PendingCredentialExpiresAt = nil
	connection.RotationID = nil
	connection.ConnectionEpoch++
	connection.AcceptedFence = s.nextFence
	s.nextFence++
	return *connection, nil
}

func (s *dispatchFenceStore) WithAdapterConnectionTransaction(ctx context.Context, fn func(store.AdapterConnectionStore) error) error {
	s.waitEffect()
	return fn(s)
}

func (s *dispatchFenceStore) Append(ctx context.Context, sessionID string, events []store.PendingEvent) (int64, error) {
	s.waitEffect()
	return s.fakeEventStore.Append(ctx, sessionID, events)
}

func (s *dispatchFenceStore) AppendAdapterEvents(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, events []store.PendingEvent) (int64, error) {
	s.waitEffect()
	if _, err := s.ValidateAdapterAdmission(ctx, sessionID, admission); err != nil {
		return 0, err
	}
	firstSeq, err := s.fakeEventStore.Append(ctx, sessionID, events)
	if err == nil && s.afterAppend != nil {
		s.afterAppend()
	}
	return firstSeq, err
}

func (s *dispatchFenceStore) Replay(ctx context.Context, sessionID string, afterSeq int64, fn func(store.Event) error) error {
	return s.fakeEventStore.Replay(ctx, sessionID, afterSeq, fn)
}

func (s *dispatchFenceStore) LatestSeq(ctx context.Context, sessionID string) (int64, error) {
	return s.fakeEventStore.LatestSeq(ctx, sessionID)
}

func (s *dispatchFenceStore) mutateConnection(mutate func(*store.AdapterConnection)) {
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	mutate(&s.connection)
}

func (s *dispatchFenceStore) blockNextEffect() (<-chan struct{}, chan<- struct{}) {
	s.effectStart = make(chan struct{})
	s.effectGo = make(chan struct{})
	s.effectOnce = sync.Once{}
	return s.effectStart, s.effectGo
}

func (s *dispatchFenceStore) waitEffect() {
	if s.effectStart == nil {
		return
	}
	s.effectOnce.Do(func() {
		close(s.effectStart)
		<-s.effectGo
	})
}

type historyCall struct {
	sessionID string
	beforeSeq *int64
	limit     int
}

type fakeHistoryStore struct {
	*fakeEventStore
	mu    sync.Mutex
	page  store.HistoryPage
	err   error
	calls []historyCall
}

func (f *fakeHistoryStore) History(_ context.Context, sessionID string, beforeSeq *int64, limit int) (store.HistoryPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := historyCall{sessionID: sessionID, limit: limit}
	if beforeSeq != nil {
		value := *beforeSeq
		call.beforeSeq = &value
	}
	f.calls = append(f.calls, call)
	return f.page, f.err
}

func (f *fakeHistoryStore) historyCall(t *testing.T) historyCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 1 {
		t.Fatalf("history calls = %d, want 1", len(f.calls))
	}
	return f.calls[0]
}

func (f *fakeHistoryStore) historyCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type appendCall struct {
	sessionID string
	events    []store.PendingEvent
}

func newFakeEventStore(latest map[string]int64, events map[string][]store.Event) *fakeEventStore {
	if latest == nil {
		latest = make(map[string]int64)
	}
	if events == nil {
		events = make(map[string][]store.Event)
	}
	truth := make(map[string]store.SessionAdmissionTruth, len(latest))
	connections := make(map[string]store.AdapterConnection, len(latest))
	nextFence := int64(1)
	for sessionID := range latest {
		truth[sessionID] = store.SessionAdmissionTruth{SessionID: sessionID, Exists: true, Complete: true, Live: true}
		connections[sessionID] = store.AdapterConnection{
			SessionID: sessionID, ConnectionEpoch: 1, AcceptedFence: nextFence, ActiveCredentialGeneration: 1,
			CredentialGenerationHighWatermark: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
		}
		nextFence++
	}
	return &fakeEventStore{latest: latest, events: events, truth: truth, connections: connections, nextFence: nextFence, pending: make(map[string]store.PendingCommand), proposals: make(map[string]fakeProposal)}
}

func (f *fakeEventStore) CommitProposedEvent(_ context.Context, sessionID string, authority store.CommandAuthority, proposal store.ProposedEventRequest) (store.ProposedEventReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	connection, ok := f.connections[sessionID]
	if !ok || authority.ConnectionEpoch != connection.ConnectionEpoch || authority.CredentialGeneration != connection.ActiveCredentialGeneration ||
		connection.RevokedAt != nil || connection.TerminalAt != nil || !connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.ProposedEventReceipt{}, errors.New("proposal authority lost")
	}
	if proposal.ProposalID == "" || len(proposal.ProposalID) > 255 || proposal.Event.Type == "" || len(proposal.Event.Payload) == 0 || !json.Valid(proposal.Event.Payload) {
		return store.ProposedEventReceipt{}, errors.New("invalid proposed event")
	}
	key := sessionID + "\x00" + proposal.ProposalID
	if existing, ok := f.proposals[key]; ok {
		if existing.event.Type != proposal.Event.Type || !existing.event.Time.Equal(proposal.Event.Time) || !bytes.Equal(existing.event.Payload, proposal.Event.Payload) {
			return store.ProposedEventReceipt{}, errors.New("conflicting proposed event retry")
		}
		return existing.receipt, nil
	}
	if f.appendErr != nil {
		return store.ProposedEventReceipt{}, f.appendErr
	}
	seq := f.latest[sessionID] + 1
	pending := store.PendingEvent{Type: proposal.Event.Type, Time: proposal.Event.Time, Payload: append([]byte(nil), proposal.Event.Payload...)}
	f.appendCalls = append(f.appendCalls, appendCall{sessionID: sessionID, events: []store.PendingEvent{pending}})
	f.events[sessionID] = append(f.events[sessionID], store.Event{SessionID: sessionID, Seq: seq, Type: pending.Type, Time: pending.Time, Payload: append(json.RawMessage(nil), pending.Payload...)})
	f.latest[sessionID] = seq
	receipt := store.ProposedEventReceipt{SessionID: sessionID, ProposalID: proposal.ProposalID, Seq: seq, Status: store.ProposedEventAccepted}
	f.proposals[key] = fakeProposal{receipt: receipt, event: pending}
	return receipt, nil
}

func (f *fakeEventStore) seedPending(command store.PendingCommand) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending[command.SessionID+"\x00"+command.CommandID] = command
}

func (f *fakeEventStore) CommitPendingCommand(context.Context, string, store.CommandAuthority, store.PendingEvent, store.PendingCommandRequest) (store.PendingCommandCommit, error) {
	return store.PendingCommandCommit{}, errors.New("test fake does not commit pending commands")
}

func (f *fakeEventStore) AdapterConnection(_ context.Context, sessionID string) (store.AdapterConnection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	connection, ok := f.connections[sessionID]
	if !ok {
		return store.AdapterConnection{}, errors.New("adapter connection not found")
	}
	return connection, nil
}

func (f *fakeEventStore) AcceptAdapterHello(_ context.Context, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	connection, ok := f.connections[sessionID]
	if !ok || hello.CredentialGeneration != connection.ActiveCredentialGeneration ||
		connection.RevokedAt != nil || connection.TerminalAt != nil || !connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.AdapterConnection{}, errors.New("adapter hello rejected")
	}
	connection.ConnectionEpoch++
	connection.AcceptedFence = f.nextFence
	f.nextFence++
	f.connections[sessionID] = connection
	return connection, nil
}

func (f *fakeEventStore) AllocateAdapterGrantFence(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fence := f.nextFence
	f.nextFence++
	return fence, nil
}

func (f *fakeEventStore) ValidateAdapterAdmission(_ context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	connection, ok := f.connections[sessionID]
	if !ok || admission.CredentialGeneration != connection.ActiveCredentialGeneration ||
		admission.ConnectionEpoch != connection.ConnectionEpoch || admission.AcceptedFence != connection.AcceptedFence ||
		admission.GrantFence <= connection.AcceptedFence || admission.GrantFence >= f.nextFence ||
		connection.RevokedAt != nil || connection.TerminalAt != nil || !connection.ActiveCredentialExpiresAt.After(time.Now()) {
		return store.AdapterConnection{}, errors.New("adapter authority lost")
	}
	return connection, nil
}

func (f *fakeEventStore) IssueAdapterConnectionAuthorityReceipt(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, writer store.SettingsWriter) (store.ConnectionAuthorityReceipt, error) {
	connection, err := f.ValidateAdapterAdmission(ctx, sessionID, admission)
	if err != nil || writer.ConnectionEpoch != connection.ConnectionEpoch || writer.CredentialGeneration != connection.ActiveCredentialGeneration || writer.LeaseID == "" {
		return store.ConnectionAuthorityReceipt{}, errors.New("connection authority receipt is fenced")
	}
	return store.ConnectionAuthorityReceipt{SessionID: sessionID, ConnectionEpoch: connection.ConnectionEpoch, CredentialGeneration: connection.ActiveCredentialGeneration, AcceptedFence: connection.AcceptedFence, WriterLeaseID: writer.LeaseID, ExpiresAt: connection.ActiveCredentialExpiresAt}, nil
}

func (f *fakeEventStore) WithAdapterConnectionTransaction(_ context.Context, fn func(store.AdapterConnectionStore) error) error {
	if fn == nil {
		return errors.New("adapter connection transaction callback is nil")
	}
	return fn(f)
}

func (f *fakeEventStore) ValidateAdapterEffectAdmission(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission) (store.AdapterConnection, error) {
	return f.ValidateAdapterAdmission(ctx, sessionID, admission)
}

func (f *fakeEventStore) AppendAdapterEvents(ctx context.Context, sessionID string, admission store.AdapterConnectionAdmission, events []store.PendingEvent) (int64, error) {
	if _, err := f.ValidateAdapterAdmission(ctx, sessionID, admission); err != nil {
		return 0, err
	}
	return f.Append(ctx, sessionID, events)
}

func (f *fakeEventStore) setAdmissionTruth(sessionID string, truth store.SessionAdmissionTruth) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.truth[sessionID] = truth
}

func (f *fakeEventStore) SessionAdmissionTruth(_ context.Context, sessionID string) (store.SessionAdmissionTruth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if truth, ok := f.truth[sessionID]; ok {
		return truth, nil
	}
	return store.SessionAdmissionTruth{SessionID: sessionID}, nil
}

func (f *fakeEventStore) replayCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.replayCalls
}

func (f *fakeEventStore) setAppendError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appendErr = err
}

func (f *fakeEventStore) appended() []appendCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := make([]appendCall, len(f.appendCalls))
	copy(copied, f.appendCalls)
	return copied
}

func (f *fakeEventStore) Append(_ context.Context, sessionID string, evs []store.PendingEvent) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	firstSeq := f.latest[sessionID] + 1
	pending := make([]store.PendingEvent, len(evs))
	copy(pending, evs)
	f.appendCalls = append(f.appendCalls, appendCall{sessionID: sessionID, events: pending})
	for i, ev := range evs {
		seq := firstSeq + int64(i)
		f.events[sessionID] = append(f.events[sessionID], store.Event{
			SessionID: sessionID,
			Seq:       seq,
			Type:      ev.Type,
			Time:      ev.Time,
			Payload:   append(json.RawMessage(nil), ev.Payload...),
		})
		f.latest[sessionID] = seq
	}
	return firstSeq, nil
}

func (f *fakeEventStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.latest[sessionID], nil
}

func (f *fakeEventStore) Replay(_ context.Context, sessionID string, afterSeq int64, fn func(store.Event) error) error {
	f.mu.Lock()
	f.replayCalls++
	events := append([]store.Event(nil), f.events[sessionID]...)
	onReplayEvent := f.onReplayEvent
	f.mu.Unlock()
	for _, ev := range events {
		if ev.Seq <= afterSeq {
			continue
		}
		if err := fn(ev); err != nil {
			return err
		}
		if onReplayEvent != nil {
			onReplayEvent()
		}
	}
	return nil
}

func (f *fakeEventStore) ListPendingCommands(_ context.Context, sessionID string, _ store.CommandAuthority) ([]store.PendingCommand, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	commands := make([]store.PendingCommand, 0)
	for _, command := range f.pending {
		if command.SessionID == sessionID && (command.Status == store.PendingCommandPending || command.Status == store.PendingCommandReceived) && command.ExpiresAt.After(time.Now()) {
			commands = append(commands, command)
		}
	}
	return commands, nil
}

func (f *fakeEventStore) ClaimPendingCommand(_ context.Context, sessionID string, _ store.CommandAuthority, commandID string) (store.PendingCommandClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := sessionID + "\x00" + commandID
	command, ok := f.pending[key]
	if !ok {
		return store.PendingCommandClaim{}, errors.New("pending command not found")
	}
	if command.Status == store.PendingCommandPending {
		command.Status = store.PendingCommandReceived
		f.pending[key] = command
		return store.PendingCommandClaim{Command: command, Claimed: true}, nil
	}
	return store.PendingCommandClaim{Command: command}, nil
}

func (f *fakeEventStore) ResolvePendingCommand(_ context.Context, sessionID string, _ store.CommandAuthority, commandID string, status store.PendingCommandStatus) (store.PendingCommand, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := sessionID + "\x00" + commandID
	command, ok := f.pending[key]
	if !ok || command.Status != store.PendingCommandReceived {
		return store.PendingCommand{}, errors.New("pending command is not received")
	}
	command.Status = status
	f.pending[key] = command
	return command, nil
}

func (f *fakeEventStore) ResolvePendingCommandUnknown(_ context.Context, sessionID string, commandID string) (store.PendingCommand, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := sessionID + "\x00" + commandID
	command, ok := f.pending[key]
	if !ok || command.Status != store.PendingCommandReceived {
		return store.PendingCommand{}, errors.New("pending command is not received")
	}
	command.Status = store.PendingCommandOutcomeUnknown
	f.pending[key] = command
	return command, nil
}
