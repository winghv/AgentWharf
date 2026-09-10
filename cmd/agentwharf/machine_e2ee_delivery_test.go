package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/winghv/agentwharf/adapter/core"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func TestEncryptedStopChildProcess(t *testing.T) {
	if len(os.Args) < 2 || os.Args[1] != "-test.run=^TestEncryptedStopChildProcess$" {
		return
	}
	for {
		time.Sleep(time.Second)
	}
}

type failingEncryptedACPWriter struct{ calls int }

func (w *failingEncryptedACPWriter) Write([]byte) (int, error) {
	w.calls++
	return 0, errors.New("synthetic provider failure")
}

func TestMachineRuntimeDeliveryIsLocallyClaimedAndNotReplayed(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	runtime, err := openMachineE2EERuntime(ctx, directory, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// Local authenticated grant provisioning, not a relay operation.
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	key, err := runtime.vault.Load(ctx, "session", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	payload := json.RawMessage(`{"content":[{"kind":"text","text":"private command"}],"launch":{"working_directory":"/synthetic/private","model_id":"reasoning","permission_mode_id":"ask"}}`)
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "cmd", Type: "session.send"}, key, private, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "cmd", "type": "session.send", "packet": packet})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.VerifyWire(ctx, "session", "cmd", "session.send", wire); err != nil {
		t.Fatal("valid launch preflight", err)
	}
	if err := runtime.executor.VerifyWire(ctx, "other", "cmd", "session.send", wire); err == nil {
		t.Fatal("substituted launch session")
	}
	var claimed int
	if err := runtime.database.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_local_commands`).Scan(&claimed); err != nil || claimed != 0 {
		t.Fatal("preflight consumed command", err)
	}
	command := &protocol.Command{SessionID: "session", CommandID: "cmd", Type: protocol.CommandSessionSend, Payload: wire}
	handoff := machineServeDispatch{SessionID: "session", EncryptedFirstInstruction: string(wire)}
	var launchCfg wrapConfig
	if err := applyEncryptedLaunchConfiguration(ctx, runtime, handoff, &launchCfg); err != nil {
		t.Fatal(err)
	}
	if launchCfg.WorkingDirectory != "/synthetic/private" || launchCfg.LaunchSettings.ModelID != "reasoning" || launchCfg.LaunchSettings.PermissionModeID != "ask" {
		t.Fatal("authenticated launch configuration missing")
	}
	if handoff.WorkingDirectory != "" || handoff.ModelID != "" || handoff.PermissionModeID != "" {
		t.Fatal("plaintext copied into persisted handoff")
	}
	if err := runtime.retainLaunch(ctx, "session", "test-provider", string(wire)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.retainLaunch(ctx, "session", "other-provider", string(wire)); err == nil {
		t.Fatal("retained launch provider substituted")
	}
	retainedProvider, retainedWire, err := runtime.loadLaunch(ctx, "session")
	if err != nil || retainedProvider != "test-provider" || retainedWire != string(wire) {
		t.Fatal("retained launch changed", err)
	}
	calls := 0
	provider := func(ctx context.Context, decoded *protocol.Command) error {
		calls++
		if string(decoded.Payload) != string(payload) {
			t.Fatal("wrong provider content")
		}
		var state string
		if err := runtime.database.QueryRowContext(ctx, `SELECT state FROM e2ee_local_commands WHERE session='session' AND message='cmd'`).Scan(&state); err != nil || state != "claimed" {
			t.Fatalf("not durably claimed: %v", err)
		}
		return nil
	}
	if _, err := runtime.deliverCommand(ctx, "other", command, provider); err == nil {
		t.Fatal("wrong session accepted")
	}
	if _, err := runtime.deliverCommand(ctx, "session", command, provider); err != nil {
		t.Fatal(err)
	}
	if err := runtime.database.Close(); err != nil {
		t.Fatal(err)
	}
	runtime, err = openMachineE2EERuntime(ctx, directory, "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	if provider, retained, err := runtime.loadLaunch(ctx, "session"); err != nil || provider != "test-provider" || retained != string(wire) {
		t.Fatal("signed launch missing after restart", err)
	}
	_, _ = runtime.deliverCommand(ctx, "session", command, provider)
	eventPayload := json.RawMessage(`{"role":"agent","content":[{"kind":"text","text":"private-reply-canary"}]}`)
	sealed, err := runtime.sealEvent(ctx, "session", "event-1", "session.message", eventPayload)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("private-reply-canary")) {
		t.Fatal("plaintext in event carrier")
	}
	var event e2ee.EventWire
	if err := json.Unmarshal(sealed, &event); err != nil {
		t.Fatal(err)
	}
	signer, err := base64.RawURLEncoding.DecodeString(runtime.public.SigningKey)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := e2ee.OpenPacket(e2ee.Context{Scope: "event", Session: "session", KeyID: "key", Sender: runtime.public.Device, MessageID: "event-1", Type: "session.message"}, key, signer, event.Packet)
	if err != nil || !bytes.Equal(opened, eventPayload) {
		t.Fatalf("event decryption: %v", err)
	}
	var used int
	if err := runtime.database.QueryRowContext(ctx, `SELECT used FROM e2ee_seal_budget WHERE session='session' AND key_id='key'`).Scan(&used); err != nil || used != 1 {
		t.Fatalf("seal reservation: %d %v", used, err)
	}
	// Exercise the production ACP boundary with a distinct authenticated command.
	acpPacket, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "acp-cmd", Type: "session.send"}, key, private, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	acpWire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "acp-cmd", "type": "session.send", "packet": acpPacket})
	if err != nil {
		t.Fatal(err)
	}
	var stdin bytes.Buffer
	nextID := int64(10)
	acknowledged := false
	err = deliverEncryptedACPPrompt(ctx, wrapConfig{SessionID: "session", e2eeRuntime: runtime}, &protocol.Command{SessionID: "session", CommandID: "acp-cmd", Type: protocol.CommandSessionSend, Payload: acpWire}, &stdin, "provider-session", &nextID, func(frame protocol.Frame) error {
		ack, ok := frame.(*protocol.CommandAck)
		if !ok || ack.Status != protocol.AckAccepted {
			t.Fatal("invalid ACP acknowledgement")
		}
		var state string
		if err := runtime.database.QueryRowContext(ctx, `SELECT state FROM e2ee_local_commands WHERE message='acp-cmd'`).Scan(&state); err != nil || state != "completed" {
			t.Fatal("ack before durable completion")
		}
		acknowledged = true
		return nil
	})
	if err != nil || !acknowledged || nextID != 11 {
		t.Fatalf("ACP delivery: %v", err)
	}
	var rpc struct {
		Method string
		Params struct {
			SessionID string `json:"sessionId"`
			Prompt    []struct {
				Text string `json:"text"`
			}
		}
	}
	if json.Unmarshal(stdin.Bytes(), &rpc) != nil || rpc.Method != "session/prompt" || rpc.Params.SessionID != "provider-session" || len(rpc.Params.Prompt) != 1 || rpc.Params.Prompt[0].Text != "private command" {
		t.Fatal("wrong ACP request")
	}
	failurePacket, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "failed-acp", Type: "session.send"}, key, private, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	failureWire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "failed-acp", "type": "session.send", "packet": failurePacket})
	if err != nil {
		t.Fatal(err)
	}
	failedCommand := &protocol.Command{SessionID: "session", CommandID: "failed-acp", Type: protocol.CommandSessionSend, Payload: failureWire}
	writer := &failingEncryptedACPWriter{}
	for attempt := 0; attempt < 2; attempt++ {
		err := deliverEncryptedACPPrompt(ctx, wrapConfig{SessionID: "session", e2eeRuntime: runtime}, failedCommand, writer, "provider-session", &nextID, func(protocol.Frame) error { t.Error("failed delivery acknowledged"); return nil })
		if err == nil {
			t.Fatal("failed delivery reported success")
		}
	}
	if writer.calls != 1 {
		t.Fatalf("ambiguous command retried: %d", writer.calls)
	}
	var failedState string
	if err := runtime.database.QueryRowContext(ctx, `SELECT state FROM e2ee_local_commands WHERE message='failed-acp'`).Scan(&failedState); err != nil || failedState != "outcome_unknown" {
		t.Fatalf("failed state=%s err=%v", failedState, err)
	}
	permissionPayload := json.RawMessage(`{"request_id":"permission-1","decision":"approve"}`)
	permissionPacket, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "approval-cmd", Type: "permission.respond"}, key, private, e2ee.PublicMetadata{RequestID: "permission-1", Decision: "approve"}, permissionPayload)
	if err != nil {
		t.Fatal(err)
	}
	permissionWire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "approval-cmd", "type": "permission.respond", "packet": permissionPacket})
	if err != nil {
		t.Fatal(err)
	}
	pending := map[string]acpPendingPermission{"permission-1": {RPCID: int64(5), Options: []map[string]any{{"kind": "allow_once", "optionId": "allow"}}}}
	stdin.Reset()
	err = deliverEncryptedACPCommand(ctx, wrapConfig{SessionID: "session", e2eeRuntime: runtime}, &protocol.Command{SessionID: "session", CommandID: "approval-cmd", Type: protocol.CommandPermissionRespond, Payload: permissionWire}, &stdin, "provider-session", &nextID, pending, &sync.Mutex{}, func(protocol.Frame) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var permissionResult struct {
		Result struct {
			Outcome struct {
				OptionID string `json:"optionId"`
			}
		}
	}
	if json.Unmarshal(stdin.Bytes(), &permissionResult) != nil || permissionResult.Result.Outcome.OptionID != "allow" || len(pending) != 0 {
		t.Fatal("encrypted approval not translated to ACP option")
	}
	interruptPacket, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "interrupt-cmd", Type: "session.interrupt"}, key, private, e2ee.PublicMetadata{}, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	interruptWire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "interrupt-cmd", "type": "session.interrupt", "packet": interruptPacket})
	if err != nil {
		t.Fatal(err)
	}
	stdin.Reset()
	err = deliverEncryptedACPCommand(ctx, wrapConfig{SessionID: "session", e2eeRuntime: runtime}, &protocol.Command{SessionID: "session", CommandID: "interrupt-cmd", Type: protocol.CommandSessionInterrupt, Payload: interruptWire}, &stdin, "provider-session", &nextID, nil, nil, func(protocol.Frame) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(stdin.Bytes(), &rpc) != nil || rpc.Method != "session/cancel" || rpc.Params.SessionID != "provider-session" {
		t.Fatal("encrypted interrupt not delivered to ACP")
	}
	stopPacket, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "stop-cmd", Type: "session.stop"}, key, private, e2ee.PublicMetadata{}, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	stopWire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "stop-cmd", "type": "session.stop", "packet": stopPacket})
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := core.NewProcessSupervisor(core.ProcessConfig{Command: core.ProcessCommand{Path: os.Args[0], Args: []string{"-test.run=^TestEncryptedStopChildProcess$"}}, GracePeriod: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	processCtx, processCancel := context.WithTimeout(ctx, 5*time.Second)
	defer processCancel()
	runDone := make(chan error, 1)
	go func() { runDone <- supervisor.Run(processCtx) }()
	select {
	case <-supervisor.Events():
	case <-processCtx.Done():
		t.Fatal("provider did not start")
	}
	stops := 0
	stopCommand := &protocol.Command{SessionID: "session", CommandID: "stop-cmd", Type: protocol.CommandSessionStop, Payload: stopWire}
	for attempt := 0; attempt < 2; attempt++ {
		_, err := runtime.deliverCommand(ctx, "session", stopCommand, func(stopCtx context.Context, _ *protocol.Command) error { stops++; return supervisor.Stop(stopCtx) })
		if attempt == 0 && err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("provider stop: %v", err)
		}
	case <-processCtx.Done():
		t.Fatal("provider did not exit after encrypted stop")
	}
	if stops != 1 {
		t.Fatal("stop redelivery repeated endpoint effect")
	}
	if calls != 1 {
		t.Fatalf("provider deliveries=%d", calls)
	}
}
