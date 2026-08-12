package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

func TestACPSettingsStateUsesCanonicalProviderReadback(t *testing.T) {
	state, err := acpSettingsStateFromConfigOptions(testACPConfigOptions("balanced", "ask"))
	if err != nil {
		t.Fatalf("acpSettingsStateFromConfigOptions() error = %v", err)
	}
	if state.ModelConfigID != "model" || state.PermissionConfigID != "mode" {
		t.Fatalf("config ids = model:%q permission:%q", state.ModelConfigID, state.PermissionConfigID)
	}
	if got := []string{state.Capability.Models[0].ID, state.Capability.Models[1].ID}; !reflect.DeepEqual(got, []string{"balanced", "reasoning"}) {
		t.Fatalf("sorted models = %v", got)
	}
	if state.Capability.EffectiveModelID != "balanced" || state.Capability.EffectivePermissionModeID != "ask" ||
		state.Capability.ModelChange != "allowed" || state.Capability.PermissionChange != "allowed" {
		t.Fatalf("capability = %+v", state.Capability)
	}
	payload, err := json.Marshal(state.Capability)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.DecodeSettingsCapabilityPayload(payload)
	if err != nil {
		t.Fatalf("protocol rejected Adapter capability: %v", err)
	}
	if decoded.Fingerprint != state.Capability.Fingerprint {
		t.Fatalf("fingerprint = %q, want %q", decoded.Fingerprint, state.Capability.Fingerprint)
	}
}

func TestACPSettingsTrackerDoesNotInventMissingProviderControls(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": []any{
		map[string]any{
			"id": "model", "category": "model", "type": "select", "currentValue": "balanced",
			"options": []any{map[string]any{"value": "balanced", "name": "Balanced"}},
		},
	}})
	if _, ok := tracker.Current(); ok {
		t.Fatal("tracker advertised settings without a provider-confirmed permission control")
	}
}

func TestACPSettingsMapsAndChangesProviderThoughtLevelWithoutInventingChoices(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask", "medium")})
	reserved, ok := tracker.Current()
	if !ok {
		t.Fatal("reasoning settings capability unavailable")
	}
	if reserved.ReasoningConfigID != "reasoning_effort" || reserved.Capability.EffectiveReasoningEffortID == nil ||
		*reserved.Capability.EffectiveReasoningEffortID != "medium" || reserved.Capability.ReasoningEffortChange != "allowed" || len(reserved.Capability.ReasoningEfforts) != 3 {
		t.Fatalf("reasoning capability = %+v", reserved)
	}
	requestedReasoning := "high"
	reservation := acpSettingsReservation{
		Command: protocol.Command{CommandID: "cmd_reasoning", Type: protocol.CommandSettingsChange, SessionID: "ses_1"},
		Change: protocol.SettingsChange{
			CapabilityFingerprint:      reserved.Capability.Fingerprint,
			RequestedReasoningEffortID: &requestedReasoning,
		},
		Reserved: reserved,
		Deadline: time.Now().Add(time.Second),
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	responses := newACPResponseRouter()
	providerDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		request := readACPSettingsTestRequest(scanner)
		params, _ := request["params"].(map[string]any)
		if stringFieldFromAny(request["method"]) != acpSetConfigOptionMethod || stringFieldFromAny(params["configId"]) != "reasoning_effort" || stringFieldFromAny(params["value"]) != "high" {
			providerDone <- fmt.Errorf("unexpected reasoning request: %+v", request)
			return
		}
		encoded, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0", "id": request["id"],
			"result": map[string]any{"configOptions": testACPConfigOptions("balanced", "ask", "high")},
		})
		if err != nil {
			providerDone <- err
			return
		}
		responses.Deliver(encoded, 1)
		providerDone <- nil
	}()
	var settingsMu sync.Mutex
	nextID := int64(3)
	execution := executeACPSettingsChange(context.Background(), reservation, "acp_ses_1", writer, responses, tracker, &settingsMu, &nextID)
	if err := <-providerDone; err != nil {
		t.Fatal(err)
	}
	if execution.Outcome != "applied" || execution.State.Capability.EffectiveReasoningEffortID == nil || *execution.State.Capability.EffectiveReasoningEffortID != "high" || nextID != 4 {
		t.Fatalf("reasoning execution = %+v, nextID=%d", execution, nextID)
	}
}

func TestExecuteACPSettingsChangeAppliesModelThenPermissionAndReadsBack(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	reserved, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	requestedModel := "reasoning"
	requestedPermission := "workspace"
	reservation := acpSettingsReservation{
		Command: protocol.Command{CommandID: "cmd_settings", Type: protocol.CommandSettingsChange, SessionID: "ses_1"},
		Change: protocol.SettingsChange{
			CapabilityFingerprint:     reserved.Capability.Fingerprint,
			RequestedModelID:          &requestedModel,
			RequestedPermissionModeID: &requestedPermission,
		},
		Reserved: reserved,
		Deadline: time.Now().Add(time.Second),
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	responses := newACPResponseRouter()
	providerDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		modelRequest := readACPSettingsTestRequest(scanner)
		if stringFieldFromAny(modelRequest["method"]) != acpSetConfigOptionMethod ||
			stringFieldFromAny(modelRequest["params"].(map[string]any)["configId"]) != "model" ||
			stringFieldFromAny(modelRequest["params"].(map[string]any)["value"]) != "reasoning" {
			providerDone <- io.ErrUnexpectedEOF
			return
		}
		responses.Deliver(testACPSettingsResponse(modelRequest["id"], "reasoning", "ask"), 1)
		permissionRequest := readACPSettingsTestRequest(scanner)
		if stringFieldFromAny(permissionRequest["params"].(map[string]any)["configId"]) != "mode" ||
			stringFieldFromAny(permissionRequest["params"].(map[string]any)["value"]) != "workspace" {
			providerDone <- io.ErrUnexpectedEOF
			return
		}
		responses.Deliver(testACPSettingsResponse(permissionRequest["id"], "reasoning", "workspace"), 2)
		providerDone <- nil
	}()
	nextID := int64(3)
	var settingsMu sync.Mutex
	execution := executeACPSettingsChange(context.Background(), reservation, "acp_ses_1", writer, responses, tracker, &settingsMu, &nextID)
	if err := <-providerDone; err != nil {
		t.Fatalf("fake provider error = %v", err)
	}
	if execution.Outcome != "applied" || execution.ReasonCode != nil || execution.TerminateProvider {
		t.Fatalf("execution = %+v", execution)
	}
	if !execution.PublishResult {
		t.Fatal("applied settings result was not publishable")
	}
	if execution.State.Capability.EffectiveModelID != "reasoning" || execution.State.Capability.EffectivePermissionModeID != "workspace" {
		t.Fatalf("effective capability = %+v", execution.State.Capability)
	}
	if nextID != 5 {
		t.Fatalf("next request id = %d, want 5", nextID)
	}
}

func TestACPSettingsRejectsUnsafeConfigIdentifiers(t *testing.T) {
	options := testACPConfigOptions("balanced", "ask")
	options[0].(map[string]any)["id"] = "mode with spaces"
	if _, err := acpSettingsStateFromConfigOptions(options); err == nil {
		t.Fatal("settings state accepted an unsafe config id")
	}
}

func TestACPSettingsTrackerFailsClosedOnUnexpectedIncrementalModeOnlyUpdate(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	line := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"current_mode_update","currentModeId":"workspace"}}}`)
	if _, changed, err := tracker.ObserveProviderLine(line, "acp_ses_1", 1); err == nil || changed {
		t.Fatalf("mode-only update = changed:%v err:%v, want fail-closed error", changed, err)
	}
	state, ok := tracker.Current()
	if !ok || state.Capability.EffectivePermissionModeID != "ask" {
		t.Fatalf("tracker mutated without full readback: %+v", state.Capability)
	}
}

func TestACPSettingsTrackerWaitsForFullReadbackDuringProviderModeMutation(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	initial, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	mutation, _, _, err := tracker.beginProviderMutation(initial.Capability.Fingerprint, acpPermissionCategory, "workspace")
	if err != nil {
		t.Fatal(err)
	}
	line := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"current_mode_update","currentModeId":"workspace"}}}`)
	state, changed, err := tracker.ObserveProviderLine(line, "acp_ses_1", 1)
	if err != nil || changed {
		t.Fatalf("in-flight mode update = changed:%v err:%v", changed, err)
	}
	if state.Capability.EffectivePermissionModeID != "ask" {
		t.Fatalf("tracker published partial mode update: %+v", state.Capability)
	}
	state, accepted, err := tracker.UpdateFromResult(map[string]any{"configOptions": testACPConfigOptions("balanced", "workspace")}, 2)
	mutation.finish()
	if err != nil {
		t.Fatal(err)
	}
	if !accepted {
		t.Fatal("full readback was unexpectedly superseded")
	}
	if state.Capability.EffectivePermissionModeID != "workspace" {
		t.Fatalf("full readback was not applied: %+v", state.Capability)
	}
	if _, changed, err := tracker.ObserveProviderLine(line, "acp_ses_1", 3); err != nil || changed {
		t.Fatalf("matching post-readback mode update = changed:%v err:%v", changed, err)
	}
}

func TestACPSettingsTrackerRejectsUnexpectedModeDuringPermissionMutation(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	initial, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	mutation, _, _, err := tracker.beginProviderMutation(initial.Capability.Fingerprint, acpPermissionCategory, "workspace")
	if err != nil {
		t.Fatal(err)
	}
	defer mutation.finish()
	line := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"current_mode_update","currentModeId":"ask"}}}`)
	if _, changed, err := tracker.ObserveProviderLine(line, "acp_ses_1", 1); err == nil || changed {
		t.Fatalf("unexpected in-flight mode update = changed:%v err:%v, want fail-closed error", changed, err)
	}
}

func TestACPSettingsResponseHandlerClosesMutationBeforeNextProviderLine(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	initial, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	mutation, _, _, err := tracker.beginProviderMutation(initial.Capability.Fingerprint, acpPermissionCategory, "workspace")
	if err != nil {
		t.Fatal(err)
	}
	router := newACPResponseRouter()
	response, cancel, err := router.registerWithHandler(7, func(response *acpRPCResponse) {
		defer mutation.finish()
		state, accepted, updateErr := tracker.UpdateFromResult(response.result, response.sourceSequence)
		if updateErr != nil {
			response.err = updateErr
			return
		}
		if !accepted {
			response.err = errACPSettingsReadbackSuperseded
			return
		}
		response.settingsState = &state
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if !router.Deliver(testACPSettingsResponse(7, "balanced", "workspace"), 1) {
		t.Fatal("router did not deliver settings response")
	}
	line := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"current_mode_update","currentModeId":"ask"}}}`)
	if _, changed, err := tracker.ObserveProviderLine(line, "acp_ses_1", 2); err == nil || changed {
		t.Fatalf("post-response mode update = changed:%v err:%v, want fail-closed error", changed, err)
	}
	select {
	case delivered := <-response:
		if delivered.err != nil || delivered.settingsState == nil {
			t.Fatalf("delivered response = %+v", delivered)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for settings response")
	}
}

func TestACPResponseRouterDoesNotConsumeProviderRequestWithCollidingID(t *testing.T) {
	router := newACPResponseRouter()
	response, cancel, err := router.register(7)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	request := []byte(`{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{}}`)
	if router.Deliver(request, 1) {
		t.Fatal("router consumed a Provider-to-Client request as an RPC response")
	}
	if router.Deliver([]byte(`{"jsonrpc":"2.0","id":"7","result":{"configOptions":[]}}`), 2) {
		t.Fatal("router matched a string response id to a numeric request id")
	}
	if !router.Deliver([]byte(`{"jsonrpc":"2.0","id":7,"result":{"configOptions":[]}}`), 3) {
		t.Fatal("router did not deliver the matching response")
	}
	select {
	case <-response:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the matching response")
	}
}

func TestExecuteACPSettingsChangeFailsClosedOnResponseWithResultAndError(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	reserved, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	requestedModel := "reasoning"
	reservation := acpSettingsReservation{
		Command:  protocol.Command{CommandID: "cmd_dual_response", Type: protocol.CommandSettingsChange, SessionID: "ses_1"},
		Change:   protocol.SettingsChange{CapabilityFingerprint: reserved.Capability.Fingerprint, RequestedModelID: &requestedModel},
		Reserved: reserved,
		Deadline: time.Now().Add(time.Second),
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	responses := newACPResponseRouter()
	providerDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		request := readACPSettingsTestRequest(scanner)
		response, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      request["id"],
			"result":  map[string]any{"configOptions": testACPConfigOptions("reasoning", "ask")},
			"error":   map[string]any{"code": -32000, "message": "ambiguous provider response"},
		})
		if err != nil {
			providerDone <- err
			return
		}
		if !responses.Deliver(response, 1) {
			providerDone <- errors.New("dual result/error response was not routed")
			return
		}
		providerDone <- nil
	}()
	nextID := int64(3)
	var settingsMu sync.Mutex
	execution := executeACPSettingsChange(context.Background(), reservation, "acp_ses_1", writer, responses, tracker, &settingsMu, &nextID)
	if err := <-providerDone; err != nil {
		t.Fatalf("fake provider error = %v", err)
	}
	if execution.PublishResult || !execution.TerminateProvider || execution.Outcome != "" || execution.ReasonCode != nil {
		t.Fatalf("dual result/error execution = %+v, want reconnect-only recovery", execution)
	}
}

func TestReadACPResponseRejectsProviderRequestWithCollidingID(t *testing.T) {
	scanner := bufio.NewScanner(strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"session/request_permission","params":{}}` + "\n"))
	if _, err := readACPResponse(context.Background(), scanner, 1); err == nil || !strings.Contains(err.Error(), "unexpected Provider request") {
		t.Fatalf("readACPResponse() error = %v", err)
	}
}

func TestReadACPResponseKeepsNumericAndStringIDsDistinct(t *testing.T) {
	scanner := bufio.NewScanner(strings.NewReader(`{"jsonrpc":"2.0","id":"1","result":{}}` + "\n"))
	if _, err := readACPResponse(context.Background(), scanner, 1); err == nil || !strings.Contains(err.Error(), "id type") {
		t.Fatalf("readACPResponse() error = %v", err)
	}
}

func TestExecuteACPSettingsChangeRechecksCapabilityBeforeProviderWrite(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	reserved, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	requestedModel := "reasoning"
	reservation := acpSettingsReservation{
		Command:  protocol.Command{CommandID: "cmd_stale", Type: protocol.CommandSettingsChange, SessionID: "ses_1"},
		Change:   protocol.SettingsChange{CapabilityFingerprint: reserved.Capability.Fingerprint, RequestedModelID: &requestedModel},
		Reserved: reserved,
		Deadline: time.Now().Add(time.Second),
	}
	update, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/update",
		"params": map[string]any{
			"sessionId": "acp_ses_1",
			"update": map[string]any{
				"sessionUpdate": "config_option_update",
				"configOptions": testACPConfigOptions("reasoning", "workspace"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := tracker.ObserveProviderLine(update, "acp_ses_1", 1); err != nil || !changed {
		t.Fatalf("update latest capability = changed:%v err:%v", changed, err)
	}
	var providerInput strings.Builder
	var settingsMu sync.Mutex
	nextID := int64(3)
	execution := executeACPSettingsChange(context.Background(), reservation, "acp_ses_1", &providerInput, newACPResponseRouter(), tracker, &settingsMu, &nextID)
	if execution.Outcome != "stale_capability" || !execution.PublishResult || execution.TerminateProvider {
		t.Fatalf("stale execution = %+v", execution)
	}
	if providerInput.Len() != 0 || nextID != 3 {
		t.Fatalf("stale reservation reached Provider: input=%q nextID=%d", providerInput.String(), nextID)
	}
}

func TestExecuteACPSettingsChangeStopsWhenReadbackIsSuperseded(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	reserved, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	requestedModel := "reasoning"
	requestedPermission := "workspace"
	reservation := acpSettingsReservation{
		Command: protocol.Command{CommandID: "cmd_superseded", Type: protocol.CommandSettingsChange, SessionID: "ses_1"},
		Change: protocol.SettingsChange{
			CapabilityFingerprint:     reserved.Capability.Fingerprint,
			RequestedModelID:          &requestedModel,
			RequestedPermissionModeID: &requestedPermission,
		},
		Reserved: reserved,
		Deadline: time.Now().Add(time.Second),
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	responses := newACPResponseRouter()
	providerDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		request := readACPSettingsTestRequest(scanner)
		update, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"method":  "session/update",
			"params": map[string]any{
				"sessionId": "acp_ses_1",
				"update": map[string]any{
					"sessionUpdate": "config_option_update",
					"configOptions": testACPConfigOptions("reasoning", "workspace"),
				},
			},
		})
		if err != nil {
			providerDone <- err
			return
		}
		if _, changed, err := tracker.ObserveProviderLine(update, "acp_ses_1", 2); err != nil || !changed {
			providerDone <- fmt.Errorf("later update = changed:%v err:%v", changed, err)
			return
		}
		responses.Deliver(testACPSettingsResponse(request["id"], "reasoning", "ask"), 1)
		providerDone <- nil
	}()
	var settingsMu sync.Mutex
	nextID := int64(3)
	execution := executeACPSettingsChange(context.Background(), reservation, "acp_ses_1", writer, responses, tracker, &settingsMu, &nextID)
	if err := <-providerDone; err != nil {
		t.Fatal(err)
	}
	if execution.Outcome != "mismatched_effective" || !execution.PublishResult || execution.TerminateProvider {
		t.Fatalf("superseded execution = %+v", execution)
	}
	if nextID != 4 {
		t.Fatalf("superseded execution wrote another Provider request; nextID=%d", nextID)
	}
}

func TestACPSettingsTrackerKeepsLaterProviderUpdateOverEarlierRPCReadback(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	update, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/update",
		"params": map[string]any{
			"sessionId": "acp_ses_1",
			"update": map[string]any{
				"sessionUpdate": "config_option_update",
				"configOptions": testACPConfigOptions("reasoning", "workspace"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := tracker.ObserveProviderLine(update, "acp_ses_1", 5); err != nil || !changed {
		t.Fatalf("observe later update = changed:%v err:%v", changed, err)
	}
	state, accepted, err := tracker.UpdateFromResult(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if accepted {
		t.Fatal("earlier RPC response unexpectedly replaced a newer Provider update")
	}
	if state.Capability.EffectiveModelID != "reasoning" || state.Capability.EffectivePermissionModeID != "workspace" {
		t.Fatalf("tracker rolled back to an earlier RPC response: %+v", state.Capability)
	}
}

func TestExecuteACPSettingsChangeDoesNotPublishOldStateAfterTimeout(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	reserved, ok := tracker.Current()
	if !ok {
		t.Fatal("initial settings capability unavailable")
	}
	requestedModel := "reasoning"
	reservation := acpSettingsReservation{
		Command:  protocol.Command{CommandID: "cmd_timeout", Type: protocol.CommandSettingsChange, SessionID: "ses_1"},
		Change:   protocol.SettingsChange{CapabilityFingerprint: reserved.Capability.Fingerprint, RequestedModelID: &requestedModel},
		Reserved: reserved,
		Deadline: time.Now().Add(-time.Millisecond),
	}
	nextID := int64(3)
	var settingsMu sync.Mutex
	operationCtx, cancel := context.WithDeadline(context.Background(), reservation.Deadline)
	defer cancel()
	execution := executeACPSettingsChange(operationCtx, reservation, "acp_ses_1", io.Discard, newACPResponseRouter(), tracker, &settingsMu, &nextID)
	if execution.PublishResult || !execution.TerminateProvider || execution.Outcome != "" {
		t.Fatalf("timeout execution = %+v, want reconnect-only recovery", execution)
	}
}

func testACPConfigOptions(model, mode string, reasoning ...string) []any {
	options := []any{
		map[string]any{
			"id": "mode", "name": "Mode", "category": "mode", "type": "select", "currentValue": mode,
			"options": []any{
				map[string]any{"value": "workspace", "name": "Workspace"},
				map[string]any{"value": "ask", "name": "Ask first"},
			},
		},
		map[string]any{
			"id": "model", "name": "Model", "category": "model", "type": "select", "currentValue": model,
			"options": []any{
				map[string]any{"value": "reasoning", "name": "Reasoning"},
				map[string]any{"value": "balanced", "name": "Balanced"},
			},
		},
	}
	if len(reasoning) > 0 {
		options = append(options, map[string]any{
			"id": "reasoning_effort", "name": "Reasoning effort", "category": "thought_level", "type": "select", "currentValue": reasoning[0],
			"options": []any{
				map[string]any{"value": "low", "name": "Low"},
				map[string]any{"value": "medium", "name": "Medium"},
				map[string]any{"value": "high", "name": "High"},
			},
		})
	}
	return options
}

func readACPSettingsTestRequest(scanner *bufio.Scanner) map[string]any {
	if !scanner.Scan() {
		return nil
	}
	var request map[string]any
	_ = json.Unmarshal(scanner.Bytes(), &request)
	return request
}

func testACPSettingsResponse(id any, model, mode string) []byte {
	encoded, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  map[string]any{"configOptions": testACPConfigOptions(model, mode)},
	})
	return []byte(strings.TrimSpace(string(encoded)))
}
