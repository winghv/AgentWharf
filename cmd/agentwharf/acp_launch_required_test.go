package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestRequiredACPLaunchRejectsUnavailableBeforeMutation(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	var writes bytes.Buffer
	err := applyRequiredACPLaunchSettings(context.Background(), tracker, "session", &writes, bufio.NewScanner(strings.NewReader("")), wrapLaunchSettings{ModelID: "reasoning", PermissionModeID: "synthetic-private-unavailable"})
	if err == nil || strings.Contains(err.Error(), "synthetic-private") || writes.Len() != 0 {
		t.Fatalf("unavailable settings mutated or leaked: %v", err)
	}
}

func TestRequiredACPLaunchChecksReadbackAndMasksProviderFailure(t *testing.T) {
	for _, model := range []string{"reasoning", "balanced"} {
		tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
		response := testACPSettingsResponse(1, model, "ask")
		var writes bytes.Buffer
		encoded := string(response)
		err := applyRequiredACPLaunchSettings(context.Background(), tracker, "session", &writes, bufio.NewScanner(strings.NewReader(encoded+"\n")), wrapLaunchSettings{ModelID: "reasoning"})
		if (err == nil) != (model == "reasoning") {
			t.Fatalf("readback %s: %v", model, err)
		}
	}
}

// testACPSettingsProviderState is the settings a scripted provider currently
// reports. Keeping it as state rather than three locals is what makes
// "switching model moves the effort" expressible instead of hardcoded.
type testACPSettingsProviderState struct {
	model     string
	mode      string
	reasoning string
}

// testACPSettingsEffortDefaults mirrors the provider behaviour these tests
// reproduce: the effort a session reports follows whichever model is active.
var testACPSettingsEffortDefaults = map[string]string{"balanced": "high", "reasoning": "medium"}

func (s testACPSettingsProviderState) configOptions() []any {
	return testACPConfigOptions(s.model, s.mode, s.reasoning)
}

func TestRequiredACPLaunchDoesNotExposeProviderError(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	var writes bytes.Buffer
	scanner := bufio.NewScanner(strings.NewReader(`{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"synthetic-private-provider-error"}}` + "\n"))
	err := applyRequiredACPLaunchSettings(context.Background(), tracker, "session", &writes, scanner, wrapLaunchSettings{ModelID: "reasoning"})
	if err == nil || strings.Contains(err.Error(), "synthetic-private") {
		t.Fatalf("provider failure exposed: %v", err)
	}
}

// Which failures no retry can fix is the whole difference between "end it and
// say why" and "reconnect until the credential expires". A crash, a lost socket
// or a provider timeout must stay restartable; a provider that will not honour
// the requested settings must not be.
func TestLaunchSettingsFailureReasonSeparatesTerminalFromRestartable(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		err      error
		reason   string
		terminal bool
	}{
		{name: "no capability", err: errLaunchSettingsUnavailable, reason: "launch_settings_unavailable", terminal: true},
		{name: "request refused", err: errLaunchSettingsRejected, reason: "launch_settings_rejected", terminal: true},
		{name: "readback mismatch", err: errLaunchSettingsReadbackMismatch, reason: "launch_settings_readback_mismatch", terminal: true},
		{name: "wrapped mismatch", err: encryptedAdapterError{errLaunchSettingsReadbackMismatch}, reason: "launch_settings_readback_mismatch", terminal: true},
		{name: "transient application failure", err: errors.New("encrypted launch settings application failed")},
		{name: "provider crash", err: errors.New("provider exited")},
		{name: "clean exit", err: nil},
	} {
		reason, terminal := launchSettingsFailureReason(testCase.err)
		if reason != testCase.reason || terminal != testCase.terminal {
			t.Fatalf("%s: reason=%q terminal=%v, want %q/%v", testCase.name, reason, terminal, testCase.reason, testCase.terminal)
		}
	}
}

// A provider may reset one setting as a side effect of another: switching model
// drops the reasoning effort to the new model's default. The requested effort
// matched the value before the model changed, so planning every mutation from
// one snapshot skipped it and then refused the launch on a readback mismatch --
// for a session that was reachable all along.
func TestRequiredACPLaunchReappliesSettingTheProviderReset(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask", "high")})
	reqReader, reqWriter := io.Pipe()
	defer reqReader.Close()
	defer reqWriter.Close()
	respReader, respWriter := io.Pipe()
	defer respReader.Close()
	defer respWriter.Close()
	scanner := bufio.NewScanner(respReader)

	providerDone := make(chan error, 1)
	go func() {
		reqScanner := bufio.NewScanner(reqReader)
		// The provider holds the settings it currently reports. Switching model
		// is what moves the effort off the value the launch asked for, so the
		// effort has to be a consequence of the model rather than a constant.
		state := testACPSettingsProviderState{model: "balanced", mode: "ask", reasoning: "high"}
		for i := 0; i < 2; i++ {
			request := readACPSettingsTestRequest(reqScanner)
			if request == nil {
				providerDone <- errors.New("provider received no request")
				return
			}
			params, _ := request["params"].(map[string]any)
			configID, value := stringFieldFromAny(params["configId"]), stringFieldFromAny(params["value"])
			switch configID {
			case "model":
				state.model = value
				state.reasoning = testACPSettingsEffortDefaults[value]
			case "reasoning_effort":
				state.reasoning = value
			default:
				providerDone <- fmt.Errorf("unexpected config id %q", configID)
				return
			}
			encoded, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": request["id"],
				"result": map[string]any{"configOptions": state.configOptions()},
			})
			if err != nil {
				providerDone <- err
				return
			}
			if _, err := respWriter.Write(append(encoded, '\n')); err != nil {
				providerDone <- err
				return
			}
		}
		providerDone <- nil
	}()

	var warnings strings.Builder
	applyACPLaunchSettings(context.Background(), tracker, "acp_ses_1", reqWriter, scanner, wrapLaunchSettings{
		ModelID: "reasoning", ReasoningEffortID: "high",
	}, &warnings)
	if err := <-providerDone; err != nil {
		t.Fatal(err)
	}
	if warnings.Len() != 0 {
		t.Fatalf("unexpected warnings: %s", warnings.String())
	}
	applied, ok := tracker.Current()
	if !ok {
		t.Fatal("tracker lost capability after launch settings")
	}
	if applied.Capability.EffectiveModelID != "reasoning" ||
		applied.Capability.EffectiveReasoningEffortID == nil || *applied.Capability.EffectiveReasoningEffortID != "high" {
		t.Fatalf("applied capability = %+v", applied.Capability)
	}
}

// The opposite case: a provider that advertises a value and then silently keeps
// its own must not be retried forever. The launch stops after a bounded number
// of attempts and the required path reports the readback mismatch.
func TestRequiredACPLaunchStopsWhenProviderKeepsItsOwnValue(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask", "medium")})
	reqReader, reqWriter := io.Pipe()
	defer reqReader.Close()
	defer reqWriter.Close()
	respReader, respWriter := io.Pipe()
	defer respReader.Close()
	defer respWriter.Close()
	scanner := bufio.NewScanner(respReader)

	providerDone := make(chan int, 1)
	go func() {
		reqScanner := bufio.NewScanner(reqReader)
		requests := 0
		for i := 0; i < acpSettingsLaunchAttempts; i++ {
			request := readACPSettingsTestRequest(reqScanner)
			if request == nil {
				providerDone <- requests
				return
			}
			requests++
			// The provider accepts the call and reports its own value back.
			encoded, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": request["id"],
				"result": map[string]any{"configOptions": testACPConfigOptions("balanced", "ask", "medium")},
			})
			if err != nil {
				providerDone <- requests
				return
			}
			if _, err := respWriter.Write(append(encoded, '\n')); err != nil {
				providerDone <- requests
				return
			}
		}
		providerDone <- requests
	}()

	var warnings strings.Builder
	applyACPLaunchSettings(context.Background(), tracker, "acp_ses_1", reqWriter, scanner, wrapLaunchSettings{
		ReasoningEffortID: "high",
	}, &warnings)
	if requests := <-providerDone; requests != acpSettingsLaunchAttempts {
		t.Fatalf("provider received %d requests, want the bounded %d", requests, acpSettingsLaunchAttempts)
	}
	state, ok := tracker.Current()
	if !ok || state.Capability.EffectiveReasoningEffortID == nil || *state.Capability.EffectiveReasoningEffortID != "medium" {
		t.Fatalf("tracker state = %+v ok=%v", state.Capability, ok)
	}
}
