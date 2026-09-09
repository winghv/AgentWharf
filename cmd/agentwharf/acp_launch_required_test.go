package main

import (
	"bufio"
	"bytes"
	"context"
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

func TestRequiredACPLaunchDoesNotExposeProviderError(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	var writes bytes.Buffer
	scanner := bufio.NewScanner(strings.NewReader(`{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"synthetic-private-provider-error"}}` + "\n"))
	err := applyRequiredACPLaunchSettings(context.Background(), tracker, "session", &writes, scanner, wrapLaunchSettings{ModelID: "reasoning"})
	if err == nil || strings.Contains(err.Error(), "synthetic-private") {
		t.Fatalf("provider failure exposed: %v", err)
	}
}
