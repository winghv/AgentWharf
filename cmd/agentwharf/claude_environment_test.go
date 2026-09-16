package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestOwnMachineEnvironmentBoundary(t *testing.T) {
	cfg := wrapConfig{Provider: "claude-code", e2eeRuntime: &machineE2EERuntime{}}
	parent := []string{"ANTHROPIC_AUTH_TOKEN=synthetic-token", "ANTHROPIC_BASE_URL=https://provider.invalid", "CUSTOM_MODEL=model", "HTTPS_PROXY=http://proxy.invalid", "CLAUDE_CONFIG_DIR=/local/config", "AGENTWHARF_ADAPTER_TOKEN=platform-token", "alias=platform-token", "superwhv_internal=excluded"}
	got, err := providerChildEnvironment(cfg, parent)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, parent[:5]) {
		t.Fatal("local configuration must survive; internal variables and aliases must not")
	}
	masker, err := providerEventMasker(cfg, parent)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer := masker.MaskWriter(&output)
	if _, err := writer.Write([]byte("error synthetic-token platform-token")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "synthetic-token") || strings.Contains(output.String(), "platform-token") {
		t.Fatal("credentials escaped masking")
	}
}

func TestEmptySecretDirDoesNotEnableLocalEnvironment(t *testing.T) {
	cfg := wrapConfig{Provider: "claude-code"}
	if ownMachineProviderEnvironment(cfg) {
		t.Fatal("missing endpoint evidence")
	}
	cfg.e2eeRuntime = &machineE2EERuntime{}
	cfg.SecretDir = "/managed"
	if ownMachineProviderEnvironment(cfg) {
		t.Fatal("managed secrets must retain strict filtering")
	}
}
