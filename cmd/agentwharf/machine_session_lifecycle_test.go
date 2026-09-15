package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

// An interactive wharf session records the launch configuration it started with
// so the daemon can recover the Session after the interactive adapter exits.
func TestAttachMachineE2EESessionPersistsLaunchRecovery(t *testing.T) {
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(t.TempDir(), "machine.json"))
	if err := saveMachineCredential(machineCredential{
		MachineID:           "machine_launch",
		MachineToken:        "machine-token",
		CloudAPIURL:         "https://cloud.superwhv.example/v1",
		LocalAccountBinding: "org/owner",
	}); err != nil {
		t.Fatalf("saveMachineCredential() error = %v", err)
	}
	cfg := wrapConfig{
		SessionID:       "ses_launch",
		Provider:        "claude-code",
		ProtocolVersion: protocol.ProtocolVersionV2,
		LaunchSettings:  wrapLaunchSettings{ModelID: "model-1", PermissionModeID: "ask"},
	}
	cleanup, err := attachMachineE2EESession(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("attachMachineE2EESession() error = %v", err)
	}
	defer cleanup()
	provider, settings, err := cfg.e2eeRuntime.loadLaunchRecovery(context.Background(), "ses_launch")
	if err != nil || provider != "claude-code" || settings.ModelID != "model-1" || settings.PermissionModeID != "ask" {
		t.Fatalf("launch recovery = provider %q settings %+v err %v", provider, settings, err)
	}
}

// The daemon must not attempt recovery without local launch evidence: recovering
// a Session it cannot restart would fence a live adapter for nothing.
func TestMachineSessionHasLaunchEvidence(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTWHARF_LOCAL_ACCOUNT_BINDING", "")
	t.Setenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE", filepath.Join(dir, "machine.json"))
	credential := machineCredential{
		MachineID:           "machine_evidence",
		CloudAPIURL:         "https://cloud.superwhv.example/v1",
		LocalAccountBinding: "org/owner",
	}
	ctx := context.Background()
	if machineSessionHasLaunchEvidence(ctx, credential, "ses_unknown") {
		t.Fatal("reported launch evidence for an unknown Session")
	}
	endpointDir, err := machineEndpointDirectory(credential, "org/owner")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := openMachineE2EERuntime(ctx, endpointDir, credential.MachineID, "org/owner")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	if err := runtime.retainLaunchRecovery(ctx, "ses_unknown", "claude-code", protocol.EncryptedLaunchSettings{Provider: "claude-code"}); err != nil {
		t.Fatal(err)
	}
	if !machineSessionHasLaunchEvidence(ctx, credential, "ses_unknown") {
		t.Fatal("recorded launch recovery was not detected as evidence")
	}
}
