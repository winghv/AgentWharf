package main

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/winghv/agentwharf/protocol"
)

// Decrypted launch configuration is held only by the in-memory wrap config.
// The persisted dispatch retains the original opaque carrier for recovery.
func applyEncryptedLaunchConfiguration(ctx context.Context, runtime *machineE2EERuntime, handoff machineServeDispatch, cfg *wrapConfig) error {
	if runtime == nil || cfg == nil {
		return errors.New("encrypted launch runtime unavailable")
	}
	id, err := decodeEncryptedLaunchCarrier(handoff.EncryptedFirstInstruction)
	if err != nil {
		return errors.New("invalid encrypted launch carrier")
	}
	var launch protocol.EncryptedLaunchSettings
	err = runtime.executor.InspectWire(ctx, handoff.SessionID, id, "session.send", []byte(handoff.EncryptedFirstInstruction), func(payload json.RawMessage) error {
		var err error
		launch, err = protocol.DecodeEncryptedLaunchSettings(payload)
		return err
	})
	if err != nil {
		return errors.New("encrypted launch configuration rejected")
	}
	if launch.Provider != handoff.Provider {
		return errors.New("encrypted launch provider mismatch")
	}
	cfg.WorkingDirectory = launch.WorkingDirectory
	cfg.LaunchSettings = wrapLaunchSettings{ModelID: launch.ModelID, ReasoningEffortID: launch.ReasoningEffortID, PermissionModeID: launch.PermissionModeID}
	return nil
}
