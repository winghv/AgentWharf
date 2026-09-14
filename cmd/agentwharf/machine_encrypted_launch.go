package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/winghv/agentwharf/protocol"
)

// Decrypted launch configuration is held only by the in-memory wrap config.
// The persisted dispatch retains the original opaque carrier for recovery.
func applyEncryptedLaunchConfiguration(ctx context.Context, runtime *machineE2EERuntime, handoff machineServeDispatch, cfg *wrapConfig) (protocol.EncryptedLaunchSettings, error) {
	if runtime == nil || cfg == nil {
		return protocol.EncryptedLaunchSettings{}, errors.New("encrypted launch runtime unavailable")
	}
	id, err := decodeEncryptedLaunchCarrier(handoff.EncryptedFirstInstruction)
	if err != nil {
		return protocol.EncryptedLaunchSettings{}, errors.New("invalid encrypted launch carrier")
	}
	var launch protocol.EncryptedLaunchSettings
	err = runtime.executor.InspectWire(ctx, handoff.SessionID, id, "session.send", []byte(handoff.EncryptedFirstInstruction), func(payload json.RawMessage) error {
		var err error
		launch, err = protocol.DecodeEncryptedLaunchSettings(payload)
		return err
	})
	if err != nil {
		return protocol.EncryptedLaunchSettings{}, errors.New("encrypted launch configuration rejected")
	}
	if launch.Provider != handoff.Provider {
		return protocol.EncryptedLaunchSettings{}, errors.New("encrypted launch provider mismatch")
	}
	setLaunchConfiguration(cfg, launch)
	return launch, nil
}

func setLaunchConfiguration(cfg *wrapConfig, launch protocol.EncryptedLaunchSettings) {
	cfg.WorkingDirectory = launch.WorkingDirectory
	cfg.LaunchSettings = wrapLaunchSettings{ModelID: launch.ModelID, ReasoningEffortID: launch.ReasoningEffortID, PermissionModeID: launch.PermissionModeID}
}

// resolveEncryptedLaunch returns a carrier that validates against the session's
// current key and control grants, applies its configuration to cfg, and persists
// the validated settings for later endpoint-owned recovery.
//
// Session content keys rotate when account-terminal trust is turned off. That
// invalidates the original terminal-signed carrier, so once no carrier validates
// the endpoint re-seals the launch it validated earlier as its own session.send
// command. The machine's current control grant authorizes the re-sealed carrier,
// keeping the same current-grant invariant without preserving a revoked
// terminal's launch authority.
func resolveEncryptedLaunch(ctx context.Context, runtime *machineE2EERuntime, handoff machineServeDispatch, cfg *wrapConfig, stderr io.Writer) (machineServeDispatch, error) {
	if runtime == nil || cfg == nil {
		return handoff, errors.New("encrypted launch runtime unavailable")
	}
	var carriers []string
	if handoff.EncryptedFirstInstruction != "" {
		carriers = append(carriers, handoff.EncryptedFirstInstruction)
	}
	if provider, wire, err := runtime.loadLaunch(ctx, handoff.SessionID); err == nil && provider == handoff.Provider && wire != handoff.EncryptedFirstInstruction {
		carriers = append(carriers, wire)
	}
	for _, carrier := range carriers {
		probe := handoff
		probe.EncryptedFirstInstruction = carrier
		launch, err := applyEncryptedLaunchConfiguration(ctx, runtime, probe, cfg)
		if err != nil {
			continue
		}
		if err := runtime.retainLaunch(ctx, probe.SessionID, probe.Provider, carrier); err != nil {
			return handoff, err
		}
		if err := runtime.retainLaunchRecovery(ctx, probe.SessionID, probe.Provider, launch); err != nil && stderr != nil {
			_, _ = fmt.Fprintf(stderr, "wharf machine serve: retain launch recovery %s: %v\n", probe.SessionID, err)
		}
		return probe, nil
	}
	provider, settings, err := runtime.loadLaunchRecovery(ctx, handoff.SessionID)
	if err != nil || provider != handoff.Provider || settings.Provider != handoff.Provider {
		return handoff, errors.New("encrypted launch configuration rejected")
	}
	wire, err := runtime.authorizeMachineLaunch(ctx, handoff.SessionID, handoff.Provider, settings)
	if err != nil {
		return handoff, err
	}
	probe := handoff
	probe.EncryptedFirstInstruction = wire
	if _, err := applyEncryptedLaunchConfiguration(ctx, runtime, probe, cfg); err != nil {
		return handoff, err
	}
	if err := runtime.retainLaunch(ctx, probe.SessionID, probe.Provider, wire); err != nil {
		return handoff, err
	}
	if stderr != nil {
		_, _ = fmt.Fprintf(stderr, "wharf machine serve: re-sealed launch for %s under the endpoint control grant after session key rotation\n", handoff.SessionID)
	}
	return probe, nil
}
