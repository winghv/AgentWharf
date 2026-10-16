package main

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/winghv/agentwharf/protocol"
)

// The three ways required launch settings can fail as a unit. They are sentinels
// rather than inline errors because the caller has to tell them apart from a
// crash: a provider that will not honour the settings cannot be fixed by
// reconnecting, so the machine ends the Session and says why instead of
// restarting the same launch forever.
var (
	errLaunchSettingsUnavailable      = errors.New("encrypted launch settings unavailable")
	errLaunchSettingsRejected         = errors.New("encrypted launch settings rejected")
	errLaunchSettingsReadbackMismatch = errors.New("encrypted launch settings readback mismatch")
)

// launchSettingsFailureReason maps a terminal launch-settings failure onto the
// closed vocabulary the platform accepts. It never carries the requested value,
// the provider's output or any path: the report names the class only.
func launchSettingsFailureReason(err error) (reason string, terminal bool) {
	switch {
	case errors.Is(err, errLaunchSettingsUnavailable):
		return "launch_settings_unavailable", true
	case errors.Is(err, errLaunchSettingsRejected):
		return "launch_settings_rejected", true
	case errors.Is(err, errLaunchSettingsReadbackMismatch):
		return "launch_settings_readback_mismatch", true
	default:
		return "", false
	}
}

// Required launch settings fail as a unit if any requested choice is unavailable.
// Provider readback must match every requested value before launch continues.
func applyRequiredACPLaunchSettings(ctx context.Context, tracker *acpSettingsTracker, session string, stdin io.Writer, scanner *bufio.Scanner, settings wrapLaunchSettings) error {
	if !settings.requested() {
		return nil
	}
	if tracker == nil || stdin == nil || scanner == nil {
		return errLaunchSettingsUnavailable
	}
	state, ok := tracker.Current()
	if !ok {
		return errLaunchSettingsUnavailable
	}
	change := protocol.SettingsChange{CapabilityFingerprint: state.Capability.Fingerprint}
	if settings.ModelID != "" {
		change.RequestedModelID = &settings.ModelID
	}
	if settings.ReasoningEffortID != "" {
		change.RequestedReasoningEffortID = &settings.ReasoningEffortID
	}
	if settings.PermissionModeID != "" {
		change.RequestedPermissionModeID = &settings.PermissionModeID
	}
	if validateACPSettingsChange(state, change) != "" {
		return errLaunchSettingsRejected
	}
	warnings := &discardLaunchWarnings{}
	applyACPLaunchSettings(ctx, tracker, session, stdin, scanner, settings, warnings)
	if warnings.failed || ctx.Err() != nil {
		return errors.New("encrypted launch settings application failed")
	}
	actual, ok := tracker.Current()
	if !ok || (settings.ModelID != "" && actual.Capability.EffectiveModelID != settings.ModelID) ||
		(settings.PermissionModeID != "" && actual.Capability.EffectivePermissionModeID != settings.PermissionModeID) ||
		(settings.ReasoningEffortID != "" && (actual.Capability.EffectiveReasoningEffortID == nil || *actual.Capability.EffectiveReasoningEffortID != settings.ReasoningEffortID)) {
		return errLaunchSettingsReadbackMismatch
	}
	return nil
}

// Retain only whether legacy application reported a failure. Values and provider
// diagnostics are never forwarded to stderr or stored in this adapter.
type discardLaunchWarnings struct{ failed bool }

func (w *discardLaunchWarnings) Write(data []byte) (int, error) {
	w.failed = true
	return len(data), nil
}

// Closing the actual provider pipes interrupts Scanner.Scan and a blocked stdin
// write. Context cancellation alone cannot interrupt either operation.
func applyRequiredACPLaunchSettingsWithPipes(ctx context.Context, tracker *acpSettingsTracker, session string, stdin io.WriteCloser, stdout *io.PipeReader, scanner *bufio.Scanner, settings wrapLaunchSettings) error {
	operationCtx, cancel := context.WithTimeout(ctx, acpSettingsOperationTimeout)
	defer cancel()
	closed := make(chan struct{})
	stop := context.AfterFunc(operationCtx, func() {
		_ = stdout.CloseWithError(operationCtx.Err())
		_ = stdin.Close()
		close(closed)
	})
	defer func() {
		if !stop() {
			<-closed
		}
	}()
	return applyRequiredACPLaunchSettings(operationCtx, tracker, session, stdin, scanner, settings)
}
