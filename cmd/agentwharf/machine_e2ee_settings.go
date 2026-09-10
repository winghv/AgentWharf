package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

// Required settings are authorized by local device grants, not a Hub delivery
// execute frame. Provider mutations occur only inside the durable local claim.
func deliverEncryptedACPSettings(ctx context.Context, cfg wrapConfig, command *protocol.Command, stdin io.Writer, providerSessionID string, nextID *int64, responses *acpResponseRouter, tracker *acpSettingsTracker, mu *sync.Mutex, writeFrame func(protocol.Frame) error) error {
	if cfg.e2eeRuntime == nil || command == nil || command.Type != protocol.CommandSettingsChange || tracker == nil || mu == nil || writeFrame == nil {
		return errors.New("encrypted settings runtime unavailable")
	}
	admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(deliveryCtx context.Context, decoded *protocol.Command) error {
		change, err := protocol.DecodeSettingsChangePayload(decoded.Payload)
		if err != nil {
			return errors.New("invalid encrypted settings request")
		}
		mu.Lock()
		state, available := tracker.Current()
		mu.Unlock()
		if !available || validateACPSettingsChange(state, change) != "" {
			return errors.New("encrypted settings capability unavailable")
		}
		reservation := acpSettingsReservation{Command: *decoded, Change: change, Reserved: state, Deadline: time.Now().Add(acpSettingsOperationTimeout)}
		operationCtx, cancel := context.WithDeadline(deliveryCtx, reservation.Deadline)
		defer cancel()
		execution := executeACPSettingsChange(operationCtx, reservation, providerSessionID, stdin, responses, tracker, mu, nextID)
		if !execution.PublishResult {
			return errors.New("encrypted settings outcome unknown")
		}
		mu.Lock()
		defer mu.Unlock()
		latest, available := tracker.Current()
		if !available {
			return errors.New("encrypted settings readback unavailable")
		}
		execution = reconcileACPSettingsExecution(execution, latest)
		if err := publishACPSettingsCapability(writeFrame, cfg.SessionID, execution.State); err != nil {
			return errors.New("encrypted settings capability publication failed")
		}
		if err := publishACPSettingsEffective(writeFrame, cfg.SessionID, reservation, execution); err != nil {
			return errors.New("encrypted settings result publication failed")
		}
		if execution.TerminateProvider {
			return errors.New("encrypted settings provider state uncertain")
		}
		return nil
	})
	if err != nil || admission.State != "completed" {
		return errors.New("encrypted settings delivery failed")
	}
	status := protocol.AckAccepted
	if !admission.Execute {
		status = protocol.AckDuplicate
	}
	return writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: status})
}
