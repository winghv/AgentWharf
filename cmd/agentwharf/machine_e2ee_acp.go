package main

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/winghv/agentwharf/protocol"
)

func deliverEncryptedACPPrompt(ctx context.Context, cfg wrapConfig, command *protocol.Command, stdin io.Writer, providerSessionID string, nextID *int64, writeFrame func(protocol.Frame) error) error {
	return deliverEncryptedACPCommand(ctx, cfg, command, stdin, providerSessionID, nextID, nil, nil, writeFrame)
}

func deliverEncryptedACPCommand(ctx context.Context, cfg wrapConfig, command *protocol.Command, stdin io.Writer, providerSessionID string, nextID *int64, pending map[string]acpPendingPermission, permissionMu *sync.Mutex, writeFrame func(protocol.Frame) error) error {
	if cfg.e2eeRuntime == nil {
		return errors.New("required encrypted command executor is unavailable")
	}
	if command == nil || (command.Type != protocol.CommandSessionSend && command.Type != protocol.CommandPermissionRespond && command.Type != protocol.CommandSessionInterrupt && command.Type != protocol.CommandFileRead && command.Type != protocol.CommandFileList) {
		return errors.New("encrypted ACP control command is unavailable")
	}
	if command.Type == protocol.CommandFileRead {
		return deliverEncryptedFileRead(ctx, cfg, command, writeFrame)
	}
	if command.Type == protocol.CommandFileList {
		return deliverEncryptedFileList(ctx, cfg, command, writeFrame)
	}
	admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(ctx context.Context, decoded *protocol.Command) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if decoded.Type == protocol.CommandSessionInterrupt {
			// Notification form per ACP (see writeACPNotification).
			if err := writeACPNotification(stdin, "session/cancel", map[string]any{"sessionId": providerSessionID}); err != nil {
				return err
			}
			return nil
		}
		if decoded.Type == protocol.CommandPermissionRespond {
			if pending == nil || permissionMu == nil {
				return errors.New("ACP permission state unavailable")
			}
			permission, result, err := acpPermissionResult(decoded.Payload, pending, permissionMu)
			if err != nil {
				return err
			}
			return writeACPResult(stdin, permission.RPCID, result)
		}
		prompt, err := acpPromptFromSessionSend(decoded.Payload)
		if err != nil {
			return err
		}
		if err := writeACPRequest(stdin, *nextID, "session/prompt", map[string]any{"sessionId": providerSessionID, "prompt": prompt}); err != nil {
			return err
		}
		*nextID++
		return nil
	})
	if err != nil {
		if encryptedEpochStale(err) {
			return writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckRejected, Reason: "epoch_stale"})
		}
		return errors.New("encrypted ACP delivery failed")
	}
	if admission.State != "completed" {
		return errors.New("encrypted ACP delivery outcome unknown")
	}
	status := protocol.AckAccepted
	if !admission.Execute {
		status = protocol.AckDuplicate
	}
	if err := writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: status}); err != nil {
		return err
	}
	// The Provider turn is now running; publish the authoritative busy state
	// (the mapper publishes ready when the prompt RPC response arrives).
	if status == protocol.AckAccepted && command.Type == protocol.CommandSessionSend {
		return publishACPWorkingState(writeFrame, cfg, providerSessionID, "busy")
	}
	return nil
}
