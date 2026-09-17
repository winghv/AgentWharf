package main

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

// deliverEncryptedOfficialCommand releases the plaintext only inside the
// durable executor callback, then injects the instruction into the running
// official CLI PTY. It mirrors deliverEncryptedACPCommand for the interactive
// transcript-mirroring entrypoint, which otherwise cannot accept a command from
// the Console once Own Machine requires encryption.
func deliverEncryptedOfficialCommand(ctx context.Context, cfg wrapConfig, connection *hubConnection, command *protocol.Command, writeFrame func(protocol.Frame) error, ptmx io.Writer, ptyMu *sync.Mutex, process *os.Process, stopInProgress *atomic.Bool, injected *injectedPromptTracker, questions *questionCache) error {
	if cfg.e2eeRuntime == nil || connection == nil {
		return errors.New("required encrypted command executor is unavailable")
	}
	if command == nil || (command.Type != protocol.CommandSessionSend && command.Type != protocol.CommandPermissionRespond && command.Type != protocol.CommandSessionStop) {
		return errors.New("encrypted official command is unavailable")
	}
	if command.Type == protocol.CommandSessionStop {
		admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(_ context.Context, _ *protocol.Command) error {
			if process == nil {
				return errors.New("official agent process is unavailable")
			}
			killErr := process.Kill()
			if errors.Is(killErr, os.ErrProcessDone) {
				killErr = nil
			}
			if killErr == nil && stopInProgress != nil {
				stopInProgress.Store(true)
			}
			return killErr
		})
		if err != nil || admission.State != "completed" {
			return errors.New("encrypted stop outcome unknown")
		}
		if !admission.Execute {
			return writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckDuplicate})
		}
		receiptCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return acknowledgeRunControl(receiptCtx, command, connection.read, writeFrame, cfg, "stop", "ended", nil)
	}
	var promptToConfirm string
	admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(deliveryCtx context.Context, decoded *protocol.Command) error {
		if err := deliveryCtx.Err(); err != nil {
			return err
		}
		if decoded.Type == protocol.CommandPermissionRespond {
			ptyMu.Lock()
			defer ptyMu.Unlock()
			return forwardQuestionAnswer(ptmx, questions, decoded.Payload)
		}
		prompt, err := acpPromptFromSessionSend(decoded.Payload)
		if err != nil {
			return err
		}
		var text strings.Builder
		for _, part := range prompt {
			if value, ok := part["text"].(string); ok {
				text.WriteString(value)
			}
		}
		if text.Len() == 0 {
			return errors.New("empty prompt")
		}
		promptText := text.String()
		if injected != nil {
			injected.add(promptText)
		}
		ptyMu.Lock()
		writeErr := writeOfficialCLIPrompt(ptmx, promptText)
		ptyMu.Unlock()
		if writeErr != nil {
			if injected != nil {
				injected.remove(promptText)
			}
			return writeErr
		}
		promptToConfirm = promptText
		return nil
	})
	if err != nil {
		if writeErr := writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckRejected, Reason: err.Error()}); writeErr != nil {
			return writeErr
		}
		return nil
	}
	if admission.State != "completed" {
		return errors.New("encrypted official delivery outcome unknown")
	}
	if !admission.Execute {
		return writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckDuplicate})
	}
	if err := writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted}); err != nil {
		return err
	}
	if promptToConfirm != "" {
		go confirmOfficialCLIPrompt(ctx, ptmx, ptyMu, cfg.SessionID, promptToConfirm, injected, writeFrame, defaultOfficialPromptInjection)
	}
	return nil
}
