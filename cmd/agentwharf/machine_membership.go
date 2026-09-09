package main

import (
	"context"
	"errors"

	"github.com/winghv/agentwharf/protocol"
)

func deliverEncryptedMembership(ctx context.Context, cfg wrapConfig, command *protocol.Command, write func(protocol.Frame) error) error {
	if cfg.e2eeRuntime == nil || command == nil || command.Type != protocol.CommandMembershipChange || command.SessionID != cfg.SessionID || write == nil {
		return errors.New("encrypted membership runtime unavailable")
	}
	if err := cfg.e2eeRuntime.executor.ApplyMembershipWire(ctx, cfg.e2eeRuntime.registry, cfg.SessionID, command.CommandID, command.Payload); err != nil {
		return errors.New("encrypted membership change rejected")
	}
	return write(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckAccepted})
}
