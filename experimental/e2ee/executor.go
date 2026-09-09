package e2ee

import (
	"context"
	"encoding/json"
	"time"
)

// CommandExecutor owns a serial execution lane. The daemon must keep exactly one
// owner per endpoint session; direct journal grant mutation bypasses this lane.
type CommandExecutor struct {
	lane    chan struct{}
	journal *CommandJournal
	vault   *SessionKeyVault
}

func NewCommandExecutor(journal *CommandJournal, vault *SessionKeyVault) (*CommandExecutor, error) {
	if journal == nil || vault == nil || journal.db != vault.db {
		return nil, ErrInvalid
	}
	return &CommandExecutor{make(chan struct{}, 1), journal, vault}, nil
}

func (e *CommandExecutor) acquire(ctx context.Context) error {
	select {
	case e.lane <- struct{}{}:
		if ctx.Err() != nil {
			<-e.lane
			return ErrJournal
		}
		return nil
	case <-ctx.Done():
		return ErrJournal
	}
}

// ExecuteWire is the endpoint ingress for a relayed command. Only plaintext
// released inside Execute's durable claim may reach the provider callback.
func (e *CommandExecutor) ExecuteWire(ctx context.Context, session, commandID, commandType string, data []byte, deliver func(context.Context, json.RawMessage) error) (CommandAdmission, error) {
	command, packet, err := DecodeCommandWire(session, commandID, commandType, data)
	if err != nil {
		return CommandAdmission{}, err
	}
	return e.Execute(ctx, command, packet, deliver)
}

// Execute validates/decrypts using the local current grant, durably claims the
// command, and only then calls the bounded provider delivery operation. Success
// means local delivery completed, not that the Provider turn succeeded.
func (e *CommandExecutor) Execute(ctx context.Context, command Context, packet ContentPacket, deliver func(context.Context, json.RawMessage) error) (CommandAdmission, error) {
	if command.Scope != "command" || deliver == nil || packet.Version != 1 || packet.Public.validate(command.Type) != nil {
		return CommandAdmission{}, ErrInvalid
	}
	switch command.Type {
	case "session.send", "permission.respond", "session.interrupt", "session.stop", "session.settings.change", "session.membership.change", "session.file.read", "session.file.list":
	default:
		return CommandAdmission{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := e.acquire(ctx); err != nil {
		return CommandAdmission{}, err
	}
	defer func() { <-e.lane }()
	key, err := e.vault.Load(ctx, command.Session, command.KeyID)
	if err != nil {
		return CommandAdmission{}, err
	}
	defer clear(key)
	var payload json.RawMessage
	admission, plaintext, err := e.journal.admitValidated(ctx, command, key, packet.Encrypted, func(data []byte) error {
		var err error
		payload, err = openPacketPlaintext(command.Type, packet.Public, data)
		return err
	})
	defer clear(plaintext)
	defer clear(payload)
	if err != nil {
		return CommandAdmission{}, err
	}
	if !admission.Execute {
		return admission, nil
	}
	state := "completed"
	if ctx.Err() != nil || deliver(ctx, payload) != nil {
		state = "outcome_unknown"
	}
	// Cancellation after claiming cannot erase ambiguity. Persist a terminal
	// delivery result with a separate bounded cleanup budget.
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer finishCancel()
	if err := e.journal.Finish(finishCtx, command.Session, command.MessageID, state); err != nil {
		return CommandAdmission{State: "outcome_unknown"}, err
	}
	return CommandAdmission{Execute: true, State: state}, nil
}

// ReplaceGrants serializes an authenticated local membership change with
// delivery. Receipt of revocation must wait until this method commits.
func (e *CommandExecutor) ReplaceGrants(ctx context.Context, session, keyID string, epoch int64, grants []DeviceGrant) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := e.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-e.lane }()
	return e.vault.TransitionSession(ctx, e.journal, session, keyID, epoch, grants)
}
