package e2ee

import (
	"context"
	"crypto/ed25519"
	"time"
)

// Membership retries may use their original epoch's ciphertext after rotation.
// Opening a historical key grants no mutation authority: ApplyMembership checks
// current controller+epoch transactionally, or returns an exact committed receipt.
func (e *CommandExecutor) ApplyMembershipWire(ctx context.Context, registry *DeviceRegistry, session, id string, wire []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if registry == nil || registry.db != e.vault.db {
		return ErrUnauthorized
	}
	command, packet, err := DecodeCommandWire(session, id, "session.membership.change", wire)
	if err != nil {
		return err
	}
	device, err := registry.Device(ctx, command.Sender)
	if err != nil {
		return ErrUnauthorized
	}
	public, err := decode(device.SigningKey, 32, 32)
	if err != nil {
		return err
	}
	key, err := e.vault.Load(ctx, session, command.KeyID)
	if err != nil {
		return err
	}
	defer clear(key)
	payload, err := OpenPacket(command, key, ed25519.PublicKey(public), packet)
	defer clear(payload)
	if err != nil {
		return err
	}
	request, err := DecodeSessionMembershipChange(payload)
	if err != nil || request.Session != session || request.Device != command.Sender {
		return ErrUnauthorized
	}
	return e.ApplyMembership(ctx, registry, request)
}
