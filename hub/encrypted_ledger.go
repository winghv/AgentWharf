package hub

import (
	"context"
	"github.com/winghv/agentwharf/store"
)

// Bind inherited lifecycle calls to the opaque methods, not legacy defaults.
type encryptedCommandLedger struct {
	store.EncryptedCommandLedgerStore
}

func (s encryptedCommandLedger) CommitPendingCommand(ctx context.Context, session string, authority store.CommandAuthority, event store.PendingEvent, request store.PendingCommandRequest) (store.PendingCommandCommit, error) {
	return s.CommitEncryptedPendingCommand(ctx, session, authority, event, request)
}

func (s encryptedCommandLedger) ListPendingCommands(ctx context.Context, session string, authority store.CommandAuthority) ([]store.PendingCommand, error) {
	return s.ListEncryptedPendingCommands(ctx, session, authority)
}
func (s encryptedCommandLedger) ClaimPendingCommand(ctx context.Context, session string, authority store.CommandAuthority, id string) (store.PendingCommandClaim, error) {
	return s.ClaimEncryptedPendingCommand(ctx, session, authority, id)
}
func (s encryptedCommandLedger) ResolvePendingCommand(ctx context.Context, session string, authority store.CommandAuthority, id string, status store.PendingCommandStatus) (store.PendingCommand, error) {
	return s.ResolveEncryptedPendingCommand(ctx, session, authority, id, status)
}
func (s encryptedCommandLedger) ResolvePendingCommandUnknown(ctx context.Context, session, id string) (store.PendingCommand, error) {
	return s.ResolveEncryptedPendingCommandUnknown(ctx, session, id)
}
