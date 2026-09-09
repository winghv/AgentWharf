//go:build !linux && !darwin

package e2ee

import "context"

// Unsupported platforms must not fall back to world-readable or ephemeral keys.
// Their protected native storage backend is required before activation.
func LoadOrCreateIdentity(context.Context, string) (LocalIdentity, error) {
	return LocalIdentity{}, ErrJournal
}
