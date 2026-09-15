//go:build windows

package e2ee

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows"
)

// LoadOrCreateIdentity uses a dedicated private directory below a trusted parent,
// protected on Windows by an explicit owner-only DACL instead of POSIX mode bits.
// It protects against other OS users, not malicious same-UID code or host
// administrators. The caller must not let Provider inherit any private material.
func LoadOrCreateIdentity(ctx context.Context, directory string) (LocalIdentity, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !filepath.IsAbs(directory) {
		return LocalIdentity{}, ErrInvalid
	}
	sid, err := windowsUserSID()
	if err != nil {
		return LocalIdentity{}, err
	}
	if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return LocalIdentity{}, fmt.Errorf("identity directory create: %w", ErrJournal)
	}
	// Tighten before any secret is written so inherited entries cannot widen it.
	if err := applyOwnerOnlyDACL(directory, sid); err != nil {
		return LocalIdentity{}, err
	}
	directoryHandle, err := openEndpointDirectory(directory, sid)
	if err != nil {
		return LocalIdentity{}, err
	}
	defer windows.CloseHandle(directoryHandle)
	lock, err := openEndpointFile(filepath.Join(directory, "identity.lock"), windows.GENERIC_READ|windows.GENERIC_WRITE, windows.OPEN_ALWAYS, sid)
	if err != nil {
		return LocalIdentity{}, fmt.Errorf("identity lock open: %w", err)
	}
	defer lock.Close()
	if err := lockEndpointFile(ctx, lock); err != nil {
		return LocalIdentity{}, err
	}
	if ctx.Err() != nil {
		return LocalIdentity{}, fmt.Errorf("identity operation cancelled: %w", ErrJournal)
	}
	identityPath := filepath.Join(directory, "identity.json")
	identityFile, err := openEndpointFile(identityPath, windows.GENERIC_READ, windows.OPEN_EXISTING, sid)
	if err == nil {
		defer identityFile.Close()
		data, err := io.ReadAll(io.LimitReader(identityFile, 2049))
		if err != nil {
			return LocalIdentity{}, fmt.Errorf("identity file read: %w", ErrJournal)
		}
		defer clear(data)
		identity, err := decodeLocalIdentity(data)
		if err != nil {
			return LocalIdentity{}, fmt.Errorf("identity file invalid: %w", ErrJournal)
		}
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) && !errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
		return LocalIdentity{}, fmt.Errorf("identity file open: %w", err)
	}
	identity, err := NewLocalIdentity()
	if err != nil {
		return LocalIdentity{}, err
	}
	data, err := json.Marshal(identity)
	if err != nil {
		return LocalIdentity{}, fmt.Errorf("identity encoding: %w", ErrJournal)
	}
	defer clear(data)
	// A stale staging file indicates interrupted initialization; never silently
	// replace it with a new identity whose trust differs from an earlier attempt.
	staging, err := openEndpointFile(filepath.Join(directory, "identity.pending"), windows.GENERIC_WRITE, windows.CREATE_NEW, sid)
	if err != nil {
		return LocalIdentity{}, fmt.Errorf("identity staging open: %w", ErrJournal)
	}
	if _, err = staging.Write(data); err != nil {
		staging.Close()
		return LocalIdentity{}, fmt.Errorf("identity staging write: %w", ErrJournal)
	}
	if staging.Sync() != nil {
		staging.Close()
		return LocalIdentity{}, fmt.Errorf("identity staging sync: %w", ErrJournal)
	}
	if staging.Close() != nil {
		return LocalIdentity{}, fmt.Errorf("identity staging close: %w", ErrJournal)
	}
	if ctx.Err() != nil {
		return LocalIdentity{}, fmt.Errorf("identity publication cancelled: %w", ErrJournal)
	}
	// Publication refuses to replace any concurrently created identity.
	if err := publishEndpointFile(filepath.Join(directory, "identity.pending"), identityPath); err != nil {
		return LocalIdentity{}, err
	}
	return identity, nil
}
