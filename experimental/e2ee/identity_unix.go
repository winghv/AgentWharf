//go:build linux || darwin

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

	"golang.org/x/sys/unix"
)

// LoadOrCreateIdentity uses a dedicated private directory below a trusted parent.
// It protects against other OS users, not malicious same-UID code or host root.
// The caller must not let Provider inherit any returned private material.
func LoadOrCreateIdentity(ctx context.Context, directory string) (LocalIdentity, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !filepath.IsAbs(directory) {
		return LocalIdentity{}, ErrInvalid
	}
	if err := os.Mkdir(directory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return LocalIdentity{}, fmt.Errorf("identity directory create: %w", ErrJournal)
	}
	fd, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return LocalIdentity{}, fmt.Errorf("identity directory open: %w", ErrJournal)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0700 {
		return LocalIdentity{}, fmt.Errorf("identity directory permissions: %w", ErrJournal)
	}
	lockFD, err := openIdentityLock(ctx, fd)
	if err != nil {
		return LocalIdentity{}, fmt.Errorf("identity lock open: %w", err)
	}
	defer unix.Close(lockFD)
	if !privateRegular(lockFD) {
		return LocalIdentity{}, fmt.Errorf("identity lock permissions: %w", ErrJournal)
	}
	for {
		err = unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if err != unix.EWOULDBLOCK && err != unix.EAGAIN {
			return LocalIdentity{}, fmt.Errorf("identity lock acquire: %w", ErrJournal)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return LocalIdentity{}, fmt.Errorf("identity lock timeout: %w", ErrJournal)
		case <-timer.C:
		}
	}
	defer unix.Flock(lockFD, unix.LOCK_UN)
	if ctx.Err() != nil {
		return LocalIdentity{}, fmt.Errorf("identity operation cancelled: %w", ErrJournal)
	}
	keyFD, err := unix.Openat(fd, "identity.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err == nil {
		f := os.NewFile(uintptr(keyFD), "endpoint-identity")
		defer f.Close()
		if !privateRegular(keyFD) {
			return LocalIdentity{}, fmt.Errorf("identity file permissions: %w", ErrJournal)
		}
		data, err := io.ReadAll(io.LimitReader(f, 2049))
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
	if err != unix.ENOENT {
		return LocalIdentity{}, fmt.Errorf("identity file open: %w", ErrJournal)
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
	stagingFD, err := unix.Openat(fd, "identity.pending", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return LocalIdentity{}, fmt.Errorf("identity staging open: %w", ErrJournal)
	}
	staging := os.NewFile(uintptr(stagingFD), "endpoint-identity-pending")
	if !privateRegular(stagingFD) {
		staging.Close()
		return LocalIdentity{}, fmt.Errorf("identity staging permissions: %w", ErrJournal)
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
	// Link publishes without replacing any concurrently created identity.
	if unix.Linkat(fd, "identity.pending", fd, "identity.json", 0) != nil {
		return LocalIdentity{}, fmt.Errorf("identity publication link: %w", ErrJournal)
	}
	if unix.Unlinkat(fd, "identity.pending", 0) != nil {
		return LocalIdentity{}, fmt.Errorf("identity staging unlink: %w", ErrJournal)
	}
	if unix.Fsync(fd) != nil {
		return LocalIdentity{}, fmt.Errorf("identity directory sync: %w", ErrJournal)
	}
	return identity, nil
}

func openIdentityLock(ctx context.Context, directoryFD int) (int, error) {
	for {
		if ctx.Err() != nil {
			return -1, ErrJournal
		}
		fd, err := unix.Openat(directoryFD, "identity.lock", unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
		if err == nil {
			return fd, nil
		}
		if err != unix.ENOENT {
			return -1, ErrJournal
		}
		fd, err = unix.Openat(directoryFD, "identity.lock", unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		if err == nil {
			return fd, nil
		}
		if err != unix.EEXIST && err != unix.ENOENT {
			return -1, ErrJournal
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return -1, ErrJournal
		case <-timer.C:
		}
	}
}

func privateRegular(fd int) bool {
	var stat unix.Stat_t
	return unix.Fstat(fd, &stat) == nil && stat.Uid == uint32(os.Geteuid()) && stat.Mode&unix.S_IFMT == unix.S_IFREG && stat.Mode&0777 == 0600 && stat.Nlink == 1
}
