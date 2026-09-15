//go:build windows

package e2ee

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// weakenEndpointDACL replaces the object DACL with world full control so the
// owner-only verification has something it must refuse.
func weakenEndpointDACL(t *testing.T, path string) {
	t.Helper()
	everyone, err := windows.CreateWellKnownSid(windows.WinWorldSid)
	if err != nil {
		t.Fatal(err)
	}
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{{
		AccessPermissions: windows.GENERIC_ALL,
		AccessMode:        windows.GRANT_ACCESS,
		Inheritance:       windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  windows.TRUSTEE_IS_WELL_KNOWN_GROUP,
			TrusteeValue: windows.TrusteeValueFromSID(everyone),
		},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION, nil, nil, acl, nil); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsIdentityIsStableAndOwnerOnly(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	first, err := LoadOrCreateIdentity(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreateIdentity(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	if first.Device != second.Device || !bytes.Equal(first.SigningSeed, second.SigningSeed) || !bytes.Equal(first.WrappingPrivate, second.WrappingPrivate) {
		t.Fatal("identity changed between loads")
	}
	if _, err := os.Stat(filepath.Join(directory, "identity.pending")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging file survived publication: %v", err)
	}
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	directoryHandle, err := openEndpointDirectory(directory, sid)
	if err != nil {
		t.Fatalf("directory is not owner-only: %v", err)
	}
	windows.CloseHandle(directoryHandle)
	identityFile, err := openEndpointFile(filepath.Join(directory, "identity.json"), windows.GENERIC_READ, windows.OPEN_EXISTING, sid)
	if err != nil {
		t.Fatalf("identity file is not owner-only: %v", err)
	}
	identityFile.Close()
}

func TestWindowsEndpointVerificationRefusesForeignGrant(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	if _, err := LoadOrCreateIdentity(ctx, directory); err != nil {
		t.Fatal(err)
	}
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	if handle, err := openEndpointDirectory(directory, sid); err != nil {
		t.Fatalf("pristine directory rejected: %v", err)
	} else {
		windows.CloseHandle(handle)
	}
	weakenEndpointDACL(t, directory)
	if handle, err := openEndpointDirectory(directory, sid); err == nil {
		windows.CloseHandle(handle)
		t.Fatal("world-writable endpoint directory accepted")
	}
	// LoadOrCreateIdentity repairs the directory instead of failing, so the
	// invariant holds again after it runs.
	if _, err := LoadOrCreateIdentity(ctx, directory); err != nil {
		t.Fatalf("identity repair failed: %v", err)
	}
	if handle, err := openEndpointDirectory(directory, sid); err != nil {
		t.Fatalf("directory not repaired: %v", err)
	} else {
		windows.CloseHandle(handle)
	}
}

func TestWindowsIdentityLockIsExclusive(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	if _, err := LoadOrCreateIdentity(ctx, directory); err != nil {
		t.Fatal(err)
	}
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	held, err := openEndpointFile(filepath.Join(directory, "identity.lock"), windows.GENERIC_READ|windows.GENERIC_WRITE, windows.OPEN_ALWAYS, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := lockEndpointFile(ctx, held); err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if _, err := LoadOrCreateIdentity(blocked, directory); !errors.Is(err, ErrJournal) {
		t.Fatalf("held identity lock was ignored: %v", err)
	}
}

func TestWindowsLocalDatabaseOpensOwnerOnlyAndRefusesForeignGrant(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "endpoint")
	if _, err := LoadOrCreateIdentity(ctx, directory); err != nil {
		t.Fatal(err)
	}
	database, err := OpenLocalDatabase(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewCommandJournal(ctx, database)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if journal == nil {
		database.Close()
		t.Fatal("command journal is nil")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	sid, err := windowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	databaseFile, err := openEndpointFile(filepath.Join(directory, "endpoint.db"), windows.GENERIC_READ, windows.OPEN_EXISTING, sid)
	if err != nil {
		t.Fatalf("database file is not owner-only: %v", err)
	}
	databaseFile.Close()
	weakenEndpointDACL(t, filepath.Join(directory, "endpoint.db"))
	if reopened, err := OpenLocalDatabase(ctx, directory); err == nil {
		reopened.Close()
		t.Fatal("world-writable endpoint database accepted")
	}
}
