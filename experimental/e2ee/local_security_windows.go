//go:build windows

package e2ee

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows has no POSIX mode bits, so the unix 0700/0600 invariant is expressed
// as an explicit owner-only DACL: the security descriptor of every endpoint
// directory, identity file and database file must grant access to the current
// user SID alone, the object must be owned by that SID, and the object must be a
// single-link file (or directory) rather than a reparse point. This protects
// against other OS users, not malicious same-UID code or host administrators.
// These checks are best-effort exact: anything unexpected fails closed.

var (
	advapi32DLL = windows.NewLazySystemDLL("advapi32.dll")
	procGetAce  = advapi32DLL.NewProc("GetAce")
)

// windowsUserSID returns a Go-owned copy of the current process token user SID.
func windowsUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("resolve endpoint user: %w", ErrJournal)
	}
	sid, err := user.User.Sid.Copy()
	if err != nil {
		return nil, fmt.Errorf("copy endpoint user sid: %w", ErrJournal)
	}
	return sid, nil
}

// applyOwnerOnlyDACL replaces the object DACL with a single inheritable
// full-control grant for the endpoint user and stops the object inheriting its
// parent DACL, so entries created afterwards stay owner-only.
func applyOwnerOnlyDACL(path string, sid *windows.SID) error {
	var pinner runtime.Pinner
	pinner.Pin(sid)
	defer pinner.Unpin()
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{{
		AccessPermissions: windows.GENERIC_ALL,
		AccessMode:        windows.GRANT_ACCESS,
		Inheritance:       windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  windows.TRUSTEE_IS_USER,
			TrusteeValue: windows.TrusteeValueFromSID(sid),
		},
	}}, nil)
	if err != nil || acl == nil {
		return fmt.Errorf("build endpoint dacl: %w", ErrJournal)
	}
	if err := windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		acl,
		nil,
	); err != nil {
		return fmt.Errorf("apply endpoint dacl: %w", ErrJournal)
	}
	return nil
}

// aclIsOwnerOnly reports whether every granting ACE belongs to sid. A missing or
// empty DACL grants nothing but also cannot be the intended object state, and an
// unreadable entry is treated as a foreign grant.
func aclIsOwnerOnly(dacl *windows.ACL, sid *windows.SID) bool {
	if dacl == nil || dacl.AceCount == 0 {
		return false
	}
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		ret, _, _ := procGetAce.Call(
			uintptr(unsafe.Pointer(dacl)),
			uintptr(index),
			uintptr(unsafe.Pointer(&ace)),
		)
		if ret == 0 || ace == nil {
			return false
		}
		switch ace.Header.AceType {
		case windows.ACCESS_DENIED_ACE_TYPE:
			continue
		case windows.ACCESS_ALLOWED_ACE_TYPE:
		default:
			// An object or audit ACE is not a state this module ever writes.
			return false
		}
		// ACCESS_ALLOWED_ACE stores the SID immediately after its header and mask.
		aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !aceSID.Equals(sid) {
			return false
		}
	}
	return true
}

// verifyOwnerOnlyHandle requires the open handle to be owned by sid, to carry an
// owner-only DACL, to be a single-link object and not to be a reparse point.
// Callers pass a handle opened with FILE_FLAG_OPEN_REPARSE_POINT so a symlink or
// junction is inspected instead of silently followed.
func verifyOwnerOnlyHandle(handle windows.Handle, sid *windows.SID, wantDirectory bool) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return fmt.Errorf("inspect endpoint object: %w", ErrJournal)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("endpoint object is a reparse point: %w", ErrJournal)
	}
	isDirectory := info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0
	if isDirectory != wantDirectory {
		return fmt.Errorf("endpoint object has the wrong type: %w", ErrJournal)
	}
	if !wantDirectory && info.NumberOfLinks != 1 {
		return fmt.Errorf("endpoint file has multiple links: %w", ErrJournal)
	}
	descriptor, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil || descriptor == nil {
		return fmt.Errorf("read endpoint security: %w", ErrJournal)
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil || !owner.Equals(sid) {
		return fmt.Errorf("endpoint object owner mismatch: %w", ErrJournal)
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || defaulted || !aclIsOwnerOnly(dacl, sid) {
		return fmt.Errorf("endpoint object dacl mismatch: %w", ErrJournal)
	}
	return nil
}

// openEndpointObject opens one path component with reparse points surfaced to the
// caller. access and disposition follow the CreateFile contract.
func openEndpointObject(path string, access, disposition uint32, attributes uint32) (windows.Handle, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, fmt.Errorf("encode endpoint path: %w", ErrJournal)
	}
	handle, err := windows.CreateFile(
		name,
		access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		disposition,
		attributes|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return 0, err
	}
	return handle, nil
}

// openEndpointDirectory opens the endpoint directory itself and enforces the
// owner-only invariant on the handle.
func openEndpointDirectory(directory string, sid *windows.SID) (windows.Handle, error) {
	handle, err := openEndpointObject(
		directory,
		windows.GENERIC_READ,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
	)
	if err != nil {
		return 0, fmt.Errorf("open endpoint directory: %w", ErrJournal)
	}
	if err := verifyOwnerOnlyHandle(handle, sid, true); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	return handle, nil
}

// openEndpointFile opens one file below the endpoint directory and enforces the
// owner-only invariant on the handle. It returns a file the caller owns.
func openEndpointFile(path string, access, disposition uint32, sid *windows.SID) (*os.File, error) {
	handle, err := openEndpointObject(path, access, disposition, windows.FILE_ATTRIBUTE_NORMAL)
	if err != nil {
		return nil, err
	}
	if err := verifyOwnerOnlyHandle(handle, sid, false); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

// publishEndpointFile publishes staging as target without replacing any
// concurrently created target, matching the unix link-and-unlink publication.
func publishEndpointFile(staging, target string) error {
	from, err := windows.UTF16PtrFromString(staging)
	if err != nil {
		return fmt.Errorf("encode staging path: %w", ErrJournal)
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return fmt.Errorf("encode target path: %w", ErrJournal)
	}
	if err := windows.MoveFileEx(from, to, 0); err != nil {
		return fmt.Errorf("publish endpoint file: %w", ErrJournal)
	}
	return nil
}

// lockEndpointFile takes a non-blocking exclusive lock over the whole file and
// retries until ctx expires, mirroring the unix bounded flock acquisition. The
// lock is released when the handle closes.
func lockEndpointFile(ctx context.Context, file *os.File) error {
	var overlapped windows.Overlapped
	for {
		if ctx.Err() != nil {
			return fmt.Errorf("endpoint lock timeout: %w", ErrJournal)
		}
		err := windows.LockFileEx(
			windows.Handle(file.Fd()),
			windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
			0,
			1,
			0,
			&overlapped,
		)
		if err == nil {
			return nil
		}
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return fmt.Errorf("endpoint lock acquire: %w", ErrJournal)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("endpoint lock timeout: %w", ErrJournal)
		case <-timer.C:
		}
	}
}
