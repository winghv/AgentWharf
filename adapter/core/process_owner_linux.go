//go:build linux
package core
import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"golang.org/x/sys/unix"
)
var (
	ErrProcessOwnershipUnavailable = errors.New("process ownership is unavailable")
	ErrProcessOwnershipQuarantined = errors.New("process ownership is quarantined")
	ErrProcessOwnershipUncertain   = errors.New("process ownership is uncertain")
)
const (
	ownershipManifestSchema = 1
	defaultOwnershipTimeout = 5 * time.Second
	defaultOwnershipLimit   = 64
)
type LinuxProcessOwnershipConfig struct { Root string; ProviderUID uint32; CleanupTimeout time.Duration; MaxTrackedProcs int }
type linuxProcessIdentity struct { PID int `json:"pid"`; StartTime uint64 `json:"start_time"` }
type linuxProcessRecord struct { ID string `json:"id"`; PID int `json:"pid"`; StartTime uint64 `json:"start_time"`; Digest string `json:"digest"` }
type linuxOwnershipManifest struct { Schema int `json:"schema"`; Version uint64 `json:"version"`; Runtime string `json:"runtime"`; State string `json:"state"`; Attempt int `json:"attempt"`; Records []linuxProcessRecord `json:"records"` }
type linuxOwnershipRecordFile struct { Version uint64 `json:"version"`; Record linuxProcessRecord `json:"record"` }
type linuxTrackedProcess struct { identity linuxProcessIdentity; pidfd int }
type LinuxProcessTreeOwnership struct { mu sync.Mutex; root, manifest string; timeout time.Duration; maxProcs int; runtimeID, state string; attempt int; rootProcess linuxProcessIdentity; baseline map[linuxProcessIdentity]struct{}; tracked map[linuxProcessIdentity]*linuxTrackedProcess; quiescent, quarantined bool; durable []linuxProcessRecord; manifestVersion uint64 }
func InitializeLinuxProcessOwnershipRoot(root string, providerUID uint32) error {
	_, rootErr := os.Lstat(root)
	root, err := protectedOwnershipRoot(root, providerUID, true)
	if err != nil {
		return err
	}
	if err := validateOwnershipRoot(root); err != nil {
		return err
	}
	manifest := filepath.Join(root, "manifest.json")
	if _, err := os.Stat(manifest); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect ownership manifest: %w", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil { return fmt.Errorf("inspect ownership root: %w", err) }
	if rootErr == nil || len(entries) != 0 { return ErrProcessOwnershipUncertain }
	runtimeID, err := newOpaqueOwnershipID()
	if err != nil {
		return err
	}
	return writeOwnershipManifest(root, linuxOwnershipManifest{
		Schema: ownershipManifestSchema, Version: 1, Runtime: runtimeID, State: "clean",
		Records: []linuxProcessRecord{},
	})
}
func NewLinuxProcessTreeOwnership(cfg LinuxProcessOwnershipConfig) (*LinuxProcessTreeOwnership, error) {
	root, err := protectedOwnershipRoot(cfg.Root, cfg.ProviderUID, false)
	if err != nil {
		return nil, err
	}
	if err := validateOwnershipRoot(root); err != nil {
		return nil, err
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return nil, fmt.Errorf("enable child subreaper: %w", err)
	}
	manifestPath := filepath.Join(root, "manifest.json")
	manifest, err := readOwnershipManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	owner := &LinuxProcessTreeOwnership{
		root: root, manifest: manifestPath, runtimeID: manifest.Runtime, manifestVersion: manifest.Version,
		timeout: cfg.CleanupTimeout, maxProcs: cfg.MaxTrackedProcs,
		state: manifest.State, baseline: make(map[linuxProcessIdentity]struct{}),
		tracked: make(map[linuxProcessIdentity]*linuxTrackedProcess), durable: manifest.Records,
	}
	if owner.timeout <= 0 {
		owner.timeout = defaultOwnershipTimeout
	}
	if owner.maxProcs <= 0 {
		owner.maxProcs = defaultOwnershipLimit
	}
	if manifest.State != "clean" {
		owner.quarantined = true
		owner.state = "quarantine"
		if err := owner.persistLocked(); err != nil {
			return nil, err
		}
		return owner, ErrProcessOwnershipQuarantined
	}
	return owner, nil
}
func (o *LinuxProcessTreeOwnership) PrepareStart(ctx context.Context, attempt int) error {
	if o == nil || attempt < 1 {
		return ErrProcessOwnershipUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.quarantined || o.state == "quarantine" {
		return ErrProcessOwnershipQuarantined
	}
	if err := o.reloadCleanLocked(); err != nil {
		return err
	}
	if err := o.reconcilePreviousLocked(); err != nil {
		return o.quarantineLocked(err)
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	baseline, err := o.currentChildrenLocked()
	if err != nil {
		return o.quarantineLocked(err)
	}
	o.baseline = baseline
	o.attempt = attempt
	o.state = "prepared"
	o.quiescent = false
	return o.persistLocked()
}
func (o *LinuxProcessTreeOwnership) AbortStart(_ context.Context, attempt int) error {
	if o == nil {
		return ErrProcessOwnershipUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.quarantined {
		return ErrProcessOwnershipQuarantined
	}
	if o.state != "prepared" || (attempt > 0 && attempt != o.attempt) {
		return nil
	}
	if err := o.ensureNoUnexpectedChildrenLocked(); err != nil {
		return o.quarantineLocked(err)
	}
	o.state, o.attempt, o.quiescent = "clean", 0, true
	return o.persistLocked()
}
func (o *LinuxProcessTreeOwnership) ObserveStarted(ctx context.Context, event ProcessEvent) error {
	if o == nil || event.Type != ProcessEventStarted || event.PID <= 0 {
		return ErrProcessOwnershipUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.quarantined || o.state != "prepared" || event.Attempt != o.attempt {
		return o.quarantineLocked(ErrProcessOwnershipUncertain)
	}
	identity, _, _, err := readLinuxProcessStat(event.PID)
	if err != nil {
		return o.quarantineLocked(err)
	}
	tracked, err := o.inventoryLocked(identity)
	if err != nil {
		return o.quarantineLocked(err)
	}
	o.tracked = tracked
	o.rootProcess = identity
	o.state = "started"
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return o.quarantineLocked(err)
		}
	}
	return o.persistLocked()
}
func (o *LinuxProcessTreeOwnership) Quiesce(ctx context.Context) error {
	if o == nil {
		return ErrProcessOwnershipUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.quarantined {
		return ErrProcessOwnershipQuarantined
	}
	if o.state == "clean" {
		if err := o.reloadCleanLocked(); err != nil {
			return o.quarantineLocked(err)
		}
		o.quiescent = true
		return nil
	}
	if o.state == "prepared" {
		if err := o.ensureNoUnexpectedChildrenLocked(); err != nil {
			return o.quarantineLocked(err)
		}
		o.state, o.attempt, o.quiescent = "clean", 0, true
		return o.persistLocked()
	}
	if o.state != "started" || len(o.tracked) == 0 {
		return o.quarantineLocked(ErrProcessOwnershipUncertain)
	}
	if err := o.refreshInventoryLocked(); err != nil {
		return o.quarantineLocked(err)
	}
	deadline := time.Now().Add(o.timeout)
	if ctx != nil {
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
	}
	termDeadline := time.Now().Add(o.timeout / 2)
	if termDeadline.After(deadline) {
		termDeadline = deadline
	}
	if err := o.signalTrackedLocked(unix.SIGTERM); err != nil {
		return o.quarantineLocked(err)
	}
	if !o.waitTrackedLocked(termDeadline) {
		if err := o.refreshInventoryLocked(); err != nil {
			return o.quarantineLocked(err)
		}
		if err := o.signalTrackedLocked(unix.SIGKILL); err != nil {
			return o.quarantineLocked(err)
		}
		if !o.waitTrackedLocked(deadline) {
			return o.quarantineLocked(ErrProcessOwnershipUncertain)
		}
	}
	return o.finalizeQuiescenceLocked(o.refreshInventoryLocked)
}
func (o *LinuxProcessTreeOwnership) finalizeQuiescenceLocked(refresh func() error) error {
	if err := refresh(); err != nil {
		return o.quarantineLocked(err)
	}
	o.reapTrackedDescendantsLocked()
	if err := o.ensureNoUnexpectedChildrenLocked(); err != nil {
		return o.quarantineLocked(err)
	}
	o.closeTrackedLocked()
	o.tracked = make(map[linuxProcessIdentity]*linuxTrackedProcess)
	o.state, o.attempt, o.quiescent = "clean", 0, true
	return o.persistLocked()
}
func (o *LinuxProcessTreeOwnership) Quarantine(_ context.Context) error {
	if o == nil {
		return ErrProcessOwnershipUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.quarantineLocked(ErrProcessOwnershipQuarantined)
}
func (o *LinuxProcessTreeOwnership) Quiescent() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.quiescent && !o.quarantined
}
func (o *LinuxProcessTreeOwnership) quarantineLocked(cause error) error {
	if len(o.tracked) > 0 {
		o.durable = o.recordsFromTrackedLocked()
		o.closeTrackedLocked()
		o.tracked = make(map[linuxProcessIdentity]*linuxTrackedProcess)
	}
	o.quarantined, o.quiescent, o.state = true, false, "quarantine"
	if err := o.persistLocked(); err != nil {
		return err
	}
	if cause == nil {
		return ErrProcessOwnershipQuarantined
	}
	return fmt.Errorf("%w: %v", ErrProcessOwnershipQuarantined, cause)
}
func (o *LinuxProcessTreeOwnership) reloadCleanLocked() error {
	manifest, err := readOwnershipManifest(o.manifest)
	if err != nil {
		return fmt.Errorf("reload ownership manifest: %w", err)
	}
	if manifest.Runtime != o.runtimeID || manifest.State != "clean" || len(manifest.Records) != 0 {
		return ErrProcessOwnershipUncertain
	}
	return nil
}
func (o *LinuxProcessTreeOwnership) reconcilePreviousLocked() error {
	if len(o.tracked) == 0 && o.state == "clean" {
		return nil
	}
	if o.state != "started" || len(o.tracked) == 0 {
		return ErrProcessOwnershipUncertain
	}
	deadline := time.Now().Add(o.timeout)
	if !o.waitTrackedLocked(deadline) {
		return ErrProcessOwnershipUncertain
	}
	o.reapTrackedDescendantsLocked()
	if err := o.ensureNoUnexpectedChildrenLocked(); err != nil {
		return err
	}
	o.closeTrackedLocked()
	o.tracked = make(map[linuxProcessIdentity]*linuxTrackedProcess)
	o.rootProcess = linuxProcessIdentity{}
	o.state, o.attempt = "clean", 0
	return nil
}
func (o *LinuxProcessTreeOwnership) inventoryLocked(root linuxProcessIdentity) (map[linuxProcessIdentity]*linuxTrackedProcess, error) {
	table, err := readLinuxProcTable()
	if err != nil {
		return nil, err
	}
	if _, ok := table[root]; !ok {
		return nil, ErrProcessOwnershipUncertain
	}
	children := make(map[int][]linuxProcessIdentity)
	for identity, row := range table {
		children[int(row.ppid)] = append(children[int(row.ppid)], identity)
	}
	identities := map[linuxProcessIdentity]struct{}{root: {}}
	queue := []int{root.PID}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range children[parent] {
			if _, seen := identities[child]; seen {
				continue
			}
			identities[child] = struct{}{}
			queue = append(queue, child.PID)
			if len(identities) > o.maxProcs {
				return nil, fmt.Errorf("%w: process tree exceeds bound", ErrProcessOwnershipUncertain)
			}
		}
	}
	tracked := make(map[linuxProcessIdentity]*linuxTrackedProcess, len(identities))
	for identity := range identities {
		fd, err := unix.PidfdOpen(identity.PID, 0)
		if err != nil {
			for _, process := range tracked {
				_ = unix.Close(process.pidfd)
			}
			return nil, fmt.Errorf("open pidfd for %d: %w", identity.PID, err)
		}
		tracked[identity] = &linuxTrackedProcess{identity: identity, pidfd: fd}
	}
	return tracked, nil
}
func (o *LinuxProcessTreeOwnership) refreshInventoryLocked() error {
	if o.rootProcess.PID <= 0 {
		return ErrProcessOwnershipUncertain
	}
	table, err := readLinuxProcTable()
	if err != nil {
		return err
	}
	row, ok := table[o.rootProcess]
	if !ok || row.state == 'Z' || row.state == 'X' || int(row.ppid) != os.Getpid() {
		return nil
	}
	refreshed, err := o.inventoryLocked(o.rootProcess)
	if err != nil {
		return err
	}
	for current, process := range o.tracked {
		if _, stillTracked := refreshed[current]; !stillTracked {
			_ = unix.Close(process.pidfd)
		}
	}
	for current, process := range refreshed {
		if previous, existed := o.tracked[current]; existed {
			_ = unix.Close(process.pidfd)
			refreshed[current] = previous
		}
	}
	o.tracked = refreshed
	return nil
}
func (o *LinuxProcessTreeOwnership) signalTrackedLocked(signal unix.Signal) error {
	for identity, process := range o.tracked {
		if process == nil || process.pidfd < 0 {
			return ErrProcessOwnershipUncertain
		}
		if err := unix.PidfdSendSignal(process.pidfd, signal, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
			return fmt.Errorf("signal owned pid %d: %w", identity.PID, err)
		}
	}
	return nil
}
func (o *LinuxProcessTreeOwnership) waitTrackedLocked(deadline time.Time) bool {
	for time.Now().Before(deadline) {
		alive := false
		for _, process := range o.tracked {
			if process == nil {
				return false
			}
			fds := []unix.PollFd{{Fd: int32(process.pidfd), Events: unix.POLLIN}}
			n, err := unix.Poll(fds, 0)
			if err != nil {
				return false
			}
			if n == 0 || fds[0].Revents&(unix.POLLIN|unix.POLLHUP|unix.POLLERR) == 0 {
				alive = true
			}
		}
		if !alive {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
func (o *LinuxProcessTreeOwnership) reapTrackedDescendantsLocked() {
	for identity := range o.tracked {
		if identity == o.rootProcess {
			continue
		}
		var status unix.WaitStatus
		_, _ = unix.Wait4(identity.PID, &status, unix.WNOHANG, nil)
	}
}
func (o *LinuxProcessTreeOwnership) ensureNoUnexpectedChildrenLocked() error {
	children, err := o.currentChildrenLocked()
	if err != nil {
		return err
	}
	for identity := range children {
		if _, baseline := o.baseline[identity]; baseline {
			continue
		}
		if _, owned := o.tracked[identity]; !owned {
			return fmt.Errorf("%w: unknown adopted child %d start=%d tracked=%v baseline=%v", ErrProcessOwnershipUncertain, identity.PID, identity.StartTime, sortedPIDs(o.tracked), sortedPIDs(o.baseline))
		}
	}
	return nil
}
func sortedPIDs[T any](processes map[linuxProcessIdentity]T) []int {
	ids := make([]int, 0, len(processes))
	for identity := range processes {
		ids = append(ids, identity.PID)
	}
	sort.Ints(ids)
	return ids
}
func (o *LinuxProcessTreeOwnership) currentChildrenLocked() (map[linuxProcessIdentity]struct{}, error) {
	table, err := readLinuxProcTable()
	if err != nil {
		return nil, err
	}
	children := make(map[linuxProcessIdentity]struct{})
	for identity, row := range table {
		if int(row.ppid) == os.Getpid() {
			children[identity] = struct{}{}
		}
	}
	return children, nil
}
func (o *LinuxProcessTreeOwnership) closeTrackedLocked() {
	for _, process := range o.tracked {
		if process != nil && process.pidfd >= 0 {
			_ = unix.Close(process.pidfd)
		}
	}
}
func (o *LinuxProcessTreeOwnership) persistLocked() error {
	records := o.recordsFromTrackedLocked()
	if len(records) == 0 && o.state == "quarantine" {
		records = append(records, o.durable...)
	}
	o.durable = append([]linuxProcessRecord(nil), records...)
	nextVersion := o.manifestVersion + 1
	if nextVersion == 0 {
		return ErrProcessOwnershipUncertain
	}
	if err := writeOwnershipManifest(o.root, linuxOwnershipManifest{
		Schema: ownershipManifestSchema, Version: nextVersion, Runtime: o.runtimeID, State: o.state,
		Attempt: o.attempt, Records: records,
	}); err != nil {
		return err
	}
	o.manifestVersion = nextVersion
	return nil
}
func (o *LinuxProcessTreeOwnership) recordsFromTrackedLocked() []linuxProcessRecord {
	records := make([]linuxProcessRecord, 0, len(o.tracked))
	for identity := range o.tracked {
		id := fmt.Sprintf("%d-%d", identity.PID, identity.StartTime)
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(id)))
		records = append(records, linuxProcessRecord{ID: id, PID: identity.PID, StartTime: identity.StartTime, Digest: digest})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	return records
}
type linuxProcRow struct {
	ppid  uint32
	state byte
}
func readLinuxProcTable() (map[linuxProcessIdentity]linuxProcRow, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("read proc inventory: %w", err)
	}
	table := make(map[linuxProcessIdentity]linuxProcRow)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		identity, ppid, state, err := readLinuxProcessStat(pid)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		table[identity] = linuxProcRow{ppid: ppid, state: state}
	}
	return table, nil
}
func readLinuxProcessStat(pid int) (linuxProcessIdentity, uint32, byte, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return linuxProcessIdentity{}, 0, 0, err
	}
	closeName := strings.LastIndexByte(string(data), ')')
	if closeName < 0 {
		return linuxProcessIdentity{}, 0, 0, ErrProcessOwnershipUncertain
	}
	fields := strings.Fields(string(data[closeName+1:]))
	if len(fields) <= 19 {
		return linuxProcessIdentity{}, 0, 0, ErrProcessOwnershipUncertain
	}
	ppid, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		return linuxProcessIdentity{}, 0, 0, ErrProcessOwnershipUncertain
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return linuxProcessIdentity{}, 0, 0, ErrProcessOwnershipUncertain
	}
	return linuxProcessIdentity{PID: pid, StartTime: start}, uint32(ppid), fields[0][0], nil
}
func protectedOwnershipRoot(root string, providerUID uint32, create bool) (string, error) {
	if root == "" || providerUID == 0 || providerUID == uint32(os.Getuid()) {
		return "", ErrProcessOwnershipUnavailable
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if create {
		if err := os.MkdirAll(abs, 0700); err != nil {
			return "", err
		}
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return "", ErrProcessOwnershipUnavailable
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Getuid()) {
		return "", ErrProcessOwnershipUnavailable
	}
	return abs, nil
}
func readOwnershipManifest(path string) (linuxOwnershipManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return linuxOwnershipManifest{}, fmt.Errorf("read ownership manifest: %w", err)
	}
	var manifest linuxOwnershipManifest
	if err := json.Unmarshal(data, &manifest); err != nil || manifest.Schema != ownershipManifestSchema || manifest.Version == 0 || manifest.Runtime == "" {
		return linuxOwnershipManifest{}, ErrProcessOwnershipUncertain
	}
	if manifest.State != "clean" && manifest.State != "prepared" && manifest.State != "started" && manifest.State != "quarantine" {
		return linuxOwnershipManifest{}, ErrProcessOwnershipUncertain
	}
	for index, record := range manifest.Records {
		if record.ID == "" || record.PID <= 0 || record.StartTime == 0 || record.Digest != fmt.Sprintf("%x", sha256.Sum256([]byte(record.ID))) || (index > 0 && manifest.Records[index-1].ID >= record.ID) {
			return linuxOwnershipManifest{}, ErrProcessOwnershipUncertain
		}
	}
	if manifest.State == "clean" && len(manifest.Records) != 0 {
		return linuxOwnershipManifest{}, ErrProcessOwnershipUncertain
	}
	if err := validateOwnershipRecords(filepath.Dir(path), manifest); err != nil {
		return linuxOwnershipManifest{}, err
	}
	return manifest, nil
}
func validateOwnershipRoot(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("inspect ownership root: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() != "manifest.json" && !strings.HasPrefix(entry.Name(), "record-") {
			return fmt.Errorf("%w: unexpected ownership root entry %q", ErrProcessOwnershipUncertain, entry.Name())
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("%w: non-regular ownership root entry %q", ErrProcessOwnershipUncertain, entry.Name())
		}
	}
	return nil
}
func validateOwnershipRecords(root string, manifest linuxOwnershipManifest) error {
	expected := make(map[string]linuxProcessRecord, len(manifest.Records))
	for _, record := range manifest.Records {
		expected["record-"+record.ID+".json"] = record
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("inspect ownership records: %w", err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "record-") {
			continue
		}
		record, ok := expected[entry.Name()]
		if !ok {
			return fmt.Errorf("%w: unexpected ownership record %q", ErrProcessOwnershipUncertain, entry.Name())
		}
		data, readErr := os.ReadFile(filepath.Join(root, entry.Name()))
		if readErr != nil {
			return fmt.Errorf("read ownership record: %w", readErr)
		}
		var stored linuxOwnershipRecordFile
		if json.Unmarshal(data, &stored) != nil || stored.Version != manifest.Version || stored.Record != record {
			return ErrProcessOwnershipUncertain
		}
		delete(expected, entry.Name())
	}
	if len(expected) > 0 {
		return ErrProcessOwnershipUncertain
	}
	return nil
}
func writeOwnershipManifest(root string, manifest linuxOwnershipManifest) error {
	for _, record := range manifest.Records {
		data, err := json.Marshal(linuxOwnershipRecordFile{Version: manifest.Version, Record: record})
		if err != nil { return err }
		if err := writeOwnershipFile(root, "record-"+record.ID+".json", ".record-*", data); err != nil { return err }
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := writeOwnershipFile(root, "manifest.json", ".manifest-*", data); err != nil {
		return err
	}
	if err := syncOwnershipRoot(root); err != nil {
		return err
	}
	return removeStaleOwnershipRecords(root, manifest)
}
func writeOwnershipFile(root, name, pattern string, data []byte) error {
	tmp, err := os.CreateTemp(root, pattern)
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(root, name))
}
func syncOwnershipRoot(root string) error {
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func removeStaleOwnershipRecords(root string, manifest linuxOwnershipManifest) error {
	expected := make(map[string]struct{}, len(manifest.Records))
	for _, record := range manifest.Records {
		expected["record-"+record.ID+".json"] = struct{}{}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "record-") { if _, keep := expected[entry.Name()]; !keep {
			if err := os.Remove(filepath.Join(root, entry.Name())); err != nil { return err }
			removed = true
		} }
	}
	if !removed {
		return nil
	}
	return syncOwnershipRoot(root)
}
func newOpaqueOwnershipID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
