//go:build linux

package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var (
	ErrChildConfidentialityUnavailable = errors.New("child confidentiality boundary is unavailable")
	ErrChildConfidentialityUnproven    = errors.New("child confidentiality boundary is unproven")
)

// ChildConfidentialityReport is deliberately evidence-shaped. A true result
// requires both supervisor state and a same-UID child access probe; callers
// must not turn an individual field into a capability decision.
type ChildConfidentialityReport struct {
	DumpableZero      bool
	CoreLimitZero     bool
	NoNewPrivileges   bool
	CapabilitiesZero  bool
	SeccompFiltered   bool
	PtraceRestricted  bool
	ProcEnvironDenied bool
	ProcMemDenied     bool
	ProcFDsDenied     bool
	ProcessVMDenied   bool
	PidfdGetfdDenied  bool
}

func (r ChildConfidentialityReport) valid() bool {
	return r.DumpableZero && r.CoreLimitZero && r.NoNewPrivileges &&
		r.CapabilitiesZero && r.SeccompFiltered && r.PtraceRestricted &&
		r.ProcEnvironDenied && r.ProcMemDenied && r.ProcFDsDenied &&
		r.ProcessVMDenied && r.PidfdGetfdDenied
}

// ProbeChildConfidentiality checks the actual supervisor confinement and then
// runs a same-UID child that attempts the credential-bearing proc accesses.
// Any missing tool, unsupported kernel policy or failed assertion is a hard
// denial so multi-Worker capability remains absent.
func ProbeChildConfidentiality(ctx context.Context) (ChildConfidentialityReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	report, err := readChildConfidentialityReport()
	if err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := probeSameUIDProcAccess(ctx); err != nil {
		return report, err
	}
	report.ProcEnvironDenied = true
	report.ProcMemDenied = true
	report.ProcFDsDenied = true
	// process_vm_* and pidfd_getfd are governed by the same ptrace/capability
	// boundary here: dumpable=0, restricted ptrace and no effective capability.
	report.ProcessVMDenied = report.PtraceRestricted && report.CapabilitiesZero
	report.PidfdGetfdDenied = report.PtraceRestricted && report.CapabilitiesZero
	if !report.valid() {
		return report, ErrChildConfidentialityUnproven
	}
	return report, nil
}

func readChildConfidentialityReport() (ChildConfidentialityReport, error) {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return ChildConfidentialityReport{}, fmt.Errorf("%w: read process status: %v", ErrChildConfidentialityUnavailable, err)
	}
	values := parseProcStatus(string(status))
	dumpable, ok := values["Dumpable"]
	if !ok {
		// Newer kernels may omit Dumpable from proc status; PR_GET_DUMPABLE is
		// the authoritative fallback.
		value, prctlErr := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
		if prctlErr != nil {
			return ChildConfidentialityReport{}, fmt.Errorf("%w: dumpable probe: %v", ErrChildConfidentialityUnavailable, prctlErr)
		}
		dumpable = strconv.Itoa(value)
	}
	noNewPrivs := values["NoNewPrivs"] == "1"
	seccomp := values["Seccomp"] == "2"
	capEff, capPrm, capBnd := values["CapEff"], values["CapPrm"], values["CapBnd"]
	capabilitiesZero := allZeroHex(capEff) && allZeroHex(capPrm) && allZeroHex(capBnd)
	coreZero, err := coreLimitZero()
	if err != nil {
		return ChildConfidentialityReport{}, err
	}
	ptraceRestricted, err := ptraceRestricted()
	if err != nil {
		return ChildConfidentialityReport{}, err
	}
	return ChildConfidentialityReport{
		DumpableZero:     dumpable == "0",
		CoreLimitZero:    coreZero,
		NoNewPrivileges:  noNewPrivs,
		CapabilitiesZero: capabilitiesZero,
		SeccompFiltered:  seccomp,
		PtraceRestricted: ptraceRestricted,
	}, nil
}

func allZeroHex(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && strings.Trim(value, "0") == ""
}

func parseProcStatus(status string) map[string]string {
	values := make(map[string]string)
	for _, line := range strings.Split(status, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			values[key] = strings.TrimSpace(value)
		}
	}
	return values
}

func coreLimitZero() (bool, error) {
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_CORE, &limit); err != nil {
		return false, fmt.Errorf("%w: core limit probe: %v", ErrChildConfidentialityUnavailable, err)
	}
	return limit.Cur == 0 && limit.Max == 0, nil
}

func ptraceRestricted() (bool, error) {
	data, err := os.ReadFile("/proc/sys/kernel/yama/ptrace_scope")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("%w: yama ptrace policy is unavailable", ErrChildConfidentialityUnavailable)
		}
		return false, fmt.Errorf("%w: read ptrace policy: %v", ErrChildConfidentialityUnavailable, err)
	}
	scope, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return false, fmt.Errorf("%w: invalid ptrace policy", ErrChildConfidentialityUnavailable)
	}
	return scope >= 1, nil
}

func probeSameUIDProcAccess(ctx context.Context) error {
	probe := `set -eu
p=$PPID
for target in environ mem fd/0; do
  if dd if="/proc/$p/$target" of=/dev/null bs=1 count=1 status=none 2>/dev/null; then
    exit 41
  fi
done
exit 0
`
	commandCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(commandCtx, "/bin/sh", "-c", probe)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		if errors.Is(commandCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: same-UID proc probe timed out", ErrChildConfidentialityUnavailable)
		}
		return fmt.Errorf("%w: same-UID proc access was not denied: %v", ErrChildConfidentialityUnproven, err)
	}
	return nil
}
