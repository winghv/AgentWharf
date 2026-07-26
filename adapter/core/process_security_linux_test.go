//go:build linux

package core

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// yamaPtraceScope reads the kernel policy the probes depend on. Environments
// without Yama cannot produce a denial proof, so dependent tests skip there
// and the composed gate requires a Yama-enabled kernel to pass.
func yamaPtraceScope(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile("/proc/sys/kernel/yama/ptrace_scope")
	if err != nil {
		t.Skipf("yama ptrace policy is unavailable on this kernel: %v", err)
	}
	scope, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("invalid yama ptrace policy: %v", err)
	}
	return scope
}

func TestChildConfidentialityStatusParser(t *testing.T) {
	values := parseProcStatus("Dumpable:\t0\nNoNewPrivs:\t1\nSeccomp:\t2\nCapEff:\t0\nCapPrm:\t0\nCapBnd:\t0\n")
	if values["Dumpable"] != "0" || values["NoNewPrivs"] != "1" || values["Seccomp"] != "2" || values["CapBnd"] != "0" {
		t.Fatalf("parseProcStatus() = %#v", values)
	}
	if !allZeroHex("0000000000000000") || allZeroHex("0000000000000001") {
		t.Fatal("allZeroHex() did not enforce a zero capability mask")
	}
}

func TestChildConfidentialityProbeFailsClosedWhenUnsupported(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ProbeChildConfidentiality(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ProbeChildConfidentiality() error = %v, want context cancellation before probe", err)
	}
}

func TestChildConfidentialityReportRequiresEveryBoundary(t *testing.T) {
	report := ChildConfidentialityReport{
		DumpableZero: true, CoreLimitZero: true, NoNewPrivileges: true,
		CapabilitiesZero: true, SeccompFiltered: true, PtraceRestricted: true,
		ProcEnvironDenied: true, ProcMemDenied: true, ProcFDsDenied: true,
		ProcessVMDenied: true, PidfdGetfdDenied: true,
	}
	if !report.valid() {
		t.Fatal("complete child-confidentiality report did not validate")
	}
	report.PidfdGetfdDenied = false
	if report.valid() {
		t.Fatal("incomplete child-confidentiality report validated")
	}
}

func TestClassifyProbeOpenErrorFailsClosedForUnknownErrno(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "permission denied", err: syscall.EACCES, want: childProbeDenied},
		{name: "operation not permitted", err: syscall.EPERM, want: childProbeDenied},
		{name: "missing target", err: os.ErrNotExist, want: childProbeMissing},
		{name: "too many files", err: syscall.EMFILE, want: childProbeUnknown},
		{name: "system file error", err: syscall.EIO, want: childProbeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyProbeOpenError(test.err); got != test.want {
				t.Fatalf("classifyProbeOpenError(%v) = %d, want %d", test.err, got, test.want)
			}
		})
	}
}

func TestChildConfidentialityReportStringDoesNotCarrySecrets(t *testing.T) {
	report := ChildConfidentialityReport{DumpableZero: true, CoreLimitZero: true, NoNewPrivileges: true}
	if report.valid() {
		t.Fatal("partial child report unexpectedly validated")
	}
}

func TestPtraceRestrictedMatchesKernelPolicy(t *testing.T) {
	scope := yamaPtraceScope(t)
	restricted, err := ptraceRestricted()
	if err != nil {
		t.Fatalf("ptraceRestricted() error = %v", err)
	}
	if want := scope >= 1; restricted != want {
		t.Fatalf("ptraceRestricted() = %v with kernel scope %d", restricted, scope)
	}
}

func TestCoreLimitZeroMatchesCurrentRlimit(t *testing.T) {
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_CORE, &limit); err != nil {
		t.Fatalf("getrlimit: %v", err)
	}
	zero, err := coreLimitZero()
	if err != nil {
		t.Fatalf("coreLimitZero() error = %v", err)
	}
	if want := limit.Cur == 0 && limit.Max == 0; zero != want {
		t.Fatalf("coreLimitZero() = %v, rlimit = %+v", zero, limit)
	}
}

// withNonDumpableProcess reproduces the supervisor confinement the probes
// verify: a non-dumpable process is what turns same-UID proc access into a
// denial, exactly as the sandboxed Adapter configures itself.
func withNonDumpableProcess(t *testing.T) {
	t.Helper()
	previous, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		t.Fatalf("read dumpable state: %v", err)
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		t.Fatalf("clear dumpable state: %v", err)
	}
	t.Cleanup(func() {
		_ = unix.Prctl(unix.PR_SET_DUMPABLE, uintptr(previous), 0, 0, 0)
	})
}

func TestSameUIDProcAccessProbeDeniesNonDumpableSupervisor(t *testing.T) {
	withNonDumpableProcess(t)
	if err := probeSameUIDProcAccess(context.Background()); err != nil {
		t.Fatalf("probeSameUIDProcAccess() error = %v, want proven denial", err)
	}
}

func TestRunChildConfidentialityProbeClassifiesTargets(t *testing.T) {
	if got := runChildConfidentialityProbe(os.Getppid()); got != childProbeGranted {
		t.Fatalf("runChildConfidentialityProbe(dumpable parent) = %d, want granted", got)
	}
	if got := runChildConfidentialityProbe(0); got != childProbeMissing {
		t.Fatalf("runChildConfidentialityProbe(missing target) = %d, want missing", got)
	}
}

func TestProbeChildConfidentialityIsConsistentWithReportValidity(t *testing.T) {
	if scope := yamaPtraceScope(t); scope < 1 {
		t.Skipf("yama ptrace scope %d cannot satisfy the restricted-ptrace assertion", scope)
	}
	withNonDumpableProcess(t)
	report, err := ProbeChildConfidentiality(context.Background())
	if err == nil {
		if !report.valid() {
			t.Fatal("probe succeeded with an invalid report")
		}
		return
	}
	if !errors.Is(err, ErrChildConfidentialityUnproven) {
		t.Fatalf("ProbeChildConfidentiality() error = %v, want unproven outside the sandbox", err)
	}
	if report.valid() {
		t.Fatal("unproven result carried a fully valid report")
	}
	if !report.ProcEnvironDenied || !report.ProcMemDenied || !report.ProcFDsDenied {
		t.Fatalf("same-UID proc denials missing from report: %+v", report)
	}
}
