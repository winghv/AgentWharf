//go:build linux

package core

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
)

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
