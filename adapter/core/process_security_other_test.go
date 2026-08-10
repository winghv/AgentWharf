//go:build !linux

package core

import (
	"context"
	"errors"
	"testing"
)

// The Linux build proves the child-confidentiality boundary and its probe is
// covered by process_security_linux_test.go. Every other platform has no
// approved proof, so this build's job is to fail closed and keep multi-Worker
// capability disabled. That is a security property, not a stub: if this ever
// returned a nil error, callers would read it as "boundary proven" and enable
// multi-Worker on a platform where nothing was verified.
func TestProbeChildConfidentialityIsUnavailableWithoutTheLinuxProof(t *testing.T) {
	t.Parallel()

	report, err := ProbeChildConfidentiality(context.Background())
	if err == nil {
		t.Fatal("ProbeChildConfidentiality returned a nil error on a non-Linux build; callers would treat the confidentiality boundary as proven and enable multi-Worker without any verification")
	}
	if !errors.Is(err, ErrChildConfidentialityUnavailable) {
		t.Fatalf("error = %v, want it to wrap ErrChildConfidentialityUnavailable so callers can distinguish 'no proof on this platform' from 'proof attempted and failed' (ErrChildConfidentialityUnproven)", err)
	}
	// Unavailable and unproven are distinct outcomes and callers branch on the
	// difference; collapsing them would misreport a platform that never ran a
	// probe as one whose probe came back negative.
	if errors.Is(err, ErrChildConfidentialityUnproven) {
		t.Fatalf("error = %v, want ErrChildConfidentialityUnavailable only; this build never runs a probe, so reporting it as unproven claims a measurement that did not happen", err)
	}
	if report != (ChildConfidentialityReport{}) {
		t.Fatalf("report = %+v, want the zero value; a non-Linux build has nothing to report", report)
	}
}

// A cancelled context must not turn the refusal into a context error. Callers
// distinguish "this platform has no proof" from "the probe was interrupted",
// and on this build the answer is always the former regardless of the context.
func TestProbeChildConfidentialityIgnoresContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ProbeChildConfidentiality(ctx)
	if !errors.Is(err, ErrChildConfidentialityUnavailable) {
		t.Fatalf("error = %v, want ErrChildConfidentialityUnavailable even with a cancelled context; the answer on this build does not depend on the probe running", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want the platform refusal rather than a context error; a caller retrying on context.Canceled would loop forever on a platform that can never succeed", err)
	}
}
