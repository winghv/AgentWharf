//go:build linux

package core

import (
	"context"
	"errors"
	"testing"
)

func TestLinuxQuiescenceLeaseRequiresOwnerAndGeneration(t *testing.T) {
	if _, err := NewLinuxQuiescenceLease(nil, "generation-1"); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("NewLinuxQuiescenceLease(nil owner) error = %v, want unavailable", err)
	}
	owner, _ := newLinuxOwnershipForTest(t)
	if _, err := NewLinuxQuiescenceLease(owner, ""); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("NewLinuxQuiescenceLease(empty generation) error = %v, want unavailable", err)
	}
}

func TestLinuxQuiescenceLeaseNilReceiverFailsClosed(t *testing.T) {
	var lease *LinuxQuiescenceLease
	if lease.Held() {
		t.Fatal("nil lease reported held")
	}
	if err := lease.Release(context.Background()); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("nil lease Release() error = %v, want unavailable", err)
	}
	if err := lease.Quarantine(context.Background()); !errors.Is(err, ErrProcessOwnershipUnavailable) {
		t.Fatalf("nil lease Quarantine() error = %v, want unavailable", err)
	}
}

func TestLinuxQuiescenceLeaseQuarantineIsTerminalAndPropagates(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	lease, err := NewLinuxQuiescenceLease(owner, "generation-1")
	if err != nil {
		t.Fatalf("NewLinuxQuiescenceLease() error = %v", err)
	}
	if err := lease.Quarantine(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Quarantine() error = %v, want quarantined owner", err)
	}
	if lease.Held() {
		t.Fatal("lease remains held after quarantine")
	}
	if owner.Quiescent() {
		t.Fatal("owner reports quiescence after lease quarantine")
	}
	if err := lease.Release(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Release() after quarantine error = %v, want quarantined", err)
	}
	if err := lease.Quarantine(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("second Quarantine() error = %v, want quarantined", err)
	}
}

func TestLinuxQuiescenceLeaseDisconnectQuarantines(t *testing.T) {
	owner, _ := newLinuxOwnershipForTest(t)
	lease, err := NewLinuxQuiescenceLease(owner, "generation-1")
	if err != nil {
		t.Fatalf("NewLinuxQuiescenceLease() error = %v", err)
	}
	if err := lease.Disconnect(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Disconnect() error = %v, want quarantine propagation", err)
	}
	if lease.Held() {
		t.Fatal("lease remains held after disconnect")
	}
	if err := lease.Release(context.Background()); !errors.Is(err, ErrProcessOwnershipQuarantined) {
		t.Fatalf("Release() after disconnect error = %v, want quarantined", err)
	}
}
