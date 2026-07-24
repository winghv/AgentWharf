//go:build linux

package core

import (
	"context"
	"errors"
	"sync"
)

type LinuxQuiescenceLease struct {
	mu          sync.Mutex
	owner       *LinuxProcessTreeOwnership
	generation  string
	held        bool
	released    bool
	quarantined bool
}

func NewLinuxQuiescenceLease(owner *LinuxProcessTreeOwnership, generation string) (*LinuxQuiescenceLease, error) {
	if owner == nil || generation == "" {
		return nil, ErrProcessOwnershipUnavailable
	}
	return &LinuxQuiescenceLease{owner: owner, generation: generation, held: true}, nil
}
func (l *LinuxQuiescenceLease) Release(_ context.Context) error {
	if l == nil {
		return ErrProcessOwnershipUnavailable
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.quarantined || l.released || !l.held || l.owner == nil {
		return ErrProcessOwnershipQuarantined
	}
	if !l.owner.Quiescent() {
		return errors.New("quiescence proof is required before lease release")
	}
	l.held, l.released = false, true
	return nil
}
func (l *LinuxQuiescenceLease) Quarantine(ctx context.Context) error {
	if l == nil {
		return ErrProcessOwnershipUnavailable
	}
	l.mu.Lock()
	if l.quarantined {
		l.mu.Unlock()
		return ErrProcessOwnershipQuarantined
	}
	l.quarantined, l.held = true, false
	owner := l.owner
	l.mu.Unlock()
	if owner == nil {
		return ErrProcessOwnershipUnavailable
	}
	return owner.Quarantine(ctx)
}
func (l *LinuxQuiescenceLease) Disconnect(ctx context.Context) error {
	return l.Quarantine(ctx)
}
func (l *LinuxQuiescenceLease) Held() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held && !l.released && !l.quarantined
}
