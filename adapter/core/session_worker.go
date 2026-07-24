package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrInvalidSessionWorkerConfig = errors.New("invalid session worker config")

// SessionWorker is the single-Session owner for the existing Provider process
// lifecycle. Hub connection, credentials, and command delivery remain outside
// this characterization boundary until their dedicated tasks own them.
type SessionWorker struct {
	sessionID string
	provider  *ProcessSupervisor
}

type SessionWorkerConfig struct {
	SessionID                 string
	Provider                  ProcessConfig
	RecoveryStartHandleSource RecoveryStartHandleSource
	RecoveryStartHandle       *RecoveryStartHandle
}

// RecoveryStartHandleSource exposes only the current opaque reference. The
// source owns lifecycle truth; callers cannot inspect or derive its value.
type RecoveryStartHandleSource interface {
	RecoveryStartHandle() (RecoveryStartHandle, error)
}

// BindRecoveryStartAdmission fences every child start against the last
// Store-admitted handle while delegating the actual prepare/started handshake
// to the existing T42B2 admission.
func BindRecoveryStartAdmission(provider ProcessConfig, source RecoveryStartHandleSource) (ProcessConfig, error) {
	return bindRecoveryStartAdmission(provider, source, nil)
}

func bindRecoveryStartAdmission(provider ProcessConfig, source RecoveryStartHandleSource, expected *RecoveryStartHandle) (ProcessConfig, error) {
	if source == nil || provider.StartAdmission == nil {
		return ProcessConfig{}, ErrRecoveryAuthorityLost
	}
	bound := &recoveryBoundProcessStartAdmission{source: source, delegate: provider.StartAdmission}
	if expected != nil {
		if expected.value == "" {
			return ProcessConfig{}, ErrRecoveryAuthorityLost
		}
		bound.expected = *expected
		bound.ready = true
	}
	provider.StartAdmission = bound
	return provider, nil
}

type recoveryBoundProcessStartAdmission struct {
	source   RecoveryStartHandleSource
	delegate ProcessStartAdmission

	mu       sync.Mutex
	expected RecoveryStartHandle
	ready    bool
}

func (a *recoveryBoundProcessStartAdmission) PrepareProcessStart(ctx context.Context, attempt int) error {
	if a == nil || a.source == nil || a.delegate == nil || attempt < 1 {
		return ErrRecoveryAuthorityLost
	}
	if err := a.verifyCurrent(); err != nil {
		return err
	}
	if err := a.delegate.PrepareProcessStart(ctx, attempt); err != nil {
		return err
	}
	return nil
}

func (a *recoveryBoundProcessStartAdmission) ConfirmProcessStarted(ctx context.Context, attempt int) error {
	if a == nil || a.source == nil || a.delegate == nil || attempt < 1 {
		return ErrRecoveryAuthorityLost
	}
	if err := a.delegate.ConfirmProcessStarted(ctx, attempt); err != nil {
		return err
	}
	handle, err := a.source.RecoveryStartHandle()
	if err != nil {
		return fmt.Errorf("%w: admitted child has no recovery handle", ErrRecoveryAuthorityLost)
	}
	a.mu.Lock()
	a.expected = handle
	a.ready = true
	a.mu.Unlock()
	return nil
}

func (a *recoveryBoundProcessStartAdmission) verifyCurrent() error {
	handle, err := a.source.RecoveryStartHandle()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		if !a.ready {
			// The first child has no committed handle yet. T42B2's delegate
			// will establish it before this wrapper permits a later retry.
			return nil
		}
		return fmt.Errorf("%w: current recovery handle unavailable", ErrRecoveryAuthorityLost)
	}
	if !a.ready {
		return nil
	}
	if a.expected.value != handle.value {
		return fmt.Errorf("%w: recovery handle replaced", ErrRecoveryAuthorityLost)
	}
	return nil
}

func NewSessionWorker(cfg SessionWorkerConfig) (*SessionWorker, error) {
	return newSessionWorker(cfg, execProcessRunner{})
}

func newSessionWorker(cfg SessionWorkerConfig, runner processRunner) (*SessionWorker, error) {
	if cfg.SessionID == "" {
		return nil, ErrInvalidSessionWorkerConfig
	}
	if cfg.RecoveryStartHandleSource != nil {
		if cfg.Provider.StartAdmission == nil {
			return nil, ErrRecoveryAuthorityLost
		}
		bound, err := bindRecoveryStartAdmission(cfg.Provider, cfg.RecoveryStartHandleSource, cfg.RecoveryStartHandle)
		if err != nil {
			return nil, err
		}
		cfg.Provider = bound
	}
	provider, err := newProcessSupervisor(cfg.Provider, runner)
	if err != nil {
		return nil, err
	}
	return &SessionWorker{sessionID: cfg.SessionID, provider: provider}, nil
}

func (w *SessionWorker) SessionID() string {
	if w == nil {
		return ""
	}
	return w.sessionID
}

func (w *SessionWorker) Events() <-chan ProcessEvent {
	if w == nil || w.provider == nil {
		return nil
	}
	return w.provider.Events()
}

func (w *SessionWorker) Run(ctx context.Context) error {
	if w == nil || w.provider == nil {
		return ErrInvalidSessionWorkerConfig
	}
	return w.provider.Run(ctx)
}

func (w *SessionWorker) Stop(ctx context.Context) error {
	if w == nil || w.provider == nil {
		return ErrInvalidSessionWorkerConfig
	}
	return w.provider.Stop(ctx)
}
