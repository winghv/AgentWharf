package core

import (
	"context"
	"errors"
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
	SessionID string
	Provider  ProcessConfig
}

func NewSessionWorker(cfg SessionWorkerConfig) (*SessionWorker, error) {
	return newSessionWorker(cfg, execProcessRunner{})
}

func newSessionWorker(cfg SessionWorkerConfig, runner processRunner) (*SessionWorker, error) {
	if cfg.SessionID == "" {
		return nil, ErrInvalidSessionWorkerConfig
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
