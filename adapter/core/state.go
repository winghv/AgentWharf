package core

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidState      = errors.New("invalid session state")
	ErrInvalidReason     = errors.New("invalid session reason")
	ErrInvalidTransition = errors.New("invalid session state transition")
	ErrTerminalState     = errors.New("session state is terminal")
)

type SessionState string

const (
	StateStarting          SessionState = "starting"
	StateReady             SessionState = "ready"
	StateBusy              SessionState = "busy"
	StateWaitingPermission SessionState = "waiting_permission"
	StateRecovering        SessionState = "recovering"
	StateEnded             SessionState = "ended"
	StateError             SessionState = "error"
)

type EndReason string

const (
	EndReasonUserStop     EndReason = "user_stop"
	EndReasonProviderExit EndReason = "provider_exit"
	EndReasonCrash        EndReason = "crash"
	EndReasonTimeout      EndReason = "timeout"
)

type Transition struct {
	From   SessionState
	To     SessionState
	Reason string
}

func (t Transition) Terminal() bool {
	return t.To == StateEnded || t.To == StateError
}

type StateMachine struct {
	state SessionState
}

func NewStateMachine(initial SessionState) (*StateMachine, error) {
	if !validState(initial) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidState, initial)
	}
	if terminalState(initial) {
		return nil, fmt.Errorf("%w: initial state %q", ErrTerminalState, initial)
	}
	return &StateMachine{state: initial}, nil
}

func (m *StateMachine) State() SessionState {
	if m == nil {
		return ""
	}
	return m.state
}

func (m *StateMachine) Transition(to SessionState, reason string) (Transition, error) {
	if m == nil || !validState(m.state) {
		return Transition{}, fmt.Errorf("%w: current state %q", ErrInvalidState, m.State())
	}
	if terminalState(m.state) {
		return Transition{}, fmt.Errorf("%w: %q", ErrTerminalState, m.state)
	}
	if !validState(to) {
		return Transition{}, fmt.Errorf("%w: %q", ErrInvalidState, to)
	}
	if to == StateEnded || to == StateError {
		if reason == "" {
			return Transition{}, ErrInvalidReason
		}
		return m.apply(to, reason), nil
	}
	if !legalTransition(m.state, to) {
		return Transition{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, m.state, to)
	}
	return m.apply(to, reason), nil
}

func (m *StateMachine) End(reason EndReason) (Transition, error) {
	if !validEndReason(reason) {
		return Transition{}, fmt.Errorf("%w: %q", ErrInvalidReason, reason)
	}
	return m.Transition(StateEnded, string(reason))
}

func (m *StateMachine) Error(reason string) (Transition, error) {
	if reason == "" {
		return Transition{}, ErrInvalidReason
	}
	return m.Transition(StateError, reason)
}

func (m *StateMachine) apply(to SessionState, reason string) Transition {
	transition := Transition{From: m.state, To: to, Reason: reason}
	m.state = to
	return transition
}

func validState(state SessionState) bool {
	switch state {
	case StateStarting, StateReady, StateBusy, StateWaitingPermission, StateRecovering, StateEnded, StateError:
		return true
	default:
		return false
	}
}

func terminalState(state SessionState) bool {
	return state == StateEnded || state == StateError
}

func legalTransition(from SessionState, to SessionState) bool {
	switch from {
	case StateStarting:
		return to == StateReady
	case StateReady:
		return to == StateBusy || to == StateRecovering
	case StateBusy:
		return to == StateReady || to == StateWaitingPermission || to == StateRecovering
	case StateWaitingPermission:
		return to == StateBusy || to == StateReady
	case StateRecovering:
		return to == StateReady
	default:
		return false
	}
}

func validEndReason(reason EndReason) bool {
	switch reason {
	case EndReasonUserStop, EndReasonProviderExit, EndReasonCrash, EndReasonTimeout:
		return true
	default:
		return false
	}
}
